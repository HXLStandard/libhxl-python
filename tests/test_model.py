"""
Unit tests for the hxl.model module
David Megginson
October 2014

License: Public Domain
"""

import io, unittest
import hxl
from hxl.datatypes import normalise_string
from hxl.model import TagPattern, Dataset, Column, Row, RowQuery

DATA = [
    ['Organisation', 'Cluster', 'District', 'Affected'],
    ['#org', '#sector+cluster', '#adm1', '#affected'],
    ['NGO A', 'WASH', 'Coast', '100'],
    ['NGO B', 'Education', 'Plains', '200'],
    ['NGO B', 'Education', 'Coast', '300']
]

SCHEMA_GOOD = [
    ['#valid_tag', '#valid_required'],
    ['#org', 'true']
]

SCHEMA_BAD = [
    ['#valid_tag', '#valid_required'],
    ['#severity', 'true']
]


class TestPattern(unittest.TestCase):
    """Test the TagPattern class."""

    def setUp(self):
        self.column = Column(tag='#tag', attributes=['foo', 'bar'])

    def test_simple(self):
        pattern = TagPattern('#tag')
        self.assertTrue(pattern.match(self.column))
        pattern = TagPattern('#tagx')
        self.assertFalse(pattern.match(self.column))

    def test_include(self):
        pattern = TagPattern('#tag', include_attributes=['foo'])
        self.assertTrue(pattern.match(self.column))
        pattern = TagPattern('#tag', include_attributes=['xxx'])
        self.assertFalse(pattern.match(self.column))

    def test_exclude(self):
        pattern = TagPattern('#tag', exclude_attributes=['xxx'])
        self.assertTrue(pattern.match(self.column))
        pattern = TagPattern('#tag', exclude_attributes=['foo'])
        self.assertFalse(pattern.match(self.column))

    def test_caseinsensitive(self):
        pattern = TagPattern.parse('#Tag')
        self.assertTrue(pattern.match(self.column))
        pattern = TagPattern.parse('#tag+fOO')
        self.assertTrue(pattern.match(self.column))

    def test_simple_wildcard(self):
        pattern = TagPattern.parse('*')
        self.assertTrue(pattern.is_wildcard())
        self.assertTrue(pattern.match(self.column))

    def test_wildcard_empty_column(self):
        pattern = TagPattern.parse('*')
        untagged_column = Column(header="Foo", column_number=1)
        self.assertFalse(pattern.match(untagged_column))

    def test_attributes_wildcard(self):
        pattern = TagPattern.parse('*+foo')
        self.assertTrue(pattern.is_wildcard())
        self.assertTrue(pattern.match(self.column))

        pattern = TagPattern.parse('*-foo')
        self.assertTrue(pattern.is_wildcard())
        self.assertFalse(pattern.match(self.column))

        pattern = TagPattern.parse('*+xxx')
        self.assertTrue(pattern.is_wildcard())
        self.assertFalse(pattern.match(self.column))

    def test_absolute(self):
        # don't allow exclusions in an absolute pattern
        with self.assertRaises(ValueError):
            pattern = TagPattern.parse('#foo+a-b!')

        pattern = TagPattern.parse('#foo+a!')
        self.assertTrue(pattern.is_absolute)
        self.assertTrue(pattern.match(Column.parse('#foo+a')))
        self.assertFalse(pattern.match(Column.parse('#foo')))
        self.assertFalse(pattern.match(Column.parse('#foo+a+b')))
        
    def test_parse(self):
        pattern = TagPattern.parse('#tag+foo-xxx')
        self.assertEqual(pattern.tag, '#tag')
        self.assertTrue('foo' in pattern.include_attributes)

        pattern = TagPattern.parse('tag+foo-xxx')
        self.assertEqual(pattern.tag, '#tag')

        pattern = TagPattern.parse('   tag +foo  -xxx  ')
        self.assertEqual(pattern.tag, '#tag')
        self.assertEqual({'foo'}, pattern.include_attributes)
        self.assertEqual({'xxx'}, pattern.exclude_attributes)

    def test_parse_list(self):
        patterns = TagPattern.parse_list('tag+foo,tag-xxx')
        for pattern in patterns:
            self.assertTrue(pattern.match(self.column))
        patterns = TagPattern.parse_list('tag-foo,tag+xxx')
        for pattern in patterns:
            self.assertFalse(pattern.match(self.column))



class TestDataset(unittest.TestCase):

    def setUp(self):
        self.source = hxl.data(DATA)

    def test_min(self):
        self.assertEquals(100, self.source.min('#affected'))

    def test_min_date(self):
        DATA = [
            ['#date'],
            ['2018-01-01'],
            ['1/1/2019']
        ]
        self.assertEquals('2018-01-01', hxl.data(DATA).min('#date'))

    def test_min_year(self):
        DATA = [
            ['#date'],
            ['2018'],
            ['2017']
        ]
        self.assertEquals('2017', hxl.data(DATA).min('#date'))

    def test_max(self):
        self.assertEquals(300, self.source.max('#affected'))

    def test_cached(self):
        dataset = Dataset()
        self.assertFalse(dataset.is_cached)

    def test_headers(self):
        self.assertEqual(DATA[0], self.source.headers)

    def test_has_headers(self):
        self.assertTrue(self.source.has_headers)
        self.assertFalse(hxl.data(DATA[1:]).has_headers)

    def test_tags(self):
        self.assertEqual([Column.parse(s).tag for s in DATA[1]], self.source.tags)

    def test_display_tags(self):
        self.assertEqual(DATA[1], self.source.display_tags)

    def test_values(self):
        self.assertEqual(DATA[2:], self.source.values)

    def test_value_set_all(self):
        expected = set()
        for r in DATA[2:]:
            expected.update(r)
        self.assertEqual(expected, self.source.get_value_set())

    def test_value_set_normalised(self):
        expected = set([normalise_string(s[1]) for s in DATA[2:]])
        self.assertEqual(expected, self.source.get_value_set('#sector', True))

    def test_value_set_unnormalised(self):
        expected = set([s[1] for s in DATA[2:]])
        self.assertEqual(expected, self.source.get_value_set('#sector', False))

    def test_validate(self):
        self.assertTrue(self.source.validate(SCHEMA_GOOD))
        self.assertFalse(self.source.validate(SCHEMA_BAD))

    def test_hash_columns(self):
        self.assertTrue(self.source.columns_hash is not None)
        self.assertEqual(32, len(self.source.columns_hash))

    # TODO test generators


class TestColumn(unittest.TestCase):

    HXL_TAG = '#foo'
    ATTRIBUTES = ['en', 'bar', 'f']
    HEADER_TEXT = 'Foo header'
    COLUMN_NUMBER = 5
    SOURCE_COLUMN_NUMBER = 7

    def setUp(self):
        self.column = Column(tag=TestColumn.HXL_TAG, attributes=TestColumn.ATTRIBUTES, header=TestColumn.HEADER_TEXT)

    def test_variables(self):
        self.assertEqual(TestColumn.HXL_TAG, self.column.tag)
        self.assertEqual(set(TestColumn.ATTRIBUTES), self.column.attributes)
        self.assertEqual(TestColumn.HEADER_TEXT, self.column.header)

    def test_display_tag(self):
        self.assertEqual(TestColumn.HXL_TAG + '+' + "+".join(TestColumn.ATTRIBUTES), self.column.display_tag)

    def test_case_insensitive(self):
        column = Column(tag='Foo', attributes=['X', 'y'])
        self.assertEqual('foo', column.tag)
        self.assertEqual(set(['x', 'y']), column.attributes)

    def test_attribute_order(self):
        TAGSPEC = '#foo+b+a+c+w+x'
        self.assertEqual(TAGSPEC, Column.parse(TAGSPEC).display_tag)

    def test_eq(self):
        col1 = Column(tag='xxx', attributes={'b','c','a'}, header='foo')
        col2 = Column(tag='xxx', attributes={'a', 'b','c'}, header='bar')
        col3 = Column(tag='xxx', attributes={'b','c'})
        self.assertEqual(col1, col2)
        self.assertNotEqual(col1, col3)

    def test_hash(self):
        col1 = Column(tag='xxx', attributes={'b','c','a'}, header='foo')
        col2 = Column(tag='xxx', attributes={'a', 'b','c'}, header='bar')
        col3 = Column(tag='xxx', attributes={'b','c'})
        self.assertEqual(hash(col1), hash(col2))
        self.assertNotEqual(hash(col1), hash(col3))


class TestRow(unittest.TestCase):

    ROW_NUMBER = 5
    TAGS = ['#sector+list', '#org', '#country']
    CONTENT = ['Health, Education', 'WFP', 'Liberia'];

    def setUp(self):
        columns = []
        for column_number, tag in enumerate(TestRow.TAGS):
            columns.append(Column.parse(tag))
        self.row = Row(columns=columns, values=self.CONTENT, row_number=self.ROW_NUMBER)

    def test_row_number(self):
        self.assertEqual(TestRow.ROW_NUMBER, self.row.row_number)

    def test_data(self):
        self.assertEqual(TestRow.CONTENT, self.row.values)

    def test_iteration(self):
        expectedLength = len(TestRow.TAGS)
        actualLength = 0;
        for value in self.row:
            actualLength = actualLength + 1
        self.assertEqual(expectedLength, actualLength)

    def test_append(self):
        column_number = len(TestRow.TAGS)
        oldLength = len(self.row.values)
        self.row.append('Lofa County')
        self.assertEqual(oldLength + 1, len(self.row.values))

    def test_get(self):
        self.assertEqual('WFP', self.row.get('#org'))

    def test_list(self):
        self.assertEqual('Health, Education', self.row.get('#sector'))
        self.assertEqual(['Health', 'Education'], self.row.get('#sector', parsed=True))
        self.assertEqual(['WFP'], self.row.get('#org', parsed=True))

    def test_get_skip_blanks(self):
        columns = [Column.parse(tag) for tag in ['#sector', '#org', '#org']]
        row = Row(columns=columns, values=['Health', '', 'WFP'])
        # Test that row.get() returns first non-blank value
        self.assertEqual('WFP', row.get('org'))

    def test_get_all(self):
        result = self.row.get_all('#org')
        self.assertTrue(type(result) is list)
        self.assertEqual(1, len(result))

    def test_dictionary(self):
        self.assertEqual({
            '#country': 'Liberia',
            '#org': 'WFP',
            '#sector+list': 'Health, Education'
        }, self.row.dictionary)

    def test_outofrange(self):
        # what happens when a row is too short?
        self.row.values = self.CONTENT[0:1]
        self.assertEqual(None, self.row.get('#country'))
        self.assertEqual([], self.row.get_all('#country'))

    def test_parse_simple(self):
        column = Column.parse('#tag')
        self.assertEqual('#tag', column.tag)

    def test_parse_attributes(self):
        # Single attribute
        specs = ['#tag+foo', '#tag +foo', ' #tag +foo   ']
        for column in [Column.parse(spec) for spec in specs]:
            column = Column.parse('#tag+foo')
            self.assertEqual('#tag', column.tag)
            self.assertEqual(['foo'], sorted(column.attributes))

        # Multiple attributes
        specs = ['#tag+foo+bar', '#tag +foo +bar', ' #tag +bar+foo   ']
        for column in [Column.parse(spec) for spec in specs]:
            self.assertEqual('#tag', column.tag)
            self.assertEqual(['bar', 'foo'], sorted(column.attributes))


class TestRowQuery(unittest.TestCase):

    ROW_NUMBER = 5
    TAGS = ['#sector', '#date', '#adm1+name', '#affected', '#inneed', '#population', '#meta']
    CONTENT = ['WASH', '12/13/2015', '  Coast  ', '200', ' 500 ', '1,000', '']

    def setUp(self):
        columns = []
        for column_number, tag in enumerate(self.TAGS):
            columns.append(Column.parse(tag))
        self.row = Row(columns=columns, values=self.CONTENT, row_number=self.ROW_NUMBER)

    def test_string(self):
        # =
        self.assertTrue(RowQuery.parse("sector=WASH").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector=wash").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector=Health").match_row(self.row))
        # <=
        self.assertTrue(RowQuery.parse("sector<=WASH").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector<=wash").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector<=Health").match_row(self.row))
        # <
        self.assertTrue(RowQuery.parse("sector<ZZZ").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector<zzz").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector<WASH").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector<wash").match_row(self.row))
        # >=
        self.assertTrue(RowQuery.parse("sector>=WASH").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector>=wash").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector>=ZZZ").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector>=zzz").match_row(self.row))
        # >
        self.assertTrue(RowQuery.parse("sector>AAA").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector>aaa").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector>WASH").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector>wash").match_row(self.row))
        # ~
        self.assertTrue(RowQuery.parse("sector~^W").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector~W").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector~AS").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector~sh$").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector~W$").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector~AS$").match_row(self.row))
        # !~
        self.assertTrue(RowQuery.parse("sector!~^A").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector!~X").match_row(self.row))
        self.assertTrue(RowQuery.parse("sector!~W$").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector!~w").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector !~ w").match_row(self.row))
        # is empty
        self.assertTrue(RowQuery.parse("sector is not empty").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector is empty").match_row(self.row))
        # is number
        self.assertTrue(RowQuery.parse("sector is not number").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector is number").match_row(self.row))
        # is date
        self.assertTrue(RowQuery.parse("sector is not date").match_row(self.row))
        self.assertFalse(RowQuery.parse("sector is date").match_row(self.row))

    def test_whitespace(self):
        self.assertTrue(RowQuery.parse("adm1=coast").match_row(self.row))

    def test_dates(self):
        # =
        self.assertTrue(RowQuery.parse("date=2015-12-13").match_row(self.row))
        self.assertFalse(RowQuery.parse("date=2015-12-12").match_row(self.row))
        # <=
        self.assertTrue(RowQuery.parse("date<=2015-12-13").match_row(self.row))
        self.assertFalse(RowQuery.parse("date<=2015-12-12").match_row(self.row))
        # <
        self.assertTrue(RowQuery.parse("date<2015-12-14").match_row(self.row))
        self.assertFalse(RowQuery.parse("date<2015-12-13").match_row(self.row))
        # >=
        self.assertTrue(RowQuery.parse("date>=2015-12-13").match_row(self.row))
        self.assertFalse(RowQuery.parse("date>=2015-12-14").match_row(self.row))
        # >
        self.assertTrue(RowQuery.parse("date>2015-12-12").match_row(self.row))
        self.assertFalse(RowQuery.parse("date>2015-12-13").match_row(self.row))
        # is (not) date
        self.assertTrue(RowQuery.parse("date is date").match_row(self.row))
        self.assertFalse(RowQuery.parse("date is not date").match_row(self.row))

    def test_numbers(self):
        """Test that we're doing numeric rather than lexical comparison"""
        # =
        self.assertTrue(RowQuery.parse("affected=200").match_row(self.row))
        self.assertFalse(RowQuery.parse("affected=300").match_row(self.row))
        # <=
        self.assertTrue(RowQuery.parse("affected<=200").match_row(self.row))
        self.assertFalse(RowQuery.parse("affected<=199").match_row(self.row))
        # <
        self.assertTrue(RowQuery.parse("affected<1000").match_row(self.row))
        self.assertFalse(RowQuery.parse("affected<100").match_row(self.row))
        # >=
        self.assertTrue(RowQuery.parse("affected>=200").match_row(self.row))
        self.assertFalse(RowQuery.parse("affected>=201").match_row(self.row))
        # >
        self.assertTrue(RowQuery.parse("affected>9").match_row(self.row))
        self.assertFalse(RowQuery.parse("affected>900").match_row(self.row))
        # is (not) number
        self.assertTrue(RowQuery.parse("affected is number").match_row(self.row))
        self.assertFalse(RowQuery.parse("affected is not number").match_row(self.row))

    def test_number_conversion(self):
        # hexadecimal
        self.assertTrue(RowQuery.parse("affected>0x01").match_row(self.row))
        # exponential notation
        self.assertTrue(RowQuery.parse("affected<1.0e+06").match_row(self.row))
        # whitespace
        self.assertTrue(RowQuery.parse("inneed>400").match_row(self.row))
        self.assertTrue(RowQuery.parse("inneed<600").match_row(self.row))

    AGGREGATE_DATA = [
        ['#adm1', '#affected'],
        ['Coast', '100'],
        ['Plains', '300'],
        ['Mountains', '200']
    ]

    def test_is_min(self):
        source = hxl.data(self.AGGREGATE_DATA).cache()
        query = RowQuery.parse('#affected is min')
        self.assertTrue(query.needs_aggregate)
        query.calc_aggregate(source)
        self.assertEquals(100, query.value)
        for row in source:
            if query.match_row(row):
                self.assertEqual(100, float(row.get('#affected')))
            else:
                self.assertNotEqual(100, float(row.get('#affected')))

    def test_is_max(self):
        source = hxl.data(self.AGGREGATE_DATA).cache()
        query = RowQuery.parse('#affected is max')
        self.assertTrue(query.needs_aggregate)
        query.calc_aggregate(source)
        self.assertEquals(300, query.value)
        for row in source:
            if query.match_row(row):
                self.assertEqual(300, float(row.get('#affected')))
            else:
                self.assertNotEqual(300, float(row.get('#affected')))

# end
