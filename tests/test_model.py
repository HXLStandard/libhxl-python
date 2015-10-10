"""
Unit tests for the hxl.model module
David Megginson
October 2014

License: Public Domain
"""

import unittest
import hxl
from hxl.common import normalise_string
from hxl.model import TagPattern, Column, Row

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
        pattern = TagPattern('#Tag')
        self.assertTrue(pattern.match(self.column))
        pattern = TagPattern('#tag', include_attributes=['fOO'])
        self.assertTrue(pattern.match(self.column))

    def test_parse(self):
        pattern = TagPattern.parse('#tag+foo-xxx')
        self.assertEqual(pattern.tag, '#tag')
        self.assertTrue('foo' in pattern.include_attributes)

        pattern = TagPattern.parse('tag+foo-xxx')
        self.assertEqual(pattern.tag, '#tag')

        pattern = TagPattern.parse('   tag +foo  -xxx  ')
        self.assertEqual(pattern.tag, '#tag')
        self.assertEqual(['foo'], pattern.include_attributes)
        self.assertEqual(['xxx'], pattern.exclude_attributes)

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

    # TODO test generators


class TestColumn(unittest.TestCase):

    HXL_TAG = '#foo'
    ATTRIBUTES = {'en', 'bar'}
    HEADER_TEXT = 'Foo header'
    COLUMN_NUMBER = 5
    SOURCE_COLUMN_NUMBER = 7

    def setUp(self):
        self.column = Column(tag=TestColumn.HXL_TAG, attributes=TestColumn.ATTRIBUTES, header=TestColumn.HEADER_TEXT)

    def test_variables(self):
        self.assertEqual(TestColumn.HXL_TAG, self.column.tag)
        self.assertEqual(TestColumn.ATTRIBUTES, self.column.attributes)
        self.assertEqual(TestColumn.HEADER_TEXT, self.column.header)

    def test_display_tag(self):
        # order is not fixed
        #self.assertEqual(TestColumn.HXL_TAG + '+' + "+".join(TestColumn.ATTRIBUTES), self.column.display_tag)
        pass

    def test_case_insensitive(self):
        column = Column(tag='Foo', attributes=['X', 'y'])
        self.assertEquals('foo', column.tag)
        self.assertEquals(set(['x', 'y']), column.attributes)


class TestRow(unittest.TestCase):

    ROW_NUMBER = 5
    TAGS = ['#sector', '#org', '#country']
    CONTENT = ['Health', 'WFP', 'Liberia'];

    def setUp(self):
        columns = []
        for column_number, tag in enumerate(TestRow.TAGS):
            columns.append(Column(tag=tag))
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

    def test_get_skip_blanks(self):
        columns = [Column.parse(tag) for tag in ['#sector', '#org', '#org']]
        row = Row(columns=columns, values=['Health', '', 'WFP'])
        # Test that row.get() returns first non-blank value
        self.assertEqual('WFP', row.get('org'))

    def test_get_all(self):
        result = self.row.get_all('#org')
        self.assertTrue(type(result) is list)
        self.assertEqual(1, len(result))

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

# end
