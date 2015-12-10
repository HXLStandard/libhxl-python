"""
Unit tests for filters
David Megginson
April 2015

License: Public Domain
"""

import unittest

import hxl

#
# Base data for tests
#

DATA = [
    ['Organisation', 'Cluster', 'District', 'Count'],
    ['#org', '#sector', '#adm1', '#meta+count'],
    ['NGO A', 'WASH', 'Coast', '200'],
    ['NGO B', 'Education', 'Plains', '100'],
    ['NGO B', 'Education', 'Coast', '300']
]


class AbstractFilterTest(unittest.TestCase):
    """Base class for all tests."""

    def setUp(self):
        # use a cache filter so that we can run tests multiple times
        self.source = hxl.data(DATA).cache()


#
# Test classes
#

class TestAddColumnsFilter(AbstractFilterTest):

    spec = 'Country#country=Country A'

    def test_before(self):
        self.assertEqual(
            ['#country', '#org', '#sector', '#adm1', '#meta'],
            self.source.add_columns(self.spec, True).tags
        )

    def test_after(self):
        self.assertEqual(
            ['#org', '#sector', '#adm1', '#meta', '#country'],
            self.source.add_columns(self.spec).tags
        )

    def test_headers(self):
        self.assertEqual(
            DATA[0] + ['Country'],
            self.source.add_columns(self.spec).headers
        )

    def test_rows(self):
        self.assertEqual(
            [values + ['Country A'] for values in DATA[2:]],
            self.source.add_columns(self.spec).values
        )


class TestAppendFilter(AbstractFilterTest):

    APPEND_DATA = [
        ['Org', 'Targeted', 'Sector 1', 'Sector 2'],
        ['#org', '#targeted', '#sector', '#sector'],
        ['NGO A', '200', 'WASH', ''],
        ['NGO C', '500', 'Health', 'Food']
    ]

    COMBINED_DATA = [
        ['Organisation', 'Cluster', 'District', 'Count', 'Targeted', 'Sector 2'],
        ['#org', '#sector', '#adm1', '#meta+count', '#targeted', '#sector'],
        ['NGO A', 'WASH', 'Coast', '200', '', ''],
        ['NGO B', 'Education', 'Plains', '100', '', ''],
        ['NGO B', 'Education', 'Coast', '300', '', ''],
        ['NGO A', 'WASH', '', '', '200', ''],
        ['NGO C', 'Health', '', '', '500', 'Food']
    ]

    COMBINED_DATA_ORIG_COLUMNS = [
        ['Organisation', 'Cluster', 'District', 'Count'],
        ['#org', '#sector', '#adm1', '#meta+count'],
        ['NGO A', 'WASH', 'Coast', '200'],
        ['NGO B', 'Education', 'Plains', '100'],
        ['NGO B', 'Education', 'Coast', '300'],
        ['NGO A', 'WASH', '', ''],
        ['NGO C', 'Health', '', '']
    ]

    COMBINED_DATA_FILTERED = [
        ['Organisation', 'Cluster', 'District', 'Count', 'Targeted', 'Sector 2'],
        ['#org', '#sector', '#adm1', '#meta+count', '#targeted', '#sector'],
        ['NGO A', 'WASH', 'Coast', '200', '', ''],
        ['NGO B', 'Education', 'Plains', '100', '', ''],
        ['NGO B', 'Education', 'Coast', '300', '', ''],
        ['NGO C', 'Health', '', '', '500', 'Food']
    ]

    def setUp(self):
        super(TestAppendFilter, self).setUp()
        self.append_source = hxl.data(TestAppendFilter.APPEND_DATA)
    
    def test_headers(self):
        self.assertEqual(self.COMBINED_DATA[0], self.source.append(self.append_source).headers)
        self.assertEqual(self.COMBINED_DATA_ORIG_COLUMNS[0], self.source.append(self.append_source, False).headers)

    def test_columns(self):
        self.assertEqual(self.COMBINED_DATA[1], self.source.append(self.append_source).display_tags)

    def test_values(self):
        self.assertEqual(self.COMBINED_DATA[2:], self.source.append(self.append_source).values)

    def test_queries(self):
        self.assertEqual(self.COMBINED_DATA_FILTERED[2:], self.source.append(self.append_source, queries='sector!=WASH').values)

        
class TestCacheFilter(AbstractFilterTest):

    def test_headers(self):
        self.assertEqual(DATA[0], self.source.cache().headers)

    def test_columns(self):
        self.assertEqual(DATA[1], self.source.cache().display_tags)

    def test_rows(self):
        self.assertEqual(DATA[2:], self.source.cache().values)

    def test_repeat(self):
        # Test repeating a cache filter directly
        source = hxl.data(DATA).cache()
        rows1 = [row.values for row in source]
        rows2 = [row.values for row in source]
        self.assertEqual(3, len(rows1))
        self.assertEqual(rows1, rows2)

    def test_repeat_sub(self):
        # Test repeating a cache filter backing another filter
        source = hxl.data(DATA).cache().with_rows('org=NGO A')
        rows1 = [row.values for row in source]
        rows2 = [row.values for row in source]
        self.assertEqual(1, len(rows1))
        self.assertEqual(rows1, rows2)


class TestCleanFilter(AbstractFilterTest):

    def test_whitespace(self):
        DATA_IN = [
            ['Organisation', 'Cluster', 'District', 'Count'],
            ['#org', '#sector', '#adm1', '#meta+count'],
            ['NGO A', '  WASH', 'Coast', '200'],
            ['NGO B', 'Education  ', 'Plains', '100'],
            ['NGO B', 'Child    Protection', 'Coast', '300']
        ]
        DATA_OUT = [
            ['NGO A', 'WASH', 'Coast', '200'],
            ['NGO B', 'Education', 'Plains', '100'],
            ['NGO B', 'Child Protection', 'Coast', '300']
        ]
        self.assertEqual(DATA_OUT, hxl.data(DATA_IN).clean_data(whitespace='sector').values)
        
    def test_numbers(self):
        DATA_IN = [
            ['Organisation', 'Cluster', 'District', 'Count'],
            ['#org', '#sector', '#adm1', '#meta+count'],
            ['NGO A', 'WASH', 'Coast', '  200'],
            ['NGO B', 'Education', 'Plains', '1,100 '],
            ['NGO B', 'Child Protection', 'Coast', '300.']
        ]
        DATA_OUT = [
            ['NGO A', 'WASH', 'Coast', '200'],
            ['NGO B', 'Education', 'Plains', '1100'],
            ['NGO B', 'Child Protection', 'Coast', '300']
        ]
        self.assertEqual(DATA_OUT, hxl.data(DATA_IN).clean_data(number='meta+count').values)
        
    def test_dates(self):
        DATA_IN = [
            ['Organisation', 'Cluster', 'District', 'Date'],
            ['#org', '#sector', '#adm1', '#date'],
            ['NGO A', 'WASH', 'Coast', 'January 1 2015'],
            ['NGO B', 'Education', 'Plains', '1/1/15'],
            ['NGO B', 'Child Protection', 'Coast', '1 Jan/15']
        ]
        DATA_OUT = [
            ['NGO A', 'WASH', 'Coast', '2015-01-01'],
            ['NGO B', 'Education', 'Plains', '2015-01-01'],
            ['NGO B', 'Child Protection', 'Coast', '2015-01-01']
        ]
        self.assertEqual(DATA_OUT, hxl.data(DATA_IN).clean_data(date='date').values)
        
    def test_upper_case(self):
        DATA_OUT = [
            ['NGO A', 'WASH', 'Coast', '200'],
            ['NGO B', 'EDUCATION', 'Plains', '100'],
            ['NGO B', 'EDUCATION', 'Coast', '300']
        ]
        self.assertEqual(DATA_OUT, self.source.clean_data(upper='sector').values)

    def test_lower_case(self):
        DATA_OUT = [
            ['NGO A', 'wash', 'Coast', '200'],
            ['NGO B', 'education', 'Plains', '100'],
            ['NGO B', 'education', 'Coast', '300']
        ]
        self.assertEqual(DATA_OUT, self.source.clean_data(lower='sector').values)

    def test_queries(self):
        DATA_OUT = [
            ['NGO A', 'WASH', 'Coast', '200'],
            ['NGO B', 'education', 'Plains', '100'],
            ['NGO B', 'Education', 'Coast', '300']
        ]
        self.assertEqual(DATA_OUT, self.source.clean_data(lower='sector', queries='adm1=Plains').values)


class TestColumnFilter(AbstractFilterTest):

    def test_with_columns(self):
        expected = ['#sector']
        self.assertEqual(expected, self.source.with_columns('#sector').tags)
        self.assertEqual(expected, self.source.with_columns(['#sector']).tags)

    def test_without_columns(self):
        expected = ['#org', '#adm1', '#meta']
        self.assertEqual(expected, self.source.without_columns('#sector').tags)
        self.assertEqual(expected, self.source.without_columns(['#sector']).tags)


class TestCountFilter(AbstractFilterTest):

    def test_tags(self):
        expected = ['#sector', '#meta']
        self.assertEqual(expected, self.source.count('#sector').tags)

    def test_values(self):
        expected = [
            ['Education', 2],
            ['WASH', 1]
        ]
        self.assertEqual(expected, self.source.count('#sector').values)

    def test_missing_column(self):
        expected_headers = [None, 'Cluster', 'Count']
        expected_tags = [None, '#sector', '#meta']
        source = self.source.count('region,sector')
        self.assertEqual(expected_headers, source.headers)
        self.assertEqual(expected_tags, source.tags)

    def test_aggregation_tags(self):
        expected = ['#sector', '#meta+count', '#meta+sum', '#meta+average', '#meta+min', '#meta+max']
        self.assertEqual(expected, self.source.count('#sector', '#meta').display_tags)

    def test_aggregation_values(self):
        expected = [
            ['Education', 2, 400, 200, 100, 300],
            ['WASH', 1, 200, 200, 200, 200]
        ]
        self.assertEqual(expected, self.source.count('#sector', '#meta').values)

    def test_custom_tag(self):
        input = [
            ['Organisation'],
            ['#org'],
            ['UNICEF'],
            ['WHO'],
            ['UNICEF']
        ]
        expected = [
            ['Organisation', 'Activities'],
            ['#org', '#output+activities'],
            ['UNICEF', 2],
            ['WHO', 1]
        ]
        source = hxl.data(input).count('org', count_spec='Activities#output+activities')
        self.assertEqual(expected, [row for row in source.gen_raw(show_headers=True)])

    def test_queries(self):
        expected = [
            ['Education', 1],
            ['WASH', 1]
        ]
        self.assertEqual(expected, self.source.count('#sector', queries='adm1=Coast').values)


class TestDeduplicationFilter (AbstractFilterTest):

    DATA_IN = DATA + DATA[2:] # double up the input data

    DATA_OUT = DATA # should be the same as the original

    DATA_OUT_FILTERED = [
        ['Organisation', 'Cluster', 'District', 'Count'],
        ['#org', '#sector', '#adm1', '#meta+count'],
        ['NGO A', 'WASH', 'Coast', '200'],
        ['NGO B', 'Education', 'Plains', '100'],
        ['NGO B', 'Education', 'Coast', '300'],
        ['NGO A', 'WASH', 'Coast', '200']
    ]

    def setUp(self):
        # use a cache filter so that we can run tests multiple times
        self.source = hxl.data(self.DATA_IN)

    def test_dedup(self):
        self.assertEqual(self.DATA_OUT[2:], self.source.dedup().values)

    def test_queries(self):
        self.assertEqual(self.DATA_OUT_FILTERED[2:], self.source.dedup(queries='sector=Education').values)


class TestMergeDataFilter(AbstractFilterTest):

    MERGE_IN = [
        ['District', 'P-code'],
        ['#adm1', '#adm1+code'],
        ['coaST', '001'],         # deliberate case variation
        ['   Plains', '002']      # deliberate whitespace variation
    ]

    MERGE_OUT = [
        ['Organisation', 'Cluster', 'District', 'Count', 'P-code'],
        ['#org', '#sector', '#adm1', '#meta+count', '#adm1+code'],
        ['NGO A', 'WASH', 'Coast', '200', '001'],
        ['NGO B', 'Education', 'Plains', '100', '002'],
        ['NGO B', 'Education', 'Coast', '300', '001']
    ]

    MERGE_EXTRA = [
        ['P-code', 'Population'],
        ['#adm1+code', '#population'],
        ['001', '10000'],
        ['002', ''] # deliberately blank
    ]

    MERGE_EXTRA_OUT = [
        ['Organisation', 'Cluster', 'District', 'Count', 'P-code', 'Population'],
        ['#org', '#sector', '#adm1', '#meta+count', '#adm1+code', '#population'],
        ['NGO A', 'WASH', 'Coast', '200', '001', '10000'],
        ['NGO B', 'Education', 'Plains', '100', '002', ''],
        ['NGO B', 'Education', 'Coast', '300', '001', '10000']
    ]

    MERGE_DISPLACED_KEY = [
        ['District 1', 'District 2', 'P-code'],
        ['#adm1', '#adm1', '#adm1+code'],
        ['coaST', 'xxx', '001'],         # deliberate case variation
        ['yyy', '   Plains', '002']      # deliberate whitespace variation
    ]

    def setUp(self):
        super(TestMergeDataFilter, self).setUp()
        self.merged = self.source.merge_data(hxl.data(self.MERGE_IN), '#adm1-code', '#adm1+code')

    def test_headers(self):
        self.assertEqual(self.MERGE_OUT[0], self.merged.headers)

    def test_tags(self):
        self.assertEqual(self.MERGE_OUT[1], self.merged.display_tags)

    def test_values(self):
        self.assertEqual(self.MERGE_OUT[2:], self.merged.values)

    def test_chaining(self):
        merged_extra = self.merged.merge_data(hxl.data(self.MERGE_EXTRA), '#adm1+code', '#population')
        self.assertEqual(self.MERGE_EXTRA_OUT[2:], merged_extra.values)

    def test_blank_merge(self):
        data1 = hxl.data([
            ['#sector', '#org+name', '#org+name'],
            ['Health', '', 'Red Cross']
            ])
        data2 = hxl.data([
            ['#org+name', '#org+code'],
            ['XX', 'YY'],
            ['Red Cross', 'IFRC']
            ])
        expected = [
            ['#sector', '#org+name', '#org+name', '#org+code'],
            ['Health', '', 'Red Cross', 'IFRC']
            ]
        merged = data1.merge_data(data2, '#org+name', '#org+code')
        self.assertEqual(expected[1:], merged.values)

    # def test_values_displaced_key(self):
    #     """Test that the filter scans all candidate keys."""
    #     data1 = hxl.data([
    #         ['#sector', '#org+name', '#org+name'],
    #         ['Health', 'xxx', 'Red Cross']
    #         ])
    #     data2 = hxl.data([
    #         ['#org+name', '#org+code'],
    #         ['XX', 'YY'],
    #         ['Red Cross', 'IFRC']
    #         ])
    #     expected = [
    #         ['#sector', '#org+name', '#org+name', '#org+code'],
    #         ['Health', 'xxx', 'Red Cross', 'IFRC']
    #         ]
    #     merged = data1.merge_data(data2, '#org+name', '#org+code')
    #     self.assertEqual(expected[1:], merged.values)

    def test_queries(self):
        MERGE_IN = [
            ['District', 'P-code', 'Foo'],
            ['#adm1', '#adm1+code', '#foo'],
            ['Coast', '003', 'hack'],
            ['Coast', '001', 'bar'],
            ['Plains', '002', 'hack']
        ]
        MERGE_OUT = [
            ['Organisation', 'Cluster', 'District', 'Count', 'P-code'],
            ['#org', '#sector', '#adm1', '#meta+count', '#adm1+code'],
            ['NGO A', 'WASH', 'Coast', '200', '003'],
            ['NGO B', 'Education', 'Plains', '100', '002'],
            ['NGO B', 'Education', 'Coast', '300', '003']
        ]
        merged = self.source.merge_data(hxl.data(MERGE_IN), 'adm1-code', 'adm1+code', queries='foo=hack')
        self.assertEqual(MERGE_OUT[2:], merged.values)


class TestRenameFilter(AbstractFilterTest):

    spec = '#sector:Sub-sector#subsector'

    def test_tags(self):
        self.assertEqual(
            ['#org', '#subsector', '#adm1', '#meta'],
            self.source.rename_columns(self.spec).tags
        )

    def test_headers(self):
        self.assertEqual(
            ['Organisation', 'Sub-sector', 'District', 'Count'],
            self.source.rename_columns(self.spec).headers
        )


class TestReplaceFilter(AbstractFilterTest):

    def test_basic_replace(self):
        # should be replaced
        self.assertEqual('Plains District', self.source.replace_data('Plains', 'Plains District', '#adm1').values[1][2])

    def test_column_ignored(self):
        # shouldn't be replaced
        self.assertEqual('Plains', self.source.replace_data('Plains', 'Plains District', '#org').values[1][2])

    def test_normalised_replace(self):
        # should ignore character case
        self.assertEqual('Plains District', self.source.replace_data('  PLainS   ', 'Plains District', '#adm1').values[1][2])

    def test_all_columns_replace(self):
        # should be replaced (anywhere in row)
        self.assertEqual('Plains District', self.source.replace_data('  PLainS   ', 'Plains District').values[1][2])

    def test_regex_replace(self):
        # not a regex
        self.assertEqual('Plains', self.source.replace_data(r'ains$', 'ains District', '#adm1', use_regex=False).values[1][2])

        # regex
        self.assertEqual('Plains District', self.source.replace_data(r'ains$', 'ains District', '#adm1', True).values[1][2])

        # non-matching regex
        self.assertEqual('Plains', self.source.replace_data(r'^ains', 'ains District', '#adm1', True).values[1][2])

        # substitution
        self.assertEqual('Plains District', self.source.replace_data('(ains)$', r'\1 District', '#adm1', use_regex=True).values[1][2])

    def test_replace_after_append(self):
        # will test with different lengths of value arrays
        NEW_DATA = [
            ['#adm1', '#org'],
            ['Mountains', 'NGO C'],
            ['Plains', 'NGO A']
        ]
        MAPPING = [
            ['#x_pattern', '#x_substitution', '#x_tag'],
            ['NGO C', 'NGO Charlie', 'org']
        ]
        source = self.source.append(NEW_DATA)
        self.assertEqual('NGO Charlie', source.replace_data_map(hxl.data(MAPPING)).values[3][0])

    def test_queries(self):
        result = self.source.replace_data('Coast', 'Coastal District', '#adm1', queries='org=NGO A')
        self.assertEqual('Coastal District', result.values[0][2])
        self.assertEqual('Coast', result.values[2][2])


class TestRowCountFilter(AbstractFilterTest):

    def test_count(self):
        counter = self.source.row_counter()
        for row in counter:
            pass
        self.assertEqual(3, counter.row_count)

    def test_queries(self):
        counter = self.source.row_counter('org=NGO B')
        for row in counter:
            pass
        self.assertEqual(2, counter.row_count)

        
class TestRowFilter(AbstractFilterTest):

    def test_with_rows(self):
        self.assertEqual(DATA[3:], self.source.with_rows(['#sector=education']).values)
        self.assertEqual(DATA[3:], self.source.with_rows('#sector=education').values)

    def test_without_rows(self):
        self.assertEqual(DATA[3:], self.source.without_rows(['#sector=wash']).values)
        self.assertEqual(DATA[3:], self.source.without_rows('#sector=wash').values)


class TestSortFilter(AbstractFilterTest):

    def test_forward(self):
        self.assertEqual(sorted(DATA[2:]), self.source.sort().values)

    def test_backward(self):
        self.assertEqual(sorted(DATA[2:], reverse=True), self.source.sort(reverse=True).values)

    def test_custom_keys(self):
        def key(r):
            return [r[2], r[1]]
        self.assertEqual(sorted(DATA[2:], key=key), self.source.sort(['#adm1', '#sector']).values)

    def test_numeric(self):
        def key(r):
            return float(r[3])
        self.assertEqual(sorted(DATA[2:], key=key), self.source.sort('#meta+count').values)


class TestChaining(AbstractFilterTest):

    def test_rowfilter_countfilter(self):
        self.assertEqual(
            [['NGO A', 1]],
            self.source.with_rows('#sector=wash').count('#org').values
        )
        self.assertEqual(
            [['NGO B', 2]],
            self.source.without_rows('#sector=wash').count('#org').values
        )


