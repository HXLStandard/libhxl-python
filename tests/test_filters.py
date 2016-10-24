# coding=utf-8
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
    ['#org', '#sector+list', '#adm1', '#meta+count'],
    ['NGO A', 'WASH', 'Coast', '200'],
    ['NGO B', 'Education', 'Plains', '100'],
    ['NGO B', 'Education', 'Coast', '300'],
    ['NGO A', 'Education, Protection', 'Plains', '150'],
]


class AbstractBaseFilterTest(unittest.TestCase):
    """Base class for all tests."""

    def setUp(self):
        # use a cache filter so that we can run tests multiple times
        self.source = hxl.data(DATA).cache()


#
# Test compiling from a recipe
#

class TestRecipe(AbstractBaseFilterTest):

    def test_spec(self):
        filtered = hxl.data({
            'data-source': DATA
        })
        self.assertEqual(filtered.values, DATA[2:])

    def test_recursive(self):
        # try appending a dataset to itself
        data_source = {
            'data-source': DATA
        }
        filtered = hxl.data(data_source).recipe([
            {
                'filter': 'append',
                'append_source': hxl.data(data_source)
            }
        ])
        self.assertEqual(filtered.values, DATA[2:] + DATA[2:])


    def test_multiple(self):
        filtered = self.source.recipe([
            {
                'filter': 'without_rows',
                'queries': 'adm1=Plains'
            },
            {
                'filter': 'sort',
                'keys': 'sector'
            }
        ])
        self.assertEqual(filtered.values, [DATA[4], DATA[2]])

    def test_json(self):
        # test using a literal JSON string for the recipe
        filtered = self.source.recipe('{"filter": "cache"}')
        self.assertEqual(type(filtered).__name__, 'CacheFilter')

    # test individual filter types in the recipe
    # all of these specify only a single filter as a dict
    
    def test_add_columns(self):
        filtered = self.source.recipe({
            'filter': 'add_columns',
            'specs': '#foo=bar'
        })
        self.assertEqual(type(filtered).__name__, 'AddColumnsFilter')

    def test_append(self):
        filtered = self.source.recipe({
            'filter': 'append',
            'append_source': DATA
        })
        self.assertEqual(type(filtered).__name__, 'AppendFilter')

    def test_cache(self):
        filtered = self.source.recipe({'filter': 'cache'})
        self.assertEqual(type(filtered).__name__, 'CacheFilter')

    def test_clean_data(self):
        filtered = self.source.recipe({'filter': 'clean_data'})
        self.assertEqual(type(filtered).__name__, 'CleanDataFilter')

    def test_count(self):
        filtered = self.source.recipe({
            'filter': 'count',
            'patterns': 'sector'
        })
        self.assertEqual(type(filtered).__name__, 'CountFilter')

    def test_dedup(self):
        filtered = self.source.recipe({'filter': 'dedup'})
        self.assertEqual(type(filtered).__name__, 'DeduplicationFilter')

    def test_explode(self):
        filtered = self.source.recipe({'filter': 'explode'})
        self.assertEqual(type(filtered).__name__, 'ExplodeFilter')

    def test_merge_data(self):
        filtered = self.source.recipe({
            'filter': 'merge_data',
            'merge_source': DATA,
            'keys': 'sector',
            'tags': 'org'
        })
        self.assertEqual(type(filtered).__name__, 'MergeDataFilter')

    def test_rename_columns(self):
        filtered = self.source.recipe({
            'filter': 'rename_columns',
            'specs': '#foo:#bar'
        })
        self.assertEqual(type(filtered).__name__, 'RenameFilter')

    def test_replace_data(self):
        filtered = self.source.recipe({
            'filter': 'replace_data',
            'original': 'foo',
            'replacement': 'bar'
        })
        self.assertEqual(type(filtered).__name__, 'ReplaceDataFilter')

    def test_replace_data_map(self):
        filtered = self.source.recipe({
            'filter': 'replace_data_map',
            'map_source': [
                ['#x_pattern', '#x_substitution', '#x_tag'],
                ['NGO C', 'NGO Charlie', 'org']
            ]
        })
        self.assertEqual(type(filtered).__name__, 'ReplaceDataFilter')

    def test_sort(self):
        filtered = self.source.recipe({'filter': 'sort'})
        self.assertEqual(type(filtered).__name__, 'SortFilter')

    def test_with_columns(self):
        filtered = self.source.recipe({
            'filter': 'with_columns',
            'whitelist': 'sector'
        })
        self.assertEqual(type(filtered).__name__, 'ColumnFilter')

    def test_with_rows(self):
        filtered = self.source.recipe({
            'filter': 'with_rows',
            'queries': 'sector=WASH'
        })
        self.assertEqual(type(filtered).__name__, 'RowFilter')

    def test_without_columns(self):
        filtered = self.source.recipe({
            'filter': 'without_columns',
            'blacklist': 'sector'
        })
        self.assertEqual(type(filtered).__name__, 'ColumnFilter')

    def test_without_rows(self):
        filtered = self.source.recipe({
            'filter': 'without_rows',
            'queries': 'sector=WASH'
        })
        self.assertEqual(type(filtered).__name__, 'RowFilter')


#
# Test classes
#

class TestAddColumnsFilter(AbstractBaseFilterTest):

    spec = 'Country#country=Country A'

    def test_before(self):
        self.assertEqual(
            ['#country', '#org', '#sector+list', '#adm1', '#meta+count'],
            self.source.add_columns(self.spec, True).display_tags
        )

    def test_after(self):
        self.assertEqual(
            ['#org', '#sector+list', '#adm1', '#meta+count', '#country'],
            self.source.add_columns(self.spec).display_tags
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

    def test_dynamic_value(self):
        self.assertEqual(
            [values + [values[1]] for values in DATA[2:]],
            self.source.add_columns('Country#country={{#sector}}').values
        )


class TestAppendFilter(AbstractBaseFilterTest):

    APPEND_DATA = [
        ['Org', 'Targeted', 'Sector 1', 'Sector 2'],
        ['#org', '#targeted', '#sector+list', '#sector+list'],
        ['NGO A', '200', 'WASH', ''],
        ['NGO C', '500', 'Health', 'Food']
    ]

    COMBINED_DATA = [
        ['Organisation', 'Cluster', 'District', 'Count', 'Targeted', 'Sector 2'],
        ['#org', '#sector+list', '#adm1', '#meta+count', '#targeted', '#sector+list'],
        ['NGO A', 'WASH', 'Coast', '200', '', ''],
        ['NGO B', 'Education', 'Plains', '100', '', ''],
        ['NGO B', 'Education', 'Coast', '300', '', ''],
        ['NGO A', 'Education, Protection', 'Plains', '150', '', ''],
        ['NGO A', 'WASH', '', '', '200', ''],
        ['NGO C', 'Health', '', '', '500', 'Food']
    ]

    COMBINED_DATA_ORIG_COLUMNS = [
        ['Organisation', 'Cluster', 'District', 'Count'],
        ['#org', '#sector+list', '#adm1', '#meta+count'],
        ['NGO A', 'WASH', 'Coast', '200'],
        ['NGO B', 'Education', 'Plains', '100'],
        ['NGO B', 'Education', 'Coast', '150'],
        ['NGO A', 'WASH', '', ''],
        ['NGO C', 'Health', '', '']
    ]

    COMBINED_DATA_FILTERED = [
        ['Organisation', 'Cluster', 'District', 'Count', 'Targeted', 'Sector 2'],
        ['#org', '#sector+list', '#adm1', '#meta+count', '#targeted', '#sector+list'],
        ['NGO A', 'WASH', 'Coast', '200', '', ''],
        ['NGO B', 'Education', 'Plains', '100', '', ''],
        ['NGO B', 'Education', 'Coast', '300', '', ''],
        ['NGO A', 'Education, Protection', 'Plains', '150', '', ''],
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

        
class TestCacheFilter(AbstractBaseFilterTest):

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
        self.assertEqual(4, len(rows1))
        self.assertEqual(rows1, rows2)

    def test_repeat_sub(self):
        # Test repeating a cache filter backing another filter
        source = hxl.data(DATA).cache().with_rows('org=NGO A')
        rows1 = [row.values for row in source]
        rows2 = [row.values for row in source]
        self.assertEqual(2, len(rows1))
        self.assertEqual(rows1, rows2)


class TestCleanFilter(AbstractBaseFilterTest):

    def test_whitespace(self):
        DATA_IN = [
            ['Organisation', 'Cluster', 'District', 'Count'],
            ['#org', '#sector+list', '#adm1', '#meta+count'],
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
            ['#org', '#sector+list', '#adm1', '#meta+count'],
            ['NGO A', 'WASH', 'Coast', '  200'],
            ['NGO B', 'Education', 'Plains', '1,100 '],
            ['NGO B', 'Child Protection', 'Coast', '300.'],
            ['NGO A', 'Logistics', 'Coast', '1.7E5']
        ]
        DATA_OUT = [
            ['NGO A', 'WASH', 'Coast', '200'],
            ['NGO B', 'Education', 'Plains', '1100'],
            ['NGO B', 'Child Protection', 'Coast', '300'],
            ['NGO A', 'Logistics', 'Coast', '170000']
        ]
        self.assertEqual(DATA_OUT, hxl.data(DATA_IN).clean_data(number='meta+count').values)
        
    def test_dates(self):
        DATA_IN = [
            ['Organisation', 'Cluster', 'District', 'Date'],
            ['#org', '#sector+list', '#adm1', '#date'],
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
            ['NGO B', 'EDUCATION', 'Coast', '300'],
            ['NGO A', 'EDUCATION, PROTECTION', 'Plains', '150'],
        ]
        self.assertEqual(DATA_OUT, self.source.clean_data(upper='sector').values)

    def test_lower_case(self):
        DATA_OUT = [
            ['NGO A', 'wash', 'Coast', '200'],
            ['NGO B', 'education', 'Plains', '100'],
            ['NGO B', 'education', 'Coast', '300'],
            ['NGO A', 'education, protection', 'Plains', '150']
        ]
        self.assertEqual(DATA_OUT, self.source.clean_data(lower='sector').values)

    def test_queries(self):
        DATA_OUT = [
            ['NGO A', 'WASH', 'Coast', '200'],
            ['NGO B', 'education', 'Plains', '100'],
            ['NGO B', 'Education', 'Coast', '300'],
            ['NGO A', 'education, protection', 'Plains', '150']
        ]
        self.assertEqual(DATA_OUT, self.source.clean_data(lower='sector', queries='adm1=Plains').values)


class TestColumnFilter(AbstractBaseFilterTest):

    def test_with_columns(self):
        expected = ['#sector+list']
        self.assertEqual(expected, self.source.with_columns('#sector').display_tags)
        self.assertEqual(expected, self.source.with_columns(['#sector']).display_tags)

    def test_without_columns(self):
        expected = ['#org', '#adm1', '#meta+count']
        self.assertEqual(expected, self.source.without_columns('#sector').display_tags)
        self.assertEqual(expected, self.source.without_columns(['#sector']).display_tags)


class TestCountFilter(AbstractBaseFilterTest):

    def test_tags(self):
        expected = ['#sector+list', '#meta+count']
        self.assertEqual(expected, self.source.count('#sector').display_tags)

    def test_values(self):
        expected = [
            ['Education', 2],
            ['Education, Protection', 1],
            ['WASH', 1],
        ]
        self.assertEqual(expected, self.source.count('#sector').values)

    def test_missing_column(self):
        expected_headers = [None, 'Cluster', 'Count']
        expected_tags = ['', '#sector+list', '#meta+count']
        source = self.source.count('region,sector')
        self.assertEqual(expected_headers, source.headers)
        self.assertEqual(expected_tags, source.display_tags)

    def test_aggregation_tags(self):
        expected = ['#sector+list', '#meta+count', '#meta+sum', '#meta+average', '#meta+min', '#meta+max']
        self.assertEqual(expected, self.source.count('#sector', '#meta').display_tags)

    def test_aggregation_values(self):
        expected = [
            ['Education', 2, 400, 200, 100, 300],
            ['Education, Protection', 1, 150, 150, 150, 150],
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


class TestDeduplicationFilter (AbstractBaseFilterTest):

    DATA_IN = DATA + DATA[2:] # double up the input data

    DATA_OUT = DATA # should be the same as the original

    DATA_OUT_FILTERED = [
        ['Organisation', 'Cluster', 'District', 'Count'],
        ['#org', '#sector+list', '#adm1', '#meta+count'],
        ['NGO A', 'WASH', 'Coast', '200'],
        ['NGO B', 'Education', 'Plains', '100'],
        ['NGO B', 'Education', 'Coast', '300'],
        ['NGO A', 'Education, Protection', 'Plains', '150'],
        ['NGO A', 'WASH', 'Coast', '200'],
        ['NGO A', 'Education, Protection', 'Plains', '150'],
    ]

    def setUp(self):
        # use a cache filter so that we can run tests multiple times
        self.source = hxl.data(self.DATA_IN)

    def test_dedup(self):
        self.assertEqual(self.DATA_OUT[2:], self.source.dedup().values)

    def test_queries(self):
        self.assertEqual(self.DATA_OUT_FILTERED[2:], self.source.dedup(queries='sector=Education').values)


class TestMergeDataFilter(AbstractBaseFilterTest):

    MERGE_IN = [
        ['District', 'P-code'],
        ['#adm1', '#adm1+code'],
        ['coaST', '001'],         # deliberate case variation
        ['   Pláins', '002']      # deliberate whitespace variation and accent
    ]

    MERGE_OUT = [
        ['Organisation', 'Cluster', 'District', 'Count', 'P-code'],
        ['#org', '#sector+list', '#adm1', '#meta+count', '#adm1+code'],
        ['NGO A', 'WASH', 'Coast', '200', '001'],
        ['NGO B', 'Education', 'Plains', '100', '002'],
        ['NGO B', 'Education', 'Coast', '300', '001'],
        ['NGO A', 'Education, Protection', 'Plains', '150', '002'],
    ]

    MERGE_EXTRA = [
        ['P-code', 'Population'],
        ['#adm1+code', '#population'],
        ['001', '10000'],
        ['002', ''] # deliberately blank
    ]

    MERGE_EXTRA_OUT = [
        ['Organisation', 'Cluster', 'District', 'Count', 'P-code', 'Population'],
        ['#org', '#sector+list', '#adm1', '#meta+count', '#adm1+code', '#population'],
        ['NGO A', 'WASH', 'Coast', '200', '001', '10000'],
        ['NGO B', 'Education', 'Plains', '100', '002', ''],
        ['NGO B', 'Education', 'Coast', '300', '001', '10000'],
        ['NGO A', 'Education, Protection', 'Plains', '150', '002', ''],
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
            ['#sector+list', '#org+name', '#org+name'],
            ['Health', '', 'Red Cross']
            ])
        data2 = hxl.data([
            ['#org+name', '#org+code'],
            ['XX', 'YY'],
            ['Red Cross', 'IFRC']
            ])
        expected = [
            ['#sector+list', '#org+name', '#org+name', '#org+code'],
            ['Health', '', 'Red Cross', 'IFRC']
            ]
        merged = data1.merge_data(data2, '#org+name', '#org+code')
        self.assertEqual(expected[1:], merged.values)

    # def test_values_displaced_key(self):
    #     """Test that the filter scans all candidate keys."""
    #     data1 = hxl.data([
    #         ['#sector+list', '#org+name', '#org+name'],
    #         ['Health', 'xxx', 'Red Cross']
    #         ])
    #     data2 = hxl.data([
    #         ['#org+name', '#org+code'],
    #         ['XX', 'YY'],
    #         ['Red Cross', 'IFRC']
    #         ])
    #     expected = [
    #         ['#sector+list', '#org+name', '#org+name', '#org+code'],
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
            ['#org', '#sector+list', '#adm1', '#meta+count', '#adm1+code'],
            ['NGO A', 'WASH', 'Coast', '200', '003'],
            ['NGO B', 'Education', 'Plains', '100', '002'],
            ['NGO B', 'Education', 'Coast', '300', '003'],
            ['NGO A', 'Education, Protection', 'Plains', '150', '002'],
        ]
        merged = self.source.merge_data(hxl.data(MERGE_IN), 'adm1-code', 'adm1+code', queries='foo=hack')
        self.assertEqual(MERGE_OUT[2:], merged.values)


class TestRenameFilter(AbstractBaseFilterTest):

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


class TestReplaceFilter(AbstractBaseFilterTest):

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
        self.assertEqual('NGO Charlie', source.replace_data_map(hxl.data(MAPPING)).values[4][0])

    def test_queries(self):
        result = self.source.replace_data('Coast', 'Coastal District', '#adm1', queries='org=NGO A')
        self.assertEqual('Coastal District', result.values[0][2])
        self.assertEqual('Coast', result.values[2][2])


class TestRowCountFilter(AbstractBaseFilterTest):

    def test_count(self):
        counter = self.source.row_counter()
        for row in counter:
            pass
        self.assertEqual(4, counter.row_count)

    def test_queries(self):
        counter = self.source.row_counter('org=NGO B')
        for row in counter:
            pass
        self.assertEqual(2, counter.row_count)

        
class TestRowFilter(AbstractBaseFilterTest):

    def test_with_rows(self):
        self.assertEqual(DATA[3:5], self.source.with_rows(['#sector=education']).values)
        self.assertEqual(DATA[3:5], self.source.with_rows('#sector=education').values)

    def test_without_rows(self):
        self.assertEqual(DATA[3:], self.source.without_rows(['#sector=wash']).values)
        self.assertEqual(DATA[3:], self.source.without_rows('#sector=wash').values)

    def test_masked(self):
        self.assertEqual(DATA[2:], self.source.with_rows('sector=education', mask='org=ngo b').values)


class TestSortFilter(AbstractBaseFilterTest):

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


class TestExplodeFilter(AbstractBaseFilterTest):

    # deliberate variation in attribute ordering
    DATA_IN = [
        ['Province', 'Date', 'Girls', 'Boys', 'Women', 'Men'],
        ['#adm1', '#date', '#affected+label+num', '#affected+num+label', '#affected+num+label', '#affected+num+label'],
        ['Coast', '2016', '200', '150', '500', '600'],
        ['Plains', '2016', '300', '450', '800', '750'],
    ]

    DATA_OUT = [
        ['Province', 'Date', 'Group', 'Number affected'],
        ['#adm1', '#date', '#affected+num+header', '#affected+num+value'],
        ['Coast', '2016', 'Girls', '200'],
        ['Coast', '2016', 'Boys', '150'],
        ['Coast', '2016', 'Women', '500'],
        ['Coast', '2016', 'Men', '600'],
        ['Plains', '2016', 'Girls', '300'],
        ['Plains', '2016', 'Boys', '450'],
        ['Plains', '2016', 'Women', '800'],
        ['Plains', '2016', 'Men', '750']
    ]

    def test_headers(self):
        source = hxl.data(self.DATA_IN).explode()
        self.assertEqual(
            ['#adm1', '#date', '#affected+num+header', '#affected+num+value'],
            source.display_tags
        )

    def test_custom_atts(self):
        source = hxl.data(self.DATA_IN).explode('foo', 'bar')
        self.assertEqual(
            ['#adm1', '#date', '#affected+num+foo', '#affected+num+bar'],
            source.display_tags
        )

    def test_content(self):
        self.assertEqual(
            hxl.data(self.DATA_IN).explode().values,
            self.DATA_OUT[2:]
        )


class TestChaining(AbstractBaseFilterTest):

    def test_rowfilter_countfilter(self):
        self.assertEqual(
            [['NGO A', 1]],
            self.source.with_rows('#sector=wash').count('#org').values
        )
        self.assertEqual(
            [['NGO A', 1], ['NGO B', 2]],
            self.source.without_rows('#sector=wash').count('#org').values
        )


