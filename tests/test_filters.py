"""
Unit tests for filters
David Megginson
April 2015

License: Public Domain
"""

import unittest

from hxl import hxl

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
        self.source = hxl(DATA).cache()


#
# Test classes
#

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

    def setUp(self):
        super(TestAppendFilter, self).setUp()
        self.append_source = hxl(TestAppendFilter.APPEND_DATA)
    
    def test_headers(self):
        self.assertEqual(self.COMBINED_DATA[0], self.source.append(self.append_source).headers)

    def test_columns(self):
        self.assertEqual(self.COMBINED_DATA[1], self.source.append(self.append_source).display_tags)

        
class TestCacheFilter(AbstractFilterTest):

    def test_headers(self):
        self.assertEqual(DATA[0], self.source.cache().headers)

    def test_columns(self):
        self.assertEqual(DATA[1], self.source.cache().display_tags)

    def test_rows(self):
        self.assertEqual(DATA[2:], self.source.cache().values)


class TestColumnFilter(AbstractFilterTest):

    def test_with_columns(self):
        expected = ['#sector']
        self.assertEqual(expected, self.source.with_columns('#sector').tags)
        self.assertEqual(expected, self.source.with_columns(['#sector']).tags)

    def test_without_columns(self):
        expected = ['#org', '#adm1', '#meta']
        self.assertEqual(expected, self.source.without_columns('#sector').tags)
        self.assertEqual(expected, self.source.without_columns(['#sector']).tags)


class TestRowFilter(AbstractFilterTest):

    def test_with_rows(self):
        self.assertEqual(DATA[3:], self.source.with_rows(['#sector=education']).values)
        self.assertEqual(DATA[3:], self.source.with_rows('#sector=education').values)

    def test_without_rows(self):
        self.assertEqual(DATA[3:], self.source.without_rows(['#sector=wash']).values)
        self.assertEqual(DATA[3:], self.source.without_rows('#sector=wash').values)


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

    def test_aggregation_tags(self):
        expected = ['#sector', '#meta+count', '#meta+sum', '#meta+average', '#meta+min', '#meta+max']
        self.assertEqual(expected, self.source.count('#sector', '#meta').display_tags)

    def test_aggregation_values(self):
        expected = [
            ['Education', 2, 400, 200, 100, 300],
            ['WASH', 1, 200, 200, 200, 200]
        ]
        self.assertEqual(expected, self.source.count('#sector', '#meta').values)


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


