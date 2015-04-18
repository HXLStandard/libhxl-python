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
    ['Organisation', 'Cluster', 'District', 'Affected'],
    ['#org', '#sector', '#adm1', '#affected'],
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



class AbstractFilterTest(unittest.TestCase):
    """Base class for all tests."""

    def setUp(self):
        # use a cache filter so that we can run tests multiple times
        self.source = hxl(DATA).cache()


#
# Test classes
#

class TestCacheFilter(AbstractFilterTest):

    def test_headers(self):
        self.assertEqual(DATA[0], self.source.cache().headers)

    def test_columns(self):
        self.assertEqual(DATA[1], self.source.cache().tags)

    def test_rows(self):
        self.assertEqual(DATA[2:], self.source.cache().values)


class TestColumnFilter(AbstractFilterTest):

    def test_with_columns(self):
        expected = ['#sector']
        self.assertEqual(expected, self.source.with_columns('#sector').tags)
        self.assertEqual(expected, self.source.with_columns(['#sector']).tags)

    def test_without_columns(self):
        expected = ['#org', '#adm1', '#affected']
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
        expected = ['#sector', '#meta+count']
        self.assertEqual(expected, self.source.count('#sector').tags)

    def test_values(self):
        expected = [
            ['Education', 2],
            ['WASH', 1]
        ]
        self.assertEqual(expected, self.source.count('#sector').values)

    def test_aggregation_tags(self):
        expected = ['#sector', '#meta+count', '#meta+sum', '#meta+average', '#meta+min', '#meta+max']
        self.assertEqual(expected, self.source.count('#sector', '#affected').tags)

    def test_aggregation_values(self):
        expected = [
            ['Education', 2, 500, 250, 200, 300],
            ['WASH', 1, 100, 100, 100, 100]
        ]
        self.assertEqual(expected, self.source.count('#sector', '#affected').values)


class TestSortFilter(AbstractFilterTest):

    def test_forward(self):
        self.assertEqual(sorted(DATA[2:]), self.source.sort().values)

    def test_backward(self):
        self.assertEqual(sorted(DATA[2:], reverse=True), self.source.sort(reverse=True).values)

    def test_custom_keys(self):
        def key(r):
            return [r[2], r[1]]
        self.assertEqual(sorted(DATA[2:], key=key), self.source.sort(['#adm1', '#sector']).values)


class TestAddColumnsFilter(AbstractFilterTest):

    spec = 'Country#country=Country A'

    def test_before(self):
        self.assertEqual(
            ['#country', '#org', '#sector', '#adm1', '#affected'],
            self.source.add_columns(self.spec, True).tags
        )

    def test_after(self):
        self.assertEqual(
            ['#org', '#sector', '#adm1', '#affected', '#country'],
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
            ['#org', '#subsector', '#adm1', '#affected'],
            self.source.rename_columns(self.spec).tags
        )

    def test_headers(self):
        self.assertEqual(
            ['Organisation', 'Sub-sector', 'District', 'Affected'],
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


class TestNonFilters(AbstractFilterTest):
    # TODO move elsewhere

    def test_validate(self):
        self.assertTrue(self.source.validate(SCHEMA_GOOD))
        self.assertFalse(self.source.validate(SCHEMA_BAD))
