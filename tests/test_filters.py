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
    ['Organisation', 'Cluster', 'District'],
    ['#org', '#sector', '#adm1'],
    ['NGO A', 'WASH', 'Coast'],
    ['NGO B', 'Education', 'Plains'],
    ['NGO B', 'Education', 'Coast']
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
        self.assertEqual(DATA[2:], [row.values for row in self.source.cache()])


class TestColumnFilter(AbstractFilterTest):

    def test_with_columns(self):
        expected = ['#sector']
        self.assertEqual(expected, self.source.with_columns('#sector').tags)
        self.assertEqual(expected, self.source.with_columns(['#sector']).tags)

    def test_without_columns(self):
        expected = ['#org', '#adm1']
        self.assertEqual(expected, self.source.without_columns('#sector').tags)
        self.assertEqual(expected, self.source.without_columns(['#sector']).tags)


class TestRowFilter(AbstractFilterTest):

    def test_with_rows(self):
        self.assertEqual(DATA[3:], [row.values for row in self.source.with_rows(['#sector=education'])])
        self.assertEqual(DATA[3:], [row.values for row in self.source.with_rows('#sector=education')])

    def test_without_rows(self):
        self.assertEqual(DATA[3:], [row.values for row in self.source.without_rows(['#sector=wash'])])
        self.assertEqual(DATA[3:], [row.values for row in self.source.without_rows('#sector=wash')])


class TestCountFilter(AbstractFilterTest):

    def test_tags(self):
        expected = ['#sector', '#x_count_num']
        self.assertEqual(expected, self.source.count('#sector').tags)

    # TODO test aggregation


class TestSortFilter(AbstractFilterTest):

    def test_forward(self):
        self.assertEqual(sorted(DATA[2:]), [row.values for row in self.source.sort()])

    def test_backward(self):
        self.assertEqual(sorted(DATA[2:], reverse=True), [row.values for row in self.source.sort(reverse=True)])

    def test_custom_keys(self):
        def key(r):
            return [r[2], r[1]]
        self.assertEqual(sorted(DATA[2:], key=key), [row.values for row in self.source.sort(['#adm1', '#sector'])])


class TestAddColumnsFilter(AbstractFilterTest):

    spec = 'Country#country=Country A'

    def test_before(self):
        self.assertEqual(
            ['#country', '#org', '#sector', '#adm1'],
            self.source.add_columns(self.spec, True).tags
        )

    def test_after(self):
        self.assertEqual(
            ['#org', '#sector', '#adm1', '#country'],
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
            [row.values for row in self.source.add_columns(self.spec)]
        )


class TestRenameFilter(AbstractFilterTest):

    spec = '#sector:Sub-sector#subsector'

    def test_tags(self):
        self.assertEqual(
            ['#org', '#subsector', '#adm1'],
            self.source.rename_columns(self.spec).tags
        )

    def test_headers(self):
        self.assertEqual(
            ['Organisation', 'Sub-sector', 'District'],
            self.source.rename_columns(self.spec).headers
        )


class TestChaining(AbstractFilterTest):

    def test_rowfilter_countfilter(self):
        self.assertEqual(
            [['NGO A', 1]],
            [row.values for row in self.source.with_rows('#sector=wash').count('#org')]
        )
        self.assertEqual(
            [['NGO B', 2]],
            [row.values for row in self.source.without_rows('#sector=wash').count('#org')]
        )


class TestNonFilters(AbstractFilterTest):
    # TODO move elsewhere

    def test_validate(self):
        self.assertTrue(self.source.validate(SCHEMA_GOOD))
        self.assertFalse(self.source.validate(SCHEMA_BAD))
