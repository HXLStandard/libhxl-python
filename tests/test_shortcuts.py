"""
Unit tests for processing shortcuts
David Megginson
April 2015

License: Public Domain
"""

import unittest

from hxl import hxl

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


class TestCacheFilter(unittest.TestCase):

    def setUp(self):
        self.source = hxl(DATA)
        self.filter = self.source.cache()
        
    def test_headers(self):
        self.assertEqual(DATA[0], self.filter.headers)

    def test_columns(self):
        self.assertEqual(DATA[1], self.filter.tags)

    def test_rows(self):
        self.assertEqual(DATA[2:], [row.values for row in self.filter])


class TestShortcuts(unittest.TestCase):

    def setUp(self):
        # use a cache filter so that we can run tests multiple times
        self.source = hxl(DATA).cache()

    def test_columns(self):
        self.assertEqual(DATA[1], [column.tag for column in self.source.columns])

    def test_rows(self):
        self.assertEqual(DATA[2:], [row.values for row in self.source]) 

    def test_with_columns(self):
        expected = ['#sector']
        self.assertEqual(expected, [column.tag for column in self.source.with_columns('#sector').columns])
        self.assertEqual(expected, [column.tag for column in self.source.with_columns(['#sector']).columns])

    def test_without_columns(self):
        expected = ['#org', '#adm1']
        self.assertEqual(expected, [column.tag for column in self.source.without_columns('#sector').columns])
        self.assertEqual(expected, [column.tag for column in self.source.without_columns(['#sector']).columns])

    def test_with_rows(self):
        self.assertEqual(DATA[3:], [row.values for row in self.source.with_rows(['#sector=education'])])
        self.assertEqual(DATA[3:], [row.values for row in self.source.with_rows('#sector=education')])

    def test_without_rows(self):
        self.assertEqual(DATA[3:], [row.values for row in self.source.without_rows(['#sector=wash'])])
        self.assertEqual(DATA[3:], [row.values for row in self.source.without_rows('#sector=wash')])

    def test_count(self):
        tags = [column.tag for column in self.source.count('#sector').columns]
        self.assertTrue('#sector' in tags)
        self.assertTrue('#x_count_num' in tags)
        self.assertTrue('#adm1' not in tags)
        # TODO test aggregation

    def test_sort(self):
        self.assertEqual(sorted(DATA[2:]), [row.values for row in self.source.sort()])
        self.assertEqual(sorted(DATA[2:], reverse=True), [row.values for row in self.source.sort(reverse=True)])
        # try with custom sort keys
        def key(r):
            return [r[2], r[1]]
        self.assertEqual(sorted(DATA[2:], key=key), [row.values for row in self.source.sort(['#adm1', '#sector'])])

    def test_add_columns(self):
        spec = 'Country#country=Country A'

        # tags at start
        self.assertEqual(
            ['#country', '#org', '#sector', '#adm1'],
            self.source.add_columns(spec, True).tags
        )

        # tags
        self.assertEqual(
            ['#org', '#sector', '#adm1', '#country'],
            self.source.add_columns(spec).tags
        )

        # headers
        self.assertEqual(
            DATA[0] + ['Country'],
            self.source.add_columns(spec).headers
        )

        # rows
        self.assertEqual(
            [values + ['Country A'] for values in DATA[2:]],
            [row.values for row in self.source.add_columns('#country=Country A')]
        )

    def test_rename_columns(self):
        spec = '#sector:Sub-sector#subsector'
        self.assertEqual(
            ['#org', '#subsector', '#adm1'],
            self.source.rename_columns(spec).tags
        )
        self.assertEqual(
            ['Organisation', 'Sub-sector', 'District'],
            self.source.rename_columns(spec).headers
        )

    def test_validate(self):
        self.assertTrue(self.source.validate(SCHEMA_GOOD))
        self.assertFalse(self.source.validate(SCHEMA_BAD))
