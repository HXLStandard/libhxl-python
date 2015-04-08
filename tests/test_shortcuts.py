"""
Unit tests for processing shortcuts
David Megginson
April 2015

License: Public Domain
"""

import unittest

from hxl import hxl

DATA = [
    ['#org', '#sector', '#adm1'],
    ['NGO A', 'WASH', 'Coast'],
    ['NGO B', 'Education', 'Plains'],
    ['NGO B', 'Education', 'Coast']
]
    
class TestShortcuts(unittest.TestCase):

    def setUp(self):
        self.source = hxl(DATA)

    def test_columns(self):
        self.assertEqual(DATA[0], [column.tag for column in self.source.columns])

    def test_rows(self):
        self.assertEqual(DATA[1:], [row.values for row in self.source]) 

    def test_with_columns(self):
        expected = ['#sector']
        self.assertEqual(expected, [column.tag for column in self.source.with_columns('#sector').columns])
        self.assertEqual(expected, [column.tag for column in self.source.with_columns(['#sector']).columns])

    def test_without_columns(self):
        expected = ['#org', '#adm1']
        self.assertEqual(expected, [column.tag for column in self.source.without_columns('#sector').columns])
        self.assertEqual(expected, [column.tag for column in self.source.without_columns(['#sector']).columns])
