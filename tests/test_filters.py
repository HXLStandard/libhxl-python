"""
Unit tests for the hxl.filters module
David Megginson
April 2015

License: Public Domain
"""

import unittest
import hxl
from hxl.filters.cache import CacheFilter


########################################################################
# Test classes
########################################################################

class TestCache(unittest.TestCase):

    DATA = [
        ['#org', '#sector', '#country'],
        ['Org A', 'WASH', 'Panama'],
        ['Org B', 'Health', 'Panama'],
        ['Org C', 'WASH', 'Colombia']
    ]

    def setUp(self):
        self.source = hxl.hxl(TestCache.DATA)
        self.filter = CacheFilter(self.source)
        
    def test_columns(self):
        self.assertEqual(self.DATA[0], [column.tag for column in self.filter.columns])

    def test_rows(self):
        self.assertEqual(self.DATA[1:], [row.values for row in self.filter])


# end
