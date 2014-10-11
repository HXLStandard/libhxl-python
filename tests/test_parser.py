"""
Unit tests for the hxl.model module
David Megginson
October 2014

License: Public Domain
"""

import unittest
import os
from hxl.parser import HXLReader

class TestParser(unittest.TestCase):

    SAMPLE_FILE = 'sample-data/sample.csv'
    EXPECTED_ROW_COUNT = 8

    def setUp(self):
        self.filename = os.path.join(os.path.dirname(__file__), TestParser.SAMPLE_FILE)
        self.reader = HXLReader(open(self.filename, 'r'))

    def test_row_count(self):
        # logical row count
        row_count = 0
        for row in self.reader:
            row_count += 1
        self.assertEquals(TestParser.EXPECTED_ROW_COUNT, row_count)

