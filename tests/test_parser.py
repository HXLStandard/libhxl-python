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
    EXPECTED_TAGS = ['#sector', '#subsector', '#org', '#country', '#sex', '#targeted_num', '#adm1'];
    EXPECTED_CONTENT = [
        ['WASH', 'Subsector 1', 'Org 1', 'Country 1', 'Males', '100', 'Region 1'],
        ['WASH', 'Subsector 1', 'Org 1', 'Country 1', 'Females', '100', 'Region 1'],
        ['Health', 'Subsector 2', 'Org 2', 'Country 2', 'Males', '', 'Region 2'],
        ['Health', 'Subsector 2', 'Org 2', 'Country 2', 'Females', '', 'Region 2'],
        ['Education', 'Subsector 3', 'Org 3', 'Country 2', 'Males', '250', 'Region 3'],
        ['Education', 'Subsector 3', 'Org 3', 'Country 2', 'Females', '300', 'Region 3'],
        ['WASH', 'Subsector 4', 'Org 1', 'Country 3', 'Males', '80', 'Region 4'],
        ['WASH', 'Subsector 4', 'Org 1', 'Country 3', 'Females', '95', 'Region 4']
    ]

    def setUp(self):
        self.filename = os.path.join(os.path.dirname(__file__), TestParser.SAMPLE_FILE)
        self.reader = HXLReader(open(self.filename, 'r'))

    def test_row_count(self):
        # logical row count
        row_count = 0
        for row in self.reader:
            row_count += 1
        self.assertEquals(TestParser.EXPECTED_ROW_COUNT, row_count)

    def test_column_count(self):
        for row in self.reader:
            self.assertEquals(len(TestParser.EXPECTED_TAGS), len(row.values))

    def test_columns(self):
        for row in self.reader:
            for i, value in enumerate(row):
                self.assertEquals(TestParser.EXPECTED_TAGS[i], value.column.hxlTag)

    def test_content(self):
        for i, row in enumerate(self.reader):
            for j, value in enumerate(row):
                self.assertEquals(TestParser.EXPECTED_CONTENT[i][j], value.content)
