#coding=UTF8
"""
Unit tests for the hxl.parser module
David Megginson
October 2014

License: Public Domain
"""

import unittest
import os
import codecs
from hxl.parser import HXLReader

class TestParser(unittest.TestCase):

    SAMPLE_FILE = 'sample-data/sample.csv'
    EXPECTED_ROW_COUNT = 8
    EXPECTED_HEADERS = ['Sector/Cluster','Subsector',"Organización",'País','Sex','Targeted','Departamento/Provincia/Estado']
    EXPECTED_TAGS = ['#sector', '#subsector', '#org', '#country', '#sex', '#targeted_num', '#adm1']
    EXPECTED_CONTENT = [
        ['WASH', 'Subsector 1', 'Org 1', 'Panamá', 'Hombres', '100', 'Los Santos'],
        ['WASH', 'Subsector 1', 'Org 1', 'Panamá', 'Mujeres', '100', 'Los Santos'],
        ['Salud', 'Subsector 2', 'Org 2', 'Colombia', 'Hombres', '', 'Cauca'],
        ['Salud', 'Subsector 2', 'Org 2', 'Colombia', 'Mujeres', '', 'Cauca'],
        ['Educación', 'Subsector 3', 'Org 3', 'Colombia', 'Hombres', '250', 'Chocó'],
        ['Educación', 'Subsector 3', 'Org 3', 'Colombia', 'Mujeres', '300', 'Chocó'],
        ['WASH', 'Subsector 4', 'Org 1', 'Venezuela', 'Hombres', '80', 'Amazonas'],
        ['WASH', 'Subsector 4', 'Org 1', 'Venezuela', 'Mujeres', '95', 'Amazonas']
    ]

    def setUp(self):
        self.filename = os.path.join(os.path.dirname(__file__), TestParser.SAMPLE_FILE)
        self.reader = HXLReader(open(self.filename, 'rb'))

    def test_row_count(self):
        # logical row count
        row_count = 0
        for row in self.reader:
            row_count += 1
        self.assertEquals(TestParser.EXPECTED_ROW_COUNT, row_count)

    def test_headers(self):
        headers = self.reader.headers
        self.assertEquals(TestParser.EXPECTED_HEADERS, headers)

    def test_tags(self):
        tags = self.reader.tags
        self.assertEquals(TestParser.EXPECTED_TAGS, tags)

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
