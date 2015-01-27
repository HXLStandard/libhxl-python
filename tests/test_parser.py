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

    SAMPLE_FILE = './files/test_parser/input-simple.csv'
    EXPECTED_ROW_COUNT = 8
    EXPECTED_HEADERS = ['Sector/Cluster','Subsector','Organización','Sex','Targeted','País','Departamento/Provincia/Estado']
    EXPECTED_TAGS = ['#sector', '#subsector', '#org', '#sex', '#targeted_num', '#country', '#adm1']
    EXPECTED_LANGUAGES = ['es', 'es', 'es', None, None, None, None]
    EXPECTED_CONTENT = [
        ['WASH', 'Higiene', 'ACNUR', 'Hombres', '100', 'Panamá', 'Los Santos'],
        ['WASH', 'Higiene', 'ACNUR', 'Mujeres', '100', 'Panamá', 'Los Santos'],
        ['Salud', 'Vacunación', 'OMS', 'Hombres', '', 'Colombia', 'Cauca'],
        ['Salud', 'Vacunación', 'OMS', 'Mujeres', '', 'Colombia', 'Cauca'],
        ['Educación', 'Formación de enseñadores', 'UNICEF', 'Hombres', '250', 'Colombia', 'Chocó'],
        ['Educación', 'Formación de enseñadores', 'UNICEF', 'Mujeres', '300', 'Colombia', 'Chocó'],
        ['WASH', 'Urbano', 'OMS', 'Hombres', '80', 'Venezuela', 'Amazonas'],
        ['WASH', 'Urbano', 'OMS', 'Mujeres', '95', 'Venezuela', 'Amazonas']
    ]

    def setUp(self):
        self.filename = os.path.join(os.path.dirname(__file__), TestParser.SAMPLE_FILE)
        self.input_file = open(self.filename, 'r')
        self.reader = HXLReader(self.input_file)

    def tearDown(self):
        self.input_file.close()

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

    def test_languages(self):
        for row in self.reader:
            for columnNumber, column in enumerate(row.columns):
                self.assertEquals(TestParser.EXPECTED_LANGUAGES[columnNumber], column.languageCode)

    def test_column_count(self):
        for row in self.reader:
            self.assertEquals(len(TestParser.EXPECTED_TAGS), len(row.values))

    def test_columns(self):
        for row in self.reader:
            for columnNumber, column in enumerate(row.columns):
                self.assertEquals(TestParser.EXPECTED_TAGS[columnNumber], column.hxlTag)

    def test_content(self):
        for i, row in enumerate(self.reader):
            for j, value in enumerate(row):
                self.assertEquals(TestParser.EXPECTED_CONTENT[i][j], value)
