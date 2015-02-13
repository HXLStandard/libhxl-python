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

    SAMPLE_FILE_GOOD = './files/test_parser/input-simple.csv'
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
        pass

    def tearDown(self):
        pass

    def test_row_count(self):
        source = _read_file()
        # logical row count
        row_count = 0
        for row in source:
            row_count += 1
        self.assertEquals(TestParser.EXPECTED_ROW_COUNT, row_count)

    def test_headers(self):
        source = _read_file()
        headers = source.headers
        self.assertEquals(TestParser.EXPECTED_HEADERS, headers)

    def test_tags(self):
        source = _read_file()
        tags = source.tags
        self.assertEquals(TestParser.EXPECTED_TAGS, tags)

    def test_languages(self):
        source = _read_file()
        for row in source:
            for columnNumber, column in enumerate(row.columns):
                self.assertEquals(TestParser.EXPECTED_LANGUAGES[columnNumber], column.languageCode)

    def test_column_count(self):
        source = _read_file()
        for row in source:
            self.assertEquals(len(TestParser.EXPECTED_TAGS), len(row.values))

    def test_columns(self):
        source = _read_file()
        for row in source:
            for columnNumber, column in enumerate(row.columns):
                self.assertEquals(TestParser.EXPECTED_TAGS[columnNumber], column.hxlTag)

    def test_content(self):
        source = _read_file()
        for i, row in enumerate(source):
            for j, value in enumerate(row):
                self.assertEquals(TestParser.EXPECTED_CONTENT[i][j], value)

def _read_file(filename=None):
    if not filename:
        filename = TestParser.SAMPLE_FILE_GOOD
    absolute_filename = os.path.join(os.path.dirname(__file__), filename)
    input = open(absolute_filename, 'r')
    return HXLReader(input)

