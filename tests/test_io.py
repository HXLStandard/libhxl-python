#coding=UTF8
"""
Unit tests for the hxl.io module
David Megginson
October 2014

License: Public Domain
"""

import unittest
import os
import codecs
from hxl.io import HXLParseException, HXLReader

class TestParser(unittest.TestCase):

    FILE_VALID = './files/test_parser/input-valid.csv'
    FILE_FUZZY = './files/test_parser/input-fuzzy.csv'
    FILE_INVALID = './files/test_parser/input-invalid.csv'

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
        row_count = 0
        with _read_file() as input:
            # logical row count
            for row in HXLReader(input):
                row_count += 1
        self.assertEqual(TestParser.EXPECTED_ROW_COUNT, row_count)

    def test_headers(self):
        with _read_file() as input:
            headers = HXLReader(input).headers
        self.assertEqual(TestParser.EXPECTED_HEADERS, headers)

    def test_tags(self):
        with _read_file() as input:
            tags = HXLReader(input).tags
        self.assertEqual(TestParser.EXPECTED_TAGS, tags)

    def test_languages(self):
        with _read_file() as input:
            for row in HXLReader(input):
                for columnNumber, column in enumerate(row.columns):
                    self.assertEqual(TestParser.EXPECTED_LANGUAGES[columnNumber], column.languageCode)

    def test_column_count(self):
        with _read_file() as input:
            for row in HXLReader(input):
                self.assertEqual(len(TestParser.EXPECTED_TAGS), len(row.values))

    def test_columns(self):
        with _read_file() as input:
            for row in HXLReader(input):
                for columnNumber, column in enumerate(row.columns):
                    self.assertEqual(TestParser.EXPECTED_TAGS[columnNumber], column.hxlTag)

    def test_content(self):
        with _read_file() as input:
            for i, row in enumerate(HXLReader(input)):
                for j, value in enumerate(row):
                    self.assertEqual(TestParser.EXPECTED_CONTENT[i][j], value)

    def test_fuzzy(self):
        """Imperfect hashtag row should still work."""
        seen_exception = False
        with _read_file(TestParser.FILE_FUZZY) as input:
            try:
                HXLReader(input).tags
            except HXLParseException:
                seen_exception = True
        self.assertFalse(seen_exception)

    def test_invalid(self):
        """Missing hashtag row should raise an exception."""
        seen_exception = False
        with _read_file(TestParser.FILE_INVALID) as input:
            try:
                HXLReader(TestParser.FILE_INVALID).tags
            except HXLParseException:
                seen_exception = True
        self.assertTrue(seen_exception)

def _read_file(filename=None):
    """Open a file containing a HXL dataset."""
    if not filename:
        filename = TestParser.FILE_VALID
    absolute_filename = os.path.join(os.path.dirname(__file__), filename)
    return open(absolute_filename, 'r')

