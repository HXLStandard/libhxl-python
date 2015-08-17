#coding=UTF8
"""
Unit tests for the hxl.io module
David Megginson
October 2014

License: Public Domain
"""

import unittest
import os
import sys
if sys.version_info < (3,):
    from urllib2 import HTTPError
else:
    from urllib.error import HTTPError
from hxl import hxl
from hxl.io import make_input, HXLParseException, HXLReader, CSVInput


def _resolve_file(filename):
    return os.path.join(os.path.dirname(__file__), filename)

FILE_VALID = _resolve_file('./files/test_parser/input-valid.csv')
FILE_FUZZY = _resolve_file('./files/test_parser/input-fuzzy.csv')
FILE_INVALID = _resolve_file('./files/test_parser/input-invalid.csv')

class TestBadInput(unittest.TestCase):

    def test_bad_file(self):
        with self.assertRaises(IOError):
            source = hxl('XXXXX', True)

    def test_bad_url(self):
        with self.assertRaises(IOError):
            source = hxl('http://example.org/XXXXX')

class TestParser(unittest.TestCase):

    EXPECTED_ROW_COUNT = 4
    EXPECTED_HEADERS = ['Registro', 'Sector/Cluster','Subsector','Organización','Hombres','Mujeres','País','Departamento/Provincia/Estado', None]
    EXPECTED_TAGS = [None, '#sector', '#subsector', '#org', '#targeted', '#targeted', '#country', '#adm1', '#date']
    EXPECTED_ATTRIBUTES = [{}, {'es'}, {'es'}, {'es'}, {'f'}, {'m'}, {}, {}, {'reported'}]
    EXPECTED_CONTENT = [
        ['001', 'WASH', 'Higiene', 'ACNUR', '100', '100', 'Panamá', 'Los Santos', '1 March 2015'],
        ['002', 'Salud', 'Vacunación', 'OMS', '', '', 'Colombia', 'Cauca', ''],
        ['003', 'Educación', 'Formación de enseñadores', 'UNICEF', '250', '300', 'Colombia', 'Chocó', ''],
        ['004', 'WASH', 'Urbano', 'OMS', '80', '95', 'Venezuela', 'Amazonas', '']
    ]

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_row_count(self):
        row_count = 0
        with hxl(FILE_VALID, True) as source:
            # logical row count
            for row in source:
                row_count += 1
        self.assertEqual(TestParser.EXPECTED_ROW_COUNT, row_count)

    def test_headers(self):
        with hxl(FILE_VALID, True) as source:
            headers = source.headers
        self.assertEqual(TestParser.EXPECTED_HEADERS, headers)

    def test_tags(self):
        with hxl(FILE_VALID, True) as source:
            tags = source.tags
        self.assertEqual(TestParser.EXPECTED_TAGS, tags)

    def test_attributes(self):
        with hxl(FILE_VALID, True) as source:
            for row in source:
                for column_number, column in enumerate(row.columns):
                    self.assertEqual(set(TestParser.EXPECTED_ATTRIBUTES[column_number]), column.attributes)

    def test_column_count(self):
        with hxl(FILE_VALID, True) as source:
            for row in source:
                self.assertEqual(len(TestParser.EXPECTED_TAGS), len(row.values))

    def test_columns(self):
        with hxl(FILE_VALID, True) as source:
            for row in source:
                for column_number, column in enumerate(row.columns):
                    self.assertEqual(TestParser.EXPECTED_TAGS[column_number], column.tag)

    def test_content(self):
        with hxl(FILE_VALID, True) as source:
            for i, row in enumerate(source):
                for j, value in enumerate(row):
                    self.assertEqual(TestParser.EXPECTED_CONTENT[i][j], value)

    def test_fuzzy(self):
        """Imperfect hashtag row should still work."""
        with hxl(FILE_FUZZY, True) as source:
            source.tags

    def test_invalid(self):
        """Missing hashtag row should raise an exception."""
        with self.assertRaises(HXLParseException):
            with hxl(FILE_INVALID, True) as source:
                source.tags



