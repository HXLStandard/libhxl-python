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

import hxl
from hxl.io import make_input, HXLParseException, HXLReader, CSVInput


def _resolve_file(filename):
    return os.path.join(os.path.dirname(__file__), filename)

FILE_CSV = _resolve_file('./files/test_io/input-valid.csv')
FILE_EXCEL = _resolve_file('./files/test_io/input-valid.xlsx')
FILE_MULTILINE = _resolve_file('./files/test_io/input-multiline.csv')
FILE_FUZZY = _resolve_file('./files/test_io/input-fuzzy.csv')
FILE_INVALID = _resolve_file('./files/test_io/input-invalid.csv')
URL_CSV = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/master/tests/files/test_io/input-valid.csv'
URL_EXCEL = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/master/tests/files/test_io/input-valid.xlsx'
URL_GOOGLE_NOHASH = 'https://docs.google.com/spreadsheets/d/1VTswL-w9EI0IdGIBFZoZ-2RmIiebXKsrhv03yd7LlIg/edit'
URL_GOOGLE_HASH = 'https://docs.google.com/spreadsheets/d/1VTswL-w9EI0IdGIBFZoZ-2RmIiebXKsrhv03yd7LlIg/edit#gid=299366282'


class TestBadInput(unittest.TestCase):

    def test_bad_file(self):
        with self.assertRaises(IOError):
            source = hxl.data('XXXXX', True)

    def test_bad_url(self):
        with self.assertRaises(IOError):
            source = hxl.data('http://example.org/XXXXX')

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
        with hxl.data(FILE_CSV, True) as source:
            # logical row count
            for row in source:
                row_count += 1
        self.assertEqual(TestParser.EXPECTED_ROW_COUNT, row_count)

    def test_headers(self):
        with hxl.data(FILE_CSV, True) as source:
            headers = source.headers
        self.assertEqual(TestParser.EXPECTED_HEADERS, headers)

    def test_tags(self):
        with hxl.data(FILE_CSV, True) as source:
            tags = source.tags
        self.assertEqual(TestParser.EXPECTED_TAGS, tags)

    def test_attributes(self):
        with hxl.data(FILE_CSV, True) as source:
            for row in source:
                for column_number, column in enumerate(row.columns):
                    self.assertEqual(set(TestParser.EXPECTED_ATTRIBUTES[column_number]), column.attributes)

    def test_column_count(self):
        with hxl.data(FILE_CSV, True) as source:
            for row in source:
                self.assertEqual(len(TestParser.EXPECTED_TAGS), len(row.values))

    def test_columns(self):
        with hxl.data(FILE_CSV, True) as source:
            for row in source:
                for column_number, column in enumerate(row.columns):
                    self.assertEqual(TestParser.EXPECTED_TAGS[column_number], column.tag)

    def test_multiline(self):
        with hxl.data(FILE_MULTILINE, True) as source:
            for row in source:
                self.assertEqual("Line 1\nLine 2\nLine 3", row.get('description'))

    def test_local_csv(self):
        """Test reading from a local CSV file."""
        with hxl.data(FILE_CSV, True) as source:
            self.compare_input(source)

    def test_local_excel(self):
        """Test reading from a local Excel file."""
        with hxl.data(FILE_EXCEL, True) as source:
            self.compare_input(source)

    def test_remote_csv(self):
        """Test reading from a remote CSV file (will fail without connectivity)."""
        with hxl.data(URL_CSV) as source:
            self.compare_input(source)

    def test_remote_excel(self):
        """Test reading from a remote Excel file (will fail without connectivity)."""
        with hxl.data(URL_EXCEL) as source:
            self.compare_input(source)

    def test_remote_google(self):
        """Test reading from a Google Sheet (will fail without connectivity)."""

        # default tab
        with hxl.data(URL_GOOGLE_NOHASH) as source:
            self.compare_input(source)

        # specific tab
        with hxl.data(URL_GOOGLE_HASH) as source:
            self.compare_input(source)

    def test_fuzzy(self):
        """Imperfect hashtag row should still work."""
        with hxl.data(FILE_FUZZY, True) as source:
            source.tags

    def test_invalid(self):
        """Missing hashtag row should raise an exception."""
        with self.assertRaises(HXLParseException):
            with hxl.data(FILE_INVALID, True) as source:
                source.tags

    def compare_input(self, source):
        """Compare an external source to the expected content."""
        for i, row in enumerate(source):
            for j, value in enumerate(row):
                # For Excel, numbers may be pre-parsed
                try:
                    self.assertEqual(float(TestParser.EXPECTED_CONTENT[i][j]), float(value))
                except:
                    self.assertEqual(TestParser.EXPECTED_CONTENT[i][j], value)
