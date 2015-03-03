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
from hxl.io import StreamInput, HXLParseException, HXLReader

class TestParser(unittest.TestCase):

    FILE_VALID = './files/test_parser/input-valid.csv'
    FILE_FUZZY = './files/test_parser/input-fuzzy.csv'
    FILE_INVALID = './files/test_parser/input-invalid.csv'

    EXPECTED_ROW_COUNT = 4
    EXPECTED_HEADERS = ['Registro', 'Sector/Cluster','Subsector','Organización','Hombres','Mujeres','País','Departamento/Provincia/Estado', None]
    EXPECTED_TAGS = [None, '#sector', '#subsector', '#org', '#targeted_num', '#targeted_num', '#country', '#adm1', '#report_date']
    EXPECTED_MODIFIERS = [[], ['es'], ['es'], ['es'], ['f'], ['m'], [], [], []]
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
        with _read_file() as input:
            # logical row count
            for row in HXLReader(StreamInput(input)):
                row_count += 1
        self.assertEqual(TestParser.EXPECTED_ROW_COUNT, row_count)

    def test_headers(self):
        with _read_file() as input:
            headers = HXLReader(StreamInput(input)).headers
        self.assertEqual(TestParser.EXPECTED_HEADERS, headers)

    def test_tags(self):
        with _read_file() as input:
            tags = HXLReader(StreamInput(input)).tags
        self.assertEqual(TestParser.EXPECTED_TAGS, tags)

    def test_modifiers(self):
        with _read_file() as input:
            for row in HXLReader(StreamInput(input)):
                for column_number, column in enumerate(row.columns):
                    self.assertEqual(TestParser.EXPECTED_MODIFIERS[column_number], column.modifiers)

    def test_column_count(self):
        with _read_file() as input:
            for row in HXLReader(StreamInput(input)):
                self.assertEqual(len(TestParser.EXPECTED_TAGS), len(row.values))

    def test_columns(self):
        with _read_file() as input:
            for row in HXLReader(StreamInput(input)):
                for column_number, column in enumerate(row.columns):
                    self.assertEqual(TestParser.EXPECTED_TAGS[column_number], column.tag)

    def test_content(self):
        with _read_file() as input:
            for i, row in enumerate(HXLReader(StreamInput(input))):
                for j, value in enumerate(row):
                    self.assertEqual(TestParser.EXPECTED_CONTENT[i][j], value)

    def test_fuzzy(self):
        """Imperfect hashtag row should still work."""
        seen_exception = False
        with _read_file(TestParser.FILE_FUZZY) as input:
            try:
                HXLReader(StreamInput(input)).tags
            except HXLParseException:
                seen_exception = True
        self.assertFalse(seen_exception)

    def test_invalid(self):
        """Missing hashtag row should raise an exception."""
        seen_exception = False
        with _read_file(TestParser.FILE_INVALID) as input:
            try:
                HXLReader(StreamInput(input)).tags
            except HXLParseException:
                seen_exception = True
        self.assertTrue(seen_exception)

def _read_file(filename=None):
    """Open a file containing a HXL dataset."""
    if not filename:
        filename = TestParser.FILE_VALID
    absolute_filename = os.path.join(os.path.dirname(__file__), filename)
    return open(absolute_filename, 'r')

