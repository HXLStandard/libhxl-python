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
import io
if sys.version_info < (3,):
    from urllib2 import HTTPError
    import StringIO
else:
    from urllib.error import HTTPError
    from io import StringIO

import hxl
from hxl.io import make_input, HXLParseException, HXLReader, CSVInput


def _resolve_file(filename):
    return os.path.join(os.path.dirname(__file__), filename)

FILE_CSV = _resolve_file('./files/test_io/input-valid.csv')
FILE_CSV_OUT = _resolve_file('./files/test_io/output-valid.csv')
FILE_EXCEL = _resolve_file('./files/test_io/input-valid.xlsx')
FILE_JSON = _resolve_file('./files/test_io/input-valid.json')
FILE_JSON_TXT = _resolve_file('./files/test_io/input-valid-json.txt')
FILE_JSON_UNTAGGED = _resolve_file('./files/test_io/input-untagged.json')
FILE_JSON_OUT = _resolve_file('./files/test_io/output-valid.json')
FILE_JSON_OBJECTS = _resolve_file('./files/test_io/input-valid-objects.json')
FILE_JSON_OBJECTS_UNTAGGED = _resolve_file('./files/test_io/input-untagged-objects.json')
FILE_JSON_OBJECTS_OUT = _resolve_file('./files/test_io/output-valid-objects.json')
FILE_JSON_NESTED = _resolve_file('./files/test_io/input-valid-nested.json')
FILE_MULTILINE = _resolve_file('./files/test_io/input-multiline.csv')
FILE_FUZZY = _resolve_file('./files/test_io/input-fuzzy.csv')
FILE_INVALID = _resolve_file('./files/test_io/input-invalid.csv')
URL_CSV = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/master/tests/files/test_io/input-valid.csv'
URL_EXCEL = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/master/tests/files/test_io/input-valid.xlsx'
URL_JSON = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/master/tests/files/test_io/input-valid.json'
URL_GOOGLE_NOHASH = 'https://docs.google.com/spreadsheets/d/1VTswL-w9EI0IdGIBFZoZ-2RmIiebXKsrhv03yd7LlIg/edit'
URL_GOOGLE_HASH = 'https://docs.google.com/spreadsheets/d/1VTswL-w9EI0IdGIBFZoZ-2RmIiebXKsrhv03yd7LlIg/edit#gid=299366282'

class TestUntaggedInput(unittest.TestCase):

    def test_untagged_json(self):
        with hxl.io.make_input(FILE_JSON_UNTAGGED, allow_local=True) as input:
            self.assertEqual([
                ['Qué?', '', '', 'Quién?', 'Para quién?', '', 'Dónde?', 'Cuándo?'],
                ['Registro', 'Sector/Cluster', 'Subsector', 'Organización', 'Hombres', 'Mujeres', 'País', 'Departamento/Provincia/Estado'],
                ['001', 'WASH', 'Higiene', 'ACNUR', '100', '100', 'Panamá', 'Los Santos', '1 March 2015'],
                ['002', 'Salud', 'Vacunación', 'OMS', '', '', 'Colombia', 'Cauca', ''],
                ['003', 'Educación', 'Formación de enseñadores', 'UNICEF', '250', '300', 'Colombia', 'Chocó', ''],
                ['004', 'WASH', 'Urbano', 'OMS', '80', '95', 'Venezuela', 'Amazonas', '']
            ], list(input))

    def test_untagged_json_objects(self):
        with hxl.io.make_input(FILE_JSON_OBJECTS_UNTAGGED, allow_local=True) as input:
            self.assertEqual([
                ['Registro', 'Sector/Cluster', 'Subsector', 'Organización', 'Hombres', 'Mujeres', 'País', 'Departamento/Provincia/Estado'],
                ['001', 'WASH', 'Higiene', 'ACNUR', '100', '100', 'Panamá', 'Los Santos'],
                ['002', 'Salud', 'Vacunación', 'OMS', '', '', 'Colombia', 'Cauca'],
                ['003', 'Educación', 'Formación de enseñadores', 'UNICEF', '250', '300', 'Colombia', 'Chocó'],
                ['004', 'WASH', 'Urbano', 'OMS', '80', '95', 'Venezuela', 'Amazonas']
            ], list(input))

class TestFunctions(unittest.TestCase):

    DATA = [
        ['Sector', 'Organisation', 'Province name'],
        ['#sector', '#org', '#adm1'],
        ['WASH', 'Org A', 'Coast'],
        ['Health', 'Org B', 'Plains']
    ]

    def test_from_spec_tagged(self):
        source = hxl.from_spec({
            'input': self.DATA,
            'recipe': [
                {
                    'filter': 'cache'
                }
            ]
        })
        self.assertEqual(self.DATA[2:], source.values)

    def test_from_spec_untagged(self):
        source = hxl.from_spec({
            'input': self.DATA[0:1]+self.DATA[2:],
            'tagger': {
                'specs': {
                    'sector': '#sector',
                    'organisation': '#org',
                    'province name': '#adm1'
                }
            },
            'recipe': [
                {
                    'filter': 'cache'
                }
            ]
        })
        self.assertEqual(self.DATA[2:], source.values)


class TestBadInput(unittest.TestCase):

    def test_bad_file(self):
        with self.assertRaises(IOError):
            source = hxl.data('XXXXX', True)

    def test_bad_url(self):
        with self.assertRaises(IOError):
            source = hxl.data('http://x.localhost/XXXXX', timeout=1)

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

    def test_local_json(self):
        """Test reading from a local JSON file."""
        with hxl.data(FILE_JSON, True) as source:
            self.compare_input(source)

    def test_local_json_text(self):
        """Test reading from a local JSON file that doesn't have a JSON extension."""
        with hxl.data(FILE_JSON_TXT, True) as source:
            self.compare_input(source)

    def test_local_json_objects(self):
        """Test reading from a local JSON file."""
        with hxl.data(FILE_JSON_OBJECTS, True) as source:
            self.compare_input(source)

    def test_local_json_nested(self):
        """Test reading from a local JSON file."""
        with hxl.data(FILE_JSON_NESTED, True) as source:
            self.compare_input(source)

    def test_remote_csv(self):
        """Test reading from a remote CSV file (will fail without connectivity)."""
        with hxl.data(URL_CSV, timeout=5) as source:
            self.compare_input(source)

    def test_remote_excel(self):
        """Test reading from a remote Excel file (will fail without connectivity)."""
        with hxl.data(URL_EXCEL, timeout=5) as source:
            self.compare_input(source)

    def x_test_remote_json(self):
        """Test reading from a remote JSON file (will fail without connectivity)."""
        with hxl.data(URL_JSON) as source:
            self.compare_input(source)

    def test_remote_google(self):
        """Test reading from a Google Sheet (will fail without connectivity)."""

        # default tab
        with hxl.data(URL_GOOGLE_NOHASH, timeout=5) as source:
            self.compare_input(source)

        # specific tab
        with hxl.data(URL_GOOGLE_HASH, timeout=5) as source:
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
                if value is None:
                    value = ''
                # For Excel, numbers may be pre-parsed
                try:
                    self.assertEqual(float(TestParser.EXPECTED_CONTENT[i][j]), float(value))
                except:
                    self.assertEqual(TestParser.EXPECTED_CONTENT[i][j], value)

class TestFunctions(unittest.TestCase):
    """Test module-level convenience functions."""

    DATA_TAGGED = [
        ["District", "Sector", "Organisation"],
        ["#adm1", "#sector", "#org"],
        ["Coast", "Health", "NGO A"],
        ["Plains", "Education", "NGO B"],
        ["Forest", "WASH", "NGO C"],
    ]

    DATA_UNTAGGED = [
        ["District", "Sector", "Organisation"],
        ["Coast", "Health", "NGO A"],
        ["Plains", "Education", "NGO B"],
        ["Forest", "WASH", "NGO C"],
    ]

    def test_tagger(self):
        input = hxl.io.tagger(TestFunctions.DATA_UNTAGGED, {
                "District": "#org"
        })
        self.assertEqual(TestFunctions.DATA_UNTAGGED[1:], [row.values for row in input])

    def test_write_csv(self):
        with open(FILE_CSV_OUT, 'rb') as input:
            expected = input.read()
            buffer = StringIO()
            with hxl.data(FILE_CSV, True) as source:
                hxl.io.write_hxl(buffer, source)
                # Need to work with bytes to handle CRLF
                self.assertEqual(expected, bytes(buffer.getvalue(), 'utf-8'))

    def test_write_json_lists(self):
        with open(FILE_JSON_OUT) as input:
            expected = input.read()
            buffer = StringIO()
            with hxl.data(FILE_CSV, True) as source:
                hxl.io.write_json(buffer, source)
                self.assertEqual(expected, buffer.getvalue())

    def test_write_json_objects(self):
        with open(FILE_JSON_OBJECTS_OUT) as input:
            expected = input.read()
            buffer = StringIO()
            with hxl.data(FILE_CSV, True) as source:
                hxl.io.write_json(buffer, source, use_objects=True)
                self.assertEqual(expected, buffer.getvalue())
