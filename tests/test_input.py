#coding=UTF8
"""
Unit tests for the hxl.input module
David Megginson
October 2014

License: Public Domain
"""

import unittest
import os
import sys
import io
import json
from urllib.error import HTTPError
from io import StringIO

import hxl
from hxl.input import make_input, HXLParseException, HXLReader, CSVInput, InputOptions


def _resolve_file(filename):
    return os.path.join(os.path.dirname(__file__), filename)

DATA = [
   ['Sector', 'Organisation', 'Province name'],
   ['#sector', '#org', '#adm1'],
   ['WASH', 'Org A', 'Coast'],
   ['Health', 'Org B', 'Plains']
]

FILE_CSV = _resolve_file('./files/test_io/input-valid.csv')
FILE_TSV = _resolve_file('./files/test_io/input-valid.tsv')
FILE_SSV = _resolve_file('./files/test_io/input-valid.ssv')
FILE_ZIP_CSV = _resolve_file('./files/test_io/input-valid-csv.zip')
FILE_ZIP_CSV_UNTAGGED = _resolve_file('./files/test_io/input-untagged-csv.zip')
FILE_ZIP_INVALID = _resolve_file('./files/test_io/input-zip-invalid.zip')
FILE_CSV_LATIN1 = _resolve_file('./files/test_io/input-valid-latin1.csv')
FILE_CSV_OUT = _resolve_file('./files/test_io/output-valid.csv')
FILE_XLSX = _resolve_file('./files/test_io/input-valid.xlsx')
FILE_XLS = _resolve_file('./files/test_io/input-valid.xls')
FILE_XLSX_BROKEN = _resolve_file('./files/test_io/input-broken.xlsx')
FILE_XLSX_NOEXT = _resolve_file('./files/test_io/input-valid-xlsx.NOEXT')
FILE_XLSX_MERGED = _resolve_file('./files/test_io/input-merged.xlsx')
FILE_XLSX_INFO = _resolve_file('./files/test_io/input-quality.xlsx')
FILE_JSON = _resolve_file('./files/test_io/input-valid.json')
FILE_JSON_TXT = _resolve_file('./files/test_io/input-valid-json.txt')
FILE_JSON_UNTAGGED = _resolve_file('./files/test_io/input-untagged.json')
FILE_JSON_OUT = _resolve_file('./files/test_io/output-valid.json')
FILE_JSON_OBJECTS = _resolve_file('./files/test_io/input-valid-objects.json')
FILE_JSON_OBJECTS_UNTAGGED = _resolve_file('./files/test_io/input-untagged-objects.json')
FILE_JSON_OBJECTS_OUT = _resolve_file('./files/test_io/output-valid-objects.json')
FILE_JSON_NESTED = _resolve_file('./files/test_io/input-valid-nested.json')
FILE_JSON_SELECTOR = _resolve_file('./files/test_io/input-valid-json-selector.json')
FILE_MULTILINE = _resolve_file('./files/test_io/input-multiline.csv')
FILE_FUZZY = _resolve_file('./files/test_io/input-fuzzy.csv')
FILE_INVALID = _resolve_file('./files/test_io/input-invalid.csv')
FILE_NOTAG1 = _resolve_file('./files/test_io/input-notag1.html')
FILE_NOTAG2 = _resolve_file('./files/test_io/input-notag2.html')
URL_CSV = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/master/tests/files/test_io/input-valid.csv'
URL_XLSX = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/master/tests/files/test_io/input-valid.xlsx'
URL_XLS = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/test/tests/files/test_io/input-valid.xls'
URL_JSON = 'https://raw.githubusercontent.com/HXLStandard/libhxl-python/master/tests/files/test_io/input-valid.json'
URL_GOOGLE_SHEET_NOHASH = 'https://docs.google.com/spreadsheets/d/1VTswL-w9EI0IdGIBFZoZ-2RmIiebXKsrhv03yd7LlIg/edit'
URL_GOOGLE_SHEET_HASH = 'https://docs.google.com/spreadsheets/d/1VTswL-w9EI0IdGIBFZoZ-2RmIiebXKsrhv03yd7LlIg/edit#gid=299366282'
URL_GOOGLE_FILE = 'https://drive.google.com/file/d/1iA0QU0CEywwCr-zDswg7C_RwZgLqS3gb/view'
URL_GOOGLE_XLSX_VIEW = 'https://docs.google.com/spreadsheets/d/1iA0QU0CEywwCr-zDswg7C_RwZgLqS3gb/edit#gid=930997768'
URL_GOOGLE_OPEN_SHEET = 'https://drive.google.com/open?id=1VTswL-w9EI0IdGIBFZoZ-2RmIiebXKsrhv03yd7LlIg'
URL_GOOGLE_OPEN_FILE = 'https://drive.google.com/open?id=1iA0QU0CEywwCr-zDswg7C_RwZgLqS3gb'


class TestInput(unittest.TestCase):

    def test_array(self):
        self.assertTrue(make_input(DATA).is_repeatable)
        self.assertTrue('#sector' in hxl.data(DATA).tags)

    def test_csv_comma_separated(self):
        with make_input(FILE_CSV, InputOptions(allow_local=True)) as input:
            self.assertFalse(input.is_repeatable)
            self.assertTrue('#sector' in hxl.data(input).tags)

    def test_csv_tab_separated(self):
        with make_input(FILE_TSV, InputOptions(allow_local=True)) as input:
            self.assertFalse(input.is_repeatable)
            self.assertTrue('#sector' in hxl.data(input).tags)

    def test_csv_semicolon_separated(self):
        with make_input(FILE_SSV, InputOptions(allow_local=True)) as input:
            self.assertFalse(input.is_repeatable)
            self.assertTrue('#sector' in hxl.data(input).tags)

    def test_csv_zipped(self):
        with make_input(FILE_ZIP_CSV, InputOptions(allow_local=True)) as input:
            self.assertFalse(input.is_repeatable)
            self.assertTrue('#sector' in hxl.data(input).tags)

    def test_zip_invalid(self):
        """Expect a HXLIOException, not a meaningless TypeError"""
        with self.assertRaises(hxl.input.HXLIOException):
            make_input(FILE_ZIP_INVALID, InputOptions(allow_local=True))

    def test_csv_latin1(self):
        with make_input(FILE_CSV_LATIN1, InputOptions(allow_local=True, encoding="latin1")) as input:
            self.assertTrue('#sector' in hxl.data(input).tags)

    def test_json_lists(self):
        with make_input(FILE_JSON, InputOptions(allow_local=True)) as input:
            self.assertFalse(input.is_repeatable)
            self.assertTrue('#sector' in hxl.data(input).tags)

    def test_json_objects(self):
        with make_input(FILE_JSON_OBJECTS, InputOptions(allow_local=True)) as input:
            self.assertFalse(input.is_repeatable)
            self.assertTrue('#sector' in hxl.data(input).tags)

    def test_json_selector(self):
        SEL1_DATA = [["Coast", "100"]]
        SEL2_DATA = [["Plains", "200"]]

        # make sure legacy selectors still work
        with make_input(FILE_JSON_SELECTOR, InputOptions(allow_local=True, selector="sel1")) as input:
            self.assertEqual(SEL1_DATA, hxl.data(input).values)
        with make_input(FILE_JSON_SELECTOR, InputOptions(allow_local=True, selector="sel2")) as input:
            self.assertEqual(SEL2_DATA, hxl.data(input).values)

        # test JSONPath support
        with make_input(FILE_JSON_SELECTOR, InputOptions(allow_local=True, selector="$.sel1")) as input:
            self.assertEqual(SEL1_DATA, hxl.data(input).values)
            
    def test_xls(self):
        with make_input(FILE_XLS, InputOptions(allow_local=True)) as input:
            self.assertTrue(input.is_repeatable)

    def test_xlsx(self):
        with make_input(FILE_XLSX, InputOptions(allow_local=True)) as input:
            self.assertTrue(input.is_repeatable)
            header_row = next(iter(input))
            self.assertEqual("¿Qué?", header_row[0])

    def test_xlsx_sheet_index(self):
        # a non-existant sheet should throw an exception
        with self.assertRaises(hxl.input.HXLIOException):
            with make_input(FILE_XLSX, InputOptions(allow_local=True, sheet_index=100)) as input:
                pass

    def test_xlsx_merged(self):
        with make_input(FILE_XLSX, InputOptions(allow_local=True, expand_merged=False)) as input:
            self.assertTrue(input.is_repeatable)
            header_row = next(iter(input))
            self.assertEqual("", header_row[1])

        with make_input(FILE_XLSX, InputOptions(allow_local=True, expand_merged=True)) as input:
            self.assertTrue(input.is_repeatable)
            header_row = next(iter(input))
            self.assertEqual("¿Qué?", header_row[1])

    def test_xlsx_info(self):
        with make_input(FILE_XLSX_INFO, InputOptions(allow_local=True)) as input:
            report = input.info()

            self.assertEqual("XLSX", report["format"])
            
            self.assertEqual(2, len(report["sheets"]))

            # Sheet 1
            self.assertEqual("input-quality-no-hxl", report["sheets"][0]["name"])
            self.assertFalse(report["sheets"][0]["is_hidden"]),
            self.assertEqual(5, report["sheets"][0]["nrows"]),
            self.assertEqual(9, report["sheets"][0]["ncols"]),
            self.assertTrue(report["sheets"][0]["has_merged_cells"])
            self.assertFalse(report["sheets"][0]["is_hxlated"])
            self.assertEqual("56c6270ee039646436af590e874e6f67", report["sheets"][0]["header_hash"])
            self.assertTrue(report["sheets"][0]["hashtag_hash"] is None)

            # Sheet 2
            self.assertEqual("input-quality-hxl", report["sheets"][1]["name"])
            self.assertFalse(report["sheets"][1]["is_hidden"]),
            self.assertEqual(6, report["sheets"][1]["nrows"]),
            self.assertEqual(9, report["sheets"][1]["ncols"]),
            self.assertFalse(report["sheets"][1]["has_merged_cells"])
            self.assertTrue(report["sheets"][1]["is_hxlated"])
            self.assertEqual("56c6270ee039646436af590e874e6f67", report["sheets"][1]["header_hash"])
            self.assertEqual("3252897e927737b2f6f423dccd07ac93", report["sheets"][1]["hashtag_hash"])

    def test_ckan_resource(self):
        source = hxl.data('https://data.humdata.org/dataset/hxl-master-vocabulary-list/resource/d22dd1b6-2ff0-47ab-85c6-08aeb911a832')
        self.assertTrue('#vocab' in source.tags)

    def test_ckan_dataset(self):
        source = hxl.data('https://data.humdata.org/dataset/hxl-master-vocabulary-list')
        self.assertTrue('#vocab' in source.tags)

    def test_bytes_buffer(self):
        """Test reading from a string via BytesIO"""
        source = hxl.data(io.BytesIO("#org\nOrg A".encode('utf-8')))
        self.assertTrue('#org' in source.tags)

    def test_optional_params(self):
        url = 'https://data.humdata.org/dataset/hxl-master-vocabulary-list/resource/d22dd1b6-2ff0-47ab-85c6-08aeb911a832'
        hxl.input.make_input(url, InputOptions(verify_ssl=True, timeout=30, http_headers={'User-Agent': 'libhxl-python'}))
        hxl.data(url, InputOptions(verify_ssl=True, timeout=30, http_headers={'User-Agent': 'libhxl-python'}))

    def test_file_object(self):
        with open(FILE_CSV, 'r') as f:
            self.assertNotNull(hxl.input.make_input(f))
        

class TestUntaggedInput(unittest.TestCase):

    def test_untagged_json(self):
        with hxl.input.make_input(FILE_JSON_UNTAGGED, InputOptions(allow_local=True)) as input:
            self.assertEqual([
                ['Qué?', '', '', 'Quién?', 'Para quién?', '', 'Dónde?', 'Cuándo?'],
                ['Registro', 'Sector/Cluster', 'Subsector', 'Organización', 'Hombres', 'Mujeres', 'País', 'Departamento/Provincia/Estado'],
                ['001', 'WASH', 'Higiene', 'ACNUR', '100', '100', 'Panamá', 'Los Santos', '1 March 2015'],
                ['002', 'Salud', 'Vacunación', 'OMS', '', '', 'Colombia', 'Cauca', ''],
                ['003', 'Educación', 'Formación de enseñadores', 'UNICEF', '250', '300', 'Colombia', 'Chocó', ''],
                ['004', 'WASH', 'Urbano', 'OMS', '80', '95', 'Venezuela', 'Amazonas', '']
            ], list(input))

    def test_untagged_json_objects(self):
        with hxl.input.make_input(FILE_JSON_OBJECTS_UNTAGGED, InputOptions(allow_local=True)) as input:
            self.assertEqual([
                ['Registro', 'Sector/Cluster', 'Subsector', 'Organización', 'Hombres', 'Mujeres', 'País', 'Departamento/Provincia/Estado'],
                ['001', 'WASH', 'Higiene', 'ACNUR', '100', '100', 'Panamá', 'Los Santos'],
                ['002', 'Salud', 'Vacunación', 'OMS', '', '', 'Colombia', 'Cauca'],
                ['003', 'Educación', 'Formación de enseñadores', 'UNICEF', '250', '300', 'Colombia', 'Chocó'],
                ['004', 'WASH', 'Urbano', 'OMS', '80', '95', 'Venezuela', 'Amazonas']
            ], list(input))

    def test_untagged_zipped_csv(self):
        with hxl.input.make_input(FILE_ZIP_CSV_UNTAGGED, InputOptions(allow_local=True)) as input:
            self.assertEqual([
                ['Registro', 'Sector/Cluster', 'Subsector', 'Organización', 'Hombres', 'Mujeres', 'País', 'Departamento/Provincia/Estado'],
                ['001', 'WASH', 'Higiene', 'ACNUR', '100', '100', 'Panamá', 'Los Santos'],
                ['002', 'Salud', 'Vacunación', 'OMS', '', '', 'Colombia', 'Cauca'],
                ['003', 'Educación', 'Formación de enseñadores', 'UNICEF', '250', '300', 'Colombia', 'Chocó']
            ], list(input))

    def test_html(self):
        """ Reject HTML for tagging """

        with self.assertRaises(hxl.input.HXLHTMLException):
            input = hxl.make_input("https://example.org")
            list(input)

        with self.assertRaises(hxl.input.HXLIOException):
            input = hxl.make_input(FILE_NOTAG1, InputOptions(allow_local=True))
            list(input)

        with self.assertRaises(hxl.input.HXLIOException):
            input = hxl.make_input(FILE_NOTAG2, InputOptions(allow_local=True))
            list(input)

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
            source = hxl.data('XXXXX', InputOptions(allow_local=True))

    def test_bad_url(self):
        with self.assertRaises(IOError):
            source = hxl.data('http://x.localhost/XXXXX', InputOptions(timeout=1))

    def test_local_file_fails(self):
        with self.assertRaises(hxl.input.HXLIOException):
            # allow_local should default to False
            source = hxl.data("/etc/passwd")

    def test_ip_address_fails(self):
        with self.assertRaises(hxl.input.HXLIOException):
            # allow_local should default to False
            source = hxl.data("http://127.0.0.1/index.html")

    def test_localhost_fails(self):
        with self.assertRaises(hxl.input.HXLIOException):
            # allow_local should default to False
            source = hxl.data("http://localhost/index.html")

    def test_localdomain_fails(self):
        with self.assertRaises(hxl.input.HXLIOException):
            # allow_local should default to False
            source = hxl.data("http://foo.localdomain/index.html")
            

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
        with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
            # logical row count
            for row in source:
                row_count += 1
        self.assertEqual(TestParser.EXPECTED_ROW_COUNT, row_count)

    def test_headers(self):
        with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
            headers = source.headers
        self.assertEqual(TestParser.EXPECTED_HEADERS, headers)

    def test_tags(self):
        with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
            tags = source.tags
        self.assertEqual(TestParser.EXPECTED_TAGS, tags)

    def test_empty_header_row(self):
        """Test for exception parsing an empty header row"""
        DATA = [
            [],
            ['X', 'Y'],
            ['#adm1', '#affected'],
            ['Coast', '100']
        ]
        hxl.data(DATA).columns

    def test_attributes(self):
        with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
            for row in source:
                for column_number, column in enumerate(row.columns):
                    self.assertEqual(set(TestParser.EXPECTED_ATTRIBUTES[column_number]), column.attributes)

    def test_column_count(self):
        with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
            for row in source:
                self.assertEqual(len(TestParser.EXPECTED_TAGS), len(row.values))

    def test_columns(self):
        with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
            for row in source:
                for column_number, column in enumerate(row.columns):
                    self.assertEqual(TestParser.EXPECTED_TAGS[column_number], column.tag)

    def test_multiline(self):
        with hxl.data(FILE_MULTILINE, InputOptions(allow_local=True)) as source:
            for row in source:
                self.assertEqual("Line 1\nLine 2\nLine 3", row.get('description'))

    def test_local_csv(self):
        """Test reading from a local CSV file."""
        with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
            self.compare_input(source)

    def test_local_xlsx(self):
        """Test reading from a local XLSX file."""
        with hxl.data(FILE_XLSX, InputOptions(allow_local=True)) as source:
            self.compare_input(source)

    def test_local_xls(self):
        """Test reading from a local XLS (legacy) file."""
        with hxl.data(FILE_XLS, InputOptions(allow_local=True)) as source:
            self.compare_input(source)

    def test_local_xlsx_broken(self):
        """Test reading from a local XLSX file."""
        with hxl.data(FILE_XLSX_BROKEN, InputOptions(allow_local=True)) as source:
            source.columns # just do something 

    def test_local_xlsx_wrong_ext(self):
        """Test reading from a local XLSX file with the wrong extension."""
        with hxl.data(FILE_XLSX_NOEXT, InputOptions(allow_local=True)) as source:
            self.compare_input(source)

    def test_local_json(self):
        """Test reading from a local JSON file."""
        with hxl.data(FILE_JSON, InputOptions(allow_local=True)) as source:
            self.compare_input(source)

    def test_local_json_text(self):
        """Test reading from a local JSON file that doesn't have a JSON extension."""
        with hxl.data(FILE_JSON_TXT, InputOptions(allow_local=True)) as source:
            self.compare_input(source)

    def test_local_json_objects(self):
        """Test reading from a local JSON file."""
        with hxl.data(FILE_JSON_OBJECTS, InputOptions(allow_local=True)) as source:
            self.compare_input(source)

    def test_local_json_nested(self):
        """Test reading from a local JSON file."""
        with hxl.data(FILE_JSON_NESTED, InputOptions(allow_local=True)) as source:
            self.compare_input(source)

    def test_remote_csv(self):
        """Test reading from a remote CSV file (will fail without connectivity)."""
        with hxl.data(URL_CSV, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def test_remote_xlsx(self):
        """Test reading from a remote XLSX file (will fail without connectivity)."""
        with hxl.data(URL_XLSX, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def test_remote_xls(self):
        """Test reading from a remote XLSX file (will fail without connectivity)."""
        with hxl.data(URL_XLS, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def x_test_remote_json(self):
        """Test reading from a remote JSON file (will fail without connectivity)."""
        with hxl.data(URL_JSON) as source:
            self.compare_input(source)

    def test_google_sheet_nohash(self):
        # Google Sheet, default tab
        with hxl.data(URL_GOOGLE_SHEET_NOHASH, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def test_google_sheet_hash(self):
        # Google Sheet, specific tab
        with hxl.data(URL_GOOGLE_SHEET_HASH, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def test_google_file(self):
        # Google Sheet, specific tab
        with hxl.data(URL_GOOGLE_FILE, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def test_google_drive_sheet(self):
        # Google Drive, "open" link for sheet
        with hxl.data(URL_GOOGLE_OPEN_SHEET, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def test_google_drive_file(self):
        # Google Drive, "open" link for file
        with hxl.data(URL_GOOGLE_OPEN_FILE, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def test_google_xlsx_view(self):
        # Google drive XLSX in view mode
        with hxl.data(URL_GOOGLE_XLSX_VIEW, InputOptions(timeout=10)) as source:
            self.compare_input(source)

    def test_fuzzy(self):
        """Imperfect hashtag row should still work."""
        with hxl.data(FILE_FUZZY, InputOptions(allow_local=True)) as source:
            source.tags

    def test_invalid(self):
        """Missing hashtag row should raise an exception."""
        with self.assertRaises(HXLParseException):
            with hxl.data(FILE_INVALID, InputOptions(allow_local=True)) as source:
                source.tags

    def compare_input(self, source):
        """Compare an external source to the expected content."""
        for i, row in enumerate(source):
            for j, value in enumerate(row):
                if value is None:
                    value = ''
                # For XLSX, numbers may be pre-parsed
                try:
                    self.assertEqual(float(TestParser.EXPECTED_CONTENT[i][j]), float(value))
                except:
                    self.assertEqual(TestParser.EXPECTED_CONTENT[i][j], value)


class TestLocationInformation(unittest.TestCase):
    """Test location information for rows and columns"""

    def test_multiple_header_row_number(self):
        with hxl.data(_resolve_file('files/test_io/input-multiple-headers.csv'), InputOptions(allow_local=True)) as source:
            for row in source:
                self.assertEqual(row.source_row_number, row.row_number+3) # there are two header rows and the hashtags
                for i, column in enumerate(row.columns):
                    self.assertEqual(i, column.column_number)

    def test_google_row_number(self):
        source = hxl.data('https://docs.google.com/spreadsheets/d/1rOO0-xYa3kIOfI-6KR-mLgMTdgIEijNxM52Nfhs8uvg/edit#gid=0')
        for row in source:
            self.assertTrue(row.source_row_number is not None)
            self.assertEqual(row.source_row_number, row.row_number+1) # there are two header rows and the hashtags
            for i, column in enumerate(row.columns):
                self.assertEqual(i, column.column_number)

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
        input = hxl.input.tagger(TestFunctions.DATA_UNTAGGED, {
                "District": "#org"
        })
        self.assertEqual(TestFunctions.DATA_UNTAGGED[1:], [row.values for row in input])

    def test_write_csv(self):
        with open(FILE_CSV_OUT, 'rb') as input:
            expected = input.read()
            buffer = StringIO()
            with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
                hxl.input.write_hxl(buffer, source)
                # Need to work with bytes to handle CRLF
                self.assertEqual(expected, buffer.getvalue().encode('utf-8'))

    def test_write_json_lists(self):
        with open(FILE_JSON_OUT) as input:
            expected = input.read()
            buffer = StringIO()
            with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
                hxl.input.write_json(buffer, source)
                self.assertEqual(expected, buffer.getvalue())

    def test_write_json_objects(self):
        with open(FILE_JSON_OBJECTS_OUT) as input:
            expected = input.read()
            buffer = StringIO()
            with hxl.data(FILE_CSV, InputOptions(allow_local=True)) as source:
                hxl.input.write_json(buffer, source, use_objects=True)
                self.assertEqual(expected, buffer.getvalue())

    def test_write_json_attribute_normalisation(self):
        DATA_IN = [
            ['#sector+es+cluster'],
            ['Hygiene']
        ]
        DATA_OUT = [
            {
                '#sector+cluster+es': 'Hygiene'
            }
        ]
        buffer = StringIO()
        source = hxl.data(DATA_IN)
        hxl.input.write_json(buffer, source, use_objects=True)
        self.assertEqual(DATA_OUT, json.loads(buffer.getvalue()))
            
