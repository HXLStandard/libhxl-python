# coding=utf-8
"""
Unit tests for the hxl.schema module
David Megginson
November 2014

License: Public Domain
"""

import unittest
import sys
import os

import hxl
from hxl.model import Column, Row
from hxl.validation import Schema, SchemaRule

from . import resolve_path


class TestRule(unittest.TestCase):
    """Test the hxl.validation.SchemaRule class."""

    def setUp(self):
        self.errors = []
        self.rule = SchemaRule('#x_test', callback=lambda error: self.errors.append(error), severity="warning")

    def test_severity(self):
        self.rule.data_type = 'number'
        self._try_rule('xxx', 1)
        self.assertEqual('warning', self.errors[0].rule.severity)

    def test_type_none(self):
        self._try_rule('')
        self._try_rule(10)
        self._try_rule('hello, world')

    def test_type_text(self):
        self.rule.data_type = 'text'
        self._try_rule('')
        self._try_rule(10)
        self._try_rule('hello, world')

    def test_type_num(self):
        self.rule.data_type = 'number'
        self._try_rule(10)
        self._try_rule(' -10.1 ');
        self._try_rule('ten', 1)

    def test_type_url(self):
        self.rule.data_type = 'url'
        self._try_rule('http://www.example.org')
        self._try_rule('hello, world', 1)

    def test_type_email(self):
        self.rule.data_type = 'email'
        self._try_rule('somebody@example.org')
        self._try_rule('hello, world', 1)

    def test_type_phone(self):
        self.rule.data_type = 'phone'
        self._try_rule('+1-613-555-1111 x1234')
        self._try_rule('(613) 555-1111')
        self._try_rule('123', 1)
        self._try_rule('123456789abc', 1)
        
    def test_type_date(self):
        self.rule.data_type = 'date'
        self._try_rule('2015-03-15')
        self._try_rule('2015-03')
        self._try_rule('2015')
        self._try_rule('xxx', 1)

    def test_value_whitespace(self):
        self.rule.check_whitespace = True
        self._try_rule('xxx', 0)
        self._try_rule('xxx yyy', 0)
        self._try_rule(' xxx', 1) # leading space not allowed
        self._try_rule('xxx  ', 1) # trailing space not allowed
        self._try_rule('xxx  yyy', 1) # multiple internal spaces not allowed
        self._try_rule("xxx\tyyy", 1) # tabs not allowed

    def test_value_range(self):
        self.rule.min_value = 3.5
        self.rule.max_value = 4.5
        self._try_rule(4.0)
        self._try_rule('4')
        self._try_rule('3.49', 1)
        self._try_rule(5.0, 1)

    def test_value_pattern(self):
        self.rule.regex = '^a+b$'
        self._try_rule('ab', 0)
        self._try_rule('aab', 0)
        self._try_rule('bb', 1)

    def test_value_enumeration(self):
        self.rule.enum=['aa', 'bb', 'cc']

        self.rule.case_sensitive = True
        self._try_rule('bb')
        self._try_rule('BB', 1)
        self._try_rule('dd', 1)

        self.rule.case_sensitive = False
        self._try_rule('bb')
        self._try_rule('BB')
        self._try_rule('dd', 1)

    def test_suggested_value_enumeration(self):
        def callback(error):
            self.assertEqual('cc', error.suggested_value)
        self.rule.callback = callback
        self.rule.enum = ['aa', 'bb', 'cc']
        self.rule.validate('ccc')
        self.rule.validate('dcc')
        self.rule.validate('cdc')

    def test_row_restrictions(self):
        row = Row(
            columns = [
                Column(tag='#x_test'),
                Column(tag='#subsector'),
                Column(tag='#x_test')
            ],
            values=['WASH', '', '']
            );

        self.rule.min_occur = 1
        self._try_rule(row)

        self.rule.min_occur = 2
        self._try_rule(row, 1)

        self.rule.min_occur = None

        self.rule.max_occur = 1
        self._try_rule(row)

        self.rule.max_occur = 0
        self._try_rule(row, 1)


    def _try_rule(self, value, errors_expected = 0):
        """Helper: Validate a single value with a SchemaRule"""
        self.errors = [] # clear errors for the next run
        if isinstance(value, Row):
            result = self.rule.validate_row(value)
        else:
            result = self.rule.validate(value)
        if errors_expected == 0:
            self.assertTrue(result)
        else:
            self.assertFalse(result)
        self.assertEqual(len(self.errors), errors_expected)


class TestValidateRow(unittest.TestCase):
    """Test the hxl.validation.Schema class."""

    DEFAULT_COLUMNS = ['#affected', '#sector', '#sector', '#sector']

    DEFAULT_SCHEMA = [
        ['#valid_tag', '#valid_datatype', '#valid_required+min', '#valid_required+max'],
        ['#sector', '', '1', '2'],
        ['#affected', 'number', '', '']
    ]

    def test_minmax(self):
        # sector is allowed 1 or 2 times
        self.assertRowErrors(['35', '', '', ''], 1)
        self.assertRowErrors(['35', 'WASH', '', ''], 0)
        self.assertRowErrors(['35', 'WASH', 'Health', ''], 0)
        self.assertRowErrors(['35', 'WASH', 'Health', 'Education'], 1)

    def test_number(self):
        self.assertRowErrors(['35', 'WASH', ''], 0)
        self.assertRowErrors(['abc', 'WASH', ''], 1)

    def test_date(self):
        COLUMNS = ['#date']
        SCHEMA = [
            ['#valid_tag', '#valid_datatype'],
            ['#date', 'date'],
        ]
        self.assertRowErrors(['2017-01-01'], 0, columns=COLUMNS, schema=SCHEMA)
        self.assertRowErrors(['1/1/17'], 0, columns=COLUMNS, schema=SCHEMA)
        self.assertRowErrors(['13/13/17'], 1, columns=COLUMNS, schema=SCHEMA)

    def test_url(self):
        COLUMNS = ['#meta+url']
        SCHEMA = [
            ['#valid_tag', '#valid_datatype'],
            ['#meta+url', 'url'],
        ]
        self.assertRowErrors(['http://example.org'], 0, columns=COLUMNS, schema=SCHEMA)
        self.assertRowErrors(['example.org'], 1, columns=COLUMNS, schema=SCHEMA)

    def test_email(self):
        COLUMNS = ['#contact+email']
        SCHEMA = [
            ['#valid_tag', '#valid_datatype'],
            ['#contact+email', 'email'],
        ]
        self.assertRowErrors(['nobody@example.org'], 0, columns=COLUMNS, schema=SCHEMA)
        self.assertRowErrors(['nobody@@example.org'], 1, columns=COLUMNS, schema=SCHEMA)

    def assertRowErrors(self, row_values, errors_expected, schema=None, columns=None):
        """Set up a HXL row and count the errors in it"""
        errors = []

        if schema is None:
            schema = self.DEFAULT_SCHEMA
        schema = hxl.schema(schema, lambda error: errors.append(error))

        if columns is None:
            columns = self.DEFAULT_COLUMNS

        row = Row(
            values=row_values,
            columns=[Column.parse(tag) for tag in columns]
        )

        if errors_expected == 0:
            self.assertTrue(schema.validate_row(row))
        else:
            self.assertFalse(schema.validate_row(row))
        self.assertEqual(len(errors), errors_expected)


class TestValidateDataset(unittest.TestCase):
    """Test dataset-wide validation"""

    DEFAULT_SCHEMA = [
        ['#valid_tag', '#valid_unique'],
        ['#meta+id', 'true'],
    ]

    def test_unique_single(self):
        DATASET = [
            ['#meta+id'],
            ['foo'],
            ['bar'],
            ['foo']
        ]
        self.assertDatasetErrors(DATASET[:3], 0)
        self.assertDatasetErrors(DATASET, 1)

    def test_unique_compound(self):
        SCHEMA = [
            ['#valid_tag', '#valid_unique+key'],
            ['#org', 'org,sector,adm1']
            ]
        DATASET = [
            ['#org', '#sector', '#adm1', '#output'],
            ['OrgA', 'Shelter', 'Coast', 'sheets'],
            ['OrgA', 'Shelter', 'Plains', 'sheets'],
            ['OrgA', 'Shelter', 'Coast', 'tents'],
        ]
        self.assertDatasetErrors(DATASET[:3], 0, schema=SCHEMA)
        self.assertDatasetErrors(DATASET, 1, schema=SCHEMA)

    def test_consistent_datatype(self):
        def callback(e):
            # expect that 'xxx' will be the bad value
            self.assertEqual('xxx', e.value)

        schema = hxl.schema([
            ['#valid_tag', '#valid_datatype+consistent'],
            ['#affected', 'true']
        ], callback=callback)

        data = hxl.data([
            ['#affected'],
            ['100'],
            ['xxx'],
            ['200'],
            ['800']
        ])

        self.assertFalse(schema.validate(data))

    def test_correlation(self):
        SCHEMA = [
            ['#valid_tag', '#valid_correlation'],
            ['#adm1+name', '#adm1+code']
        ]
        DATASET = [
            ['#adm1+name', '#adm1+code', '#sector'],
            ['Coast', 'X001', 'WASH'],
            ['Plains', 'X002', 'Education'],
            ['Plains', 'X002', 'Education'],
            ['Plains', 'X002', 'Education'],
            ['Plains', 'X002', 'Health'],
            ['Coast', 'X002', 'WASH'],
            ['Plains', 'X001', 'Education']
        ]
        self.assertDatasetErrors(DATASET[:6], 0, schema=SCHEMA)
        self.assertDatasetErrors(DATASET[:7], 1, schema=SCHEMA)
        self.assertDatasetErrors(DATASET, 2, schema=SCHEMA)

    def test_suggested_value_correlation_key(self):
        """Complex test: can we suggest a value based on the correlation key?"""
        def callback(e):
            self.assertEqual('yy', e.suggested_value)
        schema = hxl.schema([
            ['#valid_tag', '#valid_correlation'],
            ['#foo', '#bar']
        ], callback)
        data = hxl.data([
            ['#foo', '#bar'],
            ['yy', 'yyy'],
            ['yy', 'yyy'],
            ['xx', 'xxx'],
            ['xx', 'xxx'],
            ['xx', 'yyy'],
        ])
        self.assertFalse(schema.validate(data))

    def assertDatasetErrors(self, dataset, errors_expected, schema=None):
        errors = []

        def callback(error):
            errors.append(error)

        if schema is None:
            schema = self.DEFAULT_SCHEMA
        schema = hxl.schema(schema, callback)

        if errors_expected == 0:
            self.assertTrue(schema.validate(hxl.data(dataset)))
        else:
            self.assertFalse(schema.validate(hxl.data(dataset)))

        self.assertEqual(len(errors), errors_expected)


class TestLoad(unittest.TestCase):
    """Test schema I/O support."""

    def test_load_default(self):
        schema = hxl.schema()
        self.assertTrue(0 < len(schema.rules))
        self.assertTrue(schema.validate(hxl.data(DATA_GOOD)))

    def test_load_good(self):
        schema = hxl.schema(SCHEMA_BASIC)
        self.assertTrue(schema.validate(hxl.data(DATA_GOOD)))

    def test_load_bad(self):
        schema = hxl.schema(SCHEMA_BASIC)
        self.assertFalse(schema.validate(hxl.data(DATA_BAD)))

    # def test_taxonomy_good(self):
    #     schema = hxl.schema(SCHEMA_TAXONOMY)
    #     self.assertTrue(schema.validate(hxl.data(DATA_TAXONOMY_GOOD)))

    # def test_taxonomy_bad(self):
    #     schema = hxl.schema(SCHEMA_TAXONOMY)
    #     self.assertFalse(schema.validate(hxl.data(DATA_TAXONOMY_BAD)))

    # def test_taxonomy_all(self):
    #     schema = hxl.schema(SCHEMA_TAXONOMY_ALL)
    #     self.assertTrue(schema.validate(hxl.data(DATA_TAXONOMY_BAD)))


class TestJSON(unittest.TestCase):

    def test_truthy(self):
        schema = hxl.schema(hxl.data(resolve_path('files/test_validation/truthy-schema.json'), allow_local=True))
        BAD_DATA = [
            ['#sector'],
            ['Health']
        ]
        self.assertFalse(schema.validate(hxl.data(BAD_DATA)))
        GOOD_DATA = [
            ['#adm2+code'],
            ['xxx']
        ]
        self.assertTrue(schema.validate(hxl.data(GOOD_DATA)))

#
# Test data
#

# Basic schema
SCHEMA_BASIC = [
    ['#valid_tag', '#valid_required', '#valid_required+max', '#valid_datatype', '#valid_value+min', '#valid_value+max', '#valid_value+list'],
    ['#sector', 'true', '', 'text', '', '', 'WASH|Salud|Educación'],
    ['#subsector', 'true', '2', 'text', '', '', ''],
    ['#org', 'true', '1', 'text', '', '', ''],
    ['#targeted', '', '1', 'number', '0', '1000000000', ''],
    ['#country', 'true', '', 'text', '', '', ''],
    ['#adm1', '', '1', 'text', '', '', '']
]

# Data that validates properly
DATA_GOOD = [
    ['Sector/Cluster', 'Subsector', 'Organización', 'Targeted', 'País', 'Departamento/Provincia/Estado'],
    ['#sector', '#subsector', '#org', '#targeted', '#country', '#adm1'],
    ['WASH', 'Higiene', 'ACNUR', '100', 'Panamá', 'Los Santos'],
    ['Salud', 'Vacunación', 'OMS', '', 'Colombia', 'Cauca'],
    ['Educación', 'Formación de enseñadores', 'UNICEF', '250', 'Colombia', 'Chocó'],
    ['WASH', 'Urbano', 'OMS', '80', 'Venezuela', 'Amazonas']
]

# Data that fails validation with the basic schema (missing sector in second data row)
DATA_BAD = [
    ['Sector/Cluster', 'Subsector', 'Organización', 'Targeted', 'País', 'Departamento/Provincia/Estado'],
    ['#sector', '#subsector', '#org', '#targeted', '#country', '#adm1'],
    ['WASH', 'Higiene', 'ACNUR', '100', 'Panamá', 'Los Santos'],
    ['', 'Vacunación', 'OMS', '', 'Colombia', 'Cauca'],
    ['Educación', 'Formación de enseñadores', 'UNICEF', '250', 'Colombia', 'Chocó'],
    ['WASH', 'Urbano', 'OMS', '80', 'Venezuela', 'Amazonas']
]

# Taxonomy rule with a tag selector
SCHEMA_TAXONOMY = [
    ['#valid_tag', '#valid_value+url', '#valid_value+target_tag'],
    ['#adm1+code', 'http://example.org/taxonomy.csv', '#adm1+code']
]

# Taxonomy rule without a tag selector
SCHEMA_TAXONOMY_ALL = [
    ['#valid_tag', '#valid_value+url'],
    ['#adm1+code', 'http://example.org/taxonomy.csv']
]

# External taxonomy dataset as a string (for simulation)
TAXONOMY_STRING = """
#adm1,#adm1+code
Coast,C001
Plains,C002
Mountains,C003
"""

# Data that follows the taxonomy
DATA_TAXONOMY_GOOD = [
    ['#org', '#sector', '#adm1', '#adm1+code'],
    ['NGO A', 'WASH', 'Coast', 'C001'],
    ['NGO B', 'Education', 'Mountains', 'C003'],
]

# Data that doesn't follow the taxonomy (second p-code is actually a name)
DATA_TAXONOMY_BAD = [
    ['#org', '#sector', '#adm1', '#adm1+code'],
    ['NGO A', 'WASH', 'Coast', 'C001'],
    ['NGO B', 'Education', 'Mountains', 'Mountains'],
]

# end
