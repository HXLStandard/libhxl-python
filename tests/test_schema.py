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

class TestSchema(unittest.TestCase):
    """Test the hxl.validation.Schema class."""

    def setUp(self):
        self.errors = []
        self.schema = Schema(
            rules=[
                SchemaRule('#sector', min_occur=1),
                SchemaRule('#affected', data_type='number')
                ],
            callback = lambda error: self.errors.append(error)
            )
        self.row = Row(
            columns = [
                Column(tag='#affected'),
                Column(tag='#sector'),
                Column(tag='#sector')
            ]
        )

    def test_row(self):
        self.try_schema(['35', 'WASH', ''])
        self.try_schema(['35', 'WASH', 'Health'])

        self.try_schema(['35', '', ''], 1)
        self.try_schema(['abc', 'WASH', ''], 2)

    def try_schema(self, row_values, errors_expected = 0):
        self.row.values = row_values
        if errors_expected == 0:
            self.assertTrue(self.schema.validate_row(self.row))
        else:
            self.assertFalse(self.schema.validate_row(self.row))
        self.assertEqual(len(self.errors), errors_expected)
        

class TestSchemaRule(unittest.TestCase):
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


class TestSchemaLoad(unittest.TestCase):
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
