"""
Unit tests for the hxl.schema module
David Megginson
November 2014

License: Public Domain
"""

import unittest
import os
from hxl import hxl
from hxl.model import Column, Row
from hxl.schema import Schema, SchemaRule, read_schema

class TestSchema(unittest.TestCase):

    def setUp(self):
        self.errors = []
        self.schema = Schema(
            rules=[
                SchemaRule('#sector', min_occur=1),
                SchemaRule('#affected_num', data_type='number')
                ],
            callback = lambda error: self.errors.append(error)
            )
        self.row = Row(
            columns = [
                Column(tag='#affected_num'),
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

    def test_load_default(self):
        schema = read_schema()
        self.assertTrue(0 < len(schema.rules))
        with _read_file('data-good.csv') as input:
            dataset = hxl(input)
            self.assertTrue(schema.validate(dataset))

    def test_load_good(self):
        with _read_file('schema-basic.csv') as schema_input:
            schema = read_schema(hxl(schema_input))
            with _read_file('data-good.csv') as input:
                dataset = hxl(input)
                self.assertTrue(schema.validate(dataset))

    def test_load_bad(self):
        with _read_file('schema-basic.csv') as schema_input:
            schema = read_schema(hxl(schema_input))
            schema.callback = lambda e: True # to avoid seeing error messages
            with _read_file('data-bad.csv') as input:
                dataset = hxl(input)
                self.assertFalse(schema.validate(dataset))

    def _try_rule(self, value, errors_expected = 0):
        """
        Validate a single value with a SchemaRule
        """
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

########################################################################
# Support functions
########################################################################

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))
file_dir = os.path.join(root_dir, 'tests', 'files', 'test_schema')

def _read_file(name):
    return open(os.path.join(file_dir, name), 'r')

# end
