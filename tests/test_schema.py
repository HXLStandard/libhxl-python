"""
Unit tests for the hxl.schema module
David Megginson
November 2014

License: Public Domain
"""

import unittest
import os
from hxl.model import Column, Row
from hxl.io import StreamInput, HXLReader
from hxl.schema import Schema, SchemaRule, readSchema
from hxl.taxonomy import Taxonomy, Term

class TestSchema(unittest.TestCase):

    def setUp(self):
        self.errors = []
        self.schema = Schema(
            rules=[
                SchemaRule('#sector', min_occur=1),
                SchemaRule('#affected_num', dataType='number')
                ],
            callback = lambda error: self.errors.append(error)
            )
        self.row = Row(
            columns = [
                Column(tag='#affected_num', column_number=0),
                Column(tag='#sector', column_number=1),
                Column(tag='#sector', column_number=2)
            ],
            row_number = 1,
            source_row_number = 2
        )


    def test_row(self):
        self.try_schema(['35', 'WASH', ''])
        self.try_schema(['35', 'WASH', 'Health'])

        self.try_schema(['35', '', ''], 1)
        self.try_schema(['abc', 'WASH', ''], 2)

    def try_schema(self, row_values, errors_expected = 0):
        self.row.values = row_values
        if errors_expected == 0:
            self.assertTrue(self.schema.validateRow(self.row))
        else:
            self.assertFalse(self.schema.validateRow(self.row))
        self.assertEqual(len(self.errors), errors_expected)
        
class TestSchemaRule(unittest.TestCase):

    def setUp(self):
        self.errors = []
        self.rule = SchemaRule('#x_test', callback=lambda error: self.errors.append(error), severity="warning")

    def test_severity(self):
        self.rule.dataType = 'number'
        self._try_rule('xxx', 1)
        self.assertEqual('warning', self.errors[0].rule.severity)

    def test_type_none(self):
        self._try_rule('')
        self._try_rule(10)
        self._try_rule('hello, world')

    def test_type_text(self):
        self.rule.dataType = 'text'
        self._try_rule('')
        self._try_rule(10)
        self._try_rule('hello, world')

    def test_type_num(self):
        self.rule.dataType = 'number'
        self._try_rule(10)
        self._try_rule(' -10.1 ');
        self._try_rule('ten', 1)

    def test_type_url(self):
        self.rule.dataType = 'url'
        self._try_rule('http://www.example.org')
        self._try_rule('hello, world', 1)

    def test_type_email(self):
        self.rule.dataType = 'email'
        self._try_rule('somebody@example.org')
        self._try_rule('hello, world', 1)

    def test_type_phone(self):
        self.rule.dataType = 'phone'
        self._try_rule('+1-613-555-1111 x1234')
        self._try_rule('(613) 555-1111')
        self._try_rule('123', 1)
        self._try_rule('123456789abc', 1)
        
    def test_type_date(self):
        self.rule.dataType = 'date'
        self._try_rule('2015-03-15')
        self._try_rule('2015-03')
        self._try_rule('2015')
        self._try_rule('xxx', 1)

    def test_type_taxonomy(self):
        # No level specified
        self.rule.taxonomy = _make_taxonomy()
        self._try_rule('AAA') # level 1
        self._try_rule('BBB') # level 2
        self._try_rule('CCC', 1) # not defined

        # Explicit level
        self.rule.taxonomyLevel = 1
        self._try_rule('AAA') # level 1
        self._try_rule('BBB', 1) # level 2
        self._try_rule('CCC', 1) # not defined

    def test_value_range(self):
        self.rule.minValue = 3.5
        self.rule.maxValue = 4.5
        self._try_rule(4.0)
        self._try_rule('4')
        self._try_rule('3.49', 1)
        self._try_rule(5.0, 1)

    def test_value_pattern(self):
        self.rule.valuePattern = '^a+b$'
        self._try_rule('ab', 0)
        self._try_rule('aab', 0)
        self._try_rule('bb', 1)

    def test_value_enumeration(self):
        self.rule.valueEnumeration=['aa', 'bb', 'cc']

        self.rule.caseSensitive = True
        self._try_rule('bb')
        self._try_rule('BB', 1)
        self._try_rule('dd', 1)

        self.rule.caseSensitive = False
        self._try_rule('bb')
        self._try_rule('BB')
        self._try_rule('dd', 1)

    def test_row_restrictions(self):
        row = Row(
            columns=[
                Column(tag='#x_test'),
                Column(tag='#subsector'),
                Column(tag='#x_test')
                ],
            row_number = 1
            );
        row.values = ['WASH', '', ''];

        self.rule.min_occur = 1
        self._try_rule(row)

        self.rule.min_occur = 2
        self._try_rule(row, 1)

        self.rule.min_occur = None

        self.rule.maxOccur = 1
        self._try_rule(row)

        self.rule.maxOccur = 0
        self._try_rule(row, 1)

    def test_load_default(self):
        schema = readSchema()
        self.assertTrue(0 < len(schema.rules))
        with _read_file('data-good.csv') as input:
            dataset = HXLReader(StreamInput(input))
            self.assertTrue(schema.validate(dataset))

    def test_load_good(self):
        with _read_file('schema-basic.csv') as schema_input:
            schema = readSchema(HXLReader(StreamInput(schema_input)))
            with _read_file('data-good.csv') as input:
                dataset = HXLReader(StreamInput(input))
                self.assertTrue(schema.validate(dataset))

    def test_load_bad(self):
        with _read_file('schema-basic.csv') as schema_input:
            schema = readSchema(HXLReader(StreamInput(schema_input)))
            schema.callback = lambda e: True # to avoid seeing error messages
            with _read_file('data-bad.csv') as input:
                dataset = HXLReader(StreamInput(input))
                self.assertFalse(schema.validate(dataset))

    def test_load_taxonomy(self):
        with _read_file('schema-taxonomy.csv') as schema_input:
            with _read_file('data-taxonomy-good.csv') as input:
                schema = readSchema(HXLReader(StreamInput(schema_input)), baseDir=file_dir)
                dataset = HXLReader(StreamInput(input))
                self.assertTrue(schema.validate(dataset))

    def _try_rule(self, value, errors_expected = 0):
        """
        Validate a single value with a SchemaRule
        """
        self.errors = [] # clear errors for the next run
        if isinstance(value, Row):
            result = self.rule.validateRow(value)
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

def _make_taxonomy():
    return Taxonomy(terms={
        'AAA': Term('AAA', level=1),
        'BBB': Term('BBB', level=2)
        })

# end
