"""
Unit tests for the hxl.schema module
David Megginson
November 2014

License: Public Domain
"""

import unittest
from hxl.model import HXLColumn, HXLRow
from hxl.schema import HXLSchema, HXLSchemaRule

class TestSchema(unittest.TestCase):

    def setUp(self):
        self.errors = []

    def test_row(self):
        schema = HXLSchema(
            rules=[HXLSchemaRule('#sector', minOccur=1), HXLSchemaRule('#affected_num', dataType=HXLSchemaRule.TYPE_NUMBER)]
            )
        row = HXLRow(
            columns = [HXLColumn(hxlTag='#affected_num'), HXLColumn(hxlTag='#sector'), HXLColumn(hxlTag='#sector')],
            )

        row.values = ['35', 'WASH', '']
        self.assertTrue(schema.validateRow(row))
        
        row.values = ['35', 'WASH', 'Health']
        self.assertTrue(schema.validateRow(row))
        
        row.values = ['35', '', '']
        self.assertFalse(schema.validateRow(row))

        row.values = ['abc', 'WASH', '']
        self.assertFalse(schema.validateRow(row))
        
class TestSchemaRule(unittest.TestCase):

    def setUp(self):
        self.errors = []
        self.rule = HXLSchemaRule('#x_test', callback=lambda error: self.errors.append(error))

    def test_type_none(self):
        self.assertTrue(self.rule.validate(''))
        self._test_errors()

        self.assertTrue(self.rule.validate(10))
        self._test_errors()

        self.assertTrue(self.rule.validate('hello, world'))
        self._test_errors()

    def test_type_text(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_TEXT, callback=self._callback)

        self.assertTrue(rule.validate(''))
        self._test_errors()

        self.assertTrue(rule.validate(10))
        self._test_errors()

        self.assertTrue(rule.validate('hello, world'))
        self._test_errors()

    def test_type_num(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_NUMBER, callback=self._callback)

        self.assertTrue(rule.validate(10))
        self._test_errors()

        self.assertTrue(rule.validate(' -10.1  '))
        self._test_errors()

        self.assertFalse(rule.validate('ten'))
        self._test_errors(1)

    def test_type_url(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_URL, callback=self._callback)

        self.assertTrue(rule.validate('http://www.example.org'))
        self._test_errors()

        self.assertFalse(rule.validate('hello, world'))
        self._test_errors(1)

    def test_type_email(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_EMAIL, callback=self._callback)

        self.assertTrue(rule.validate('somebody@example.org'))
        self._test_errors()

        self.assertFalse(rule.validate('hello, world'))
        self._test_errors(1)

    def test_type_phone(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_PHONE, callback=self._callback)

        self.assertTrue(rule.validate('+1-613-555-1111 x1234'))
        self._test_errors()

        self.assertTrue(rule.validate('(613) 555-1111'))
        self._test_errors()

        self.assertFalse(rule.validate('123'))
        self._test_errors(1)

        self.assertFalse(rule.validate('123456789abc'))
        self._test_errors(1)

    def test_value_range(self):
        rule = HXLSchemaRule('#sector',minValue=3.5, maxValue=4.5, callback=self._callback)
        self.assertTrue(rule.validate(4.0))
        self.assertTrue(rule.validate('4'))
        self.assertFalse(rule.validate('3.49'))
        self.assertFalse(rule.validate('5.0'))

    def test_value_pattern(self):
        self.rule.valuePattern = '^a+b$'
        self.validate_value('ab', 0)
        self.validate_value('aab', 0)
        self.validate_value('bb', 1)

    def test_value_enumeration(self):
        rule = HXLSchemaRule('#sector',valueEnumeration=['aa', 'bb', 'cc'], callback=self._callback)
        self.assertTrue(rule.validate('bb'))
        self.assertFalse(rule.validate('BB'))
        self.assertFalse(rule.validate('dd'))

        rule.caseSensitive = False
        self.assertTrue(rule.validate('bb'))
        self.assertTrue(rule.validate('BB'))
        self.assertFalse(rule.validate('dd'))

    def test_row_restrictions(self):
        row = HXLRow([HXLColumn(hxlTag='#sector'), HXLColumn(hxlTag='#subsector'), HXLColumn(hxlTag='#sector')]);
        row.values = ['WASH', '', ''];

        rule = HXLSchemaRule('#sector', minOccur=1, callback=self._callback)
        self.assertTrue(rule.validateRow(row))

        rule = HXLSchemaRule('#sector', minOccur=2, callback=self._callback)
        self.assertFalse(rule.validateRow(row))

        rule = HXLSchemaRule('#sector', maxOccur=1, callback=self._callback)
        self.assertTrue(rule.validateRow(row))

        rule = HXLSchemaRule('#sector', maxOccur=0, callback=self._callback)
        self.assertFalse(rule.validateRow(row))

    def validate_value(self, value, errors_expected = 0):
        """
        Validate a single value with a HXLSchemaRule
        """
        if errors_expected == 0:
            self.assertTrue(self.rule.validate(value))
        else:
            self.assertFalse(self.rule.validate(value))
        self.assertEqual(len(self.errors), errors_expected)
        self.errors = [] # clear errors for the next run

    def _callback(self, error):
        self.errors.append(error)

    def _test_errors(self, number_expected = 0):
        self.assertEqual(len(self.errors), number_expected)
        self.errors = []

# end
