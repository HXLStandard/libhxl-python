"""
Unit tests for the hxl.schema module
David Megginson
November 2014

License: Public Domain
"""

import unittest
from hxl.schema import HXLSchemaRule

class TestSchemaRule(unittest.TestCase):

    def setUp(self):
        return True

    def test_type_none(self):
        rule = HXLSchemaRule()
        self.assertTrue(rule.validate(''))
        self.assertTrue(rule.validate(10))
        self.assertTrue(rule.validate('hello, world'))

    def test_type_text(self):
        rule = HXLSchemaRule(dataType=HXLSchemaRule.TYPE_TEXT)
        self.assertTrue(rule.validate(''))
        self.assertTrue(rule.validate(10))
        self.assertTrue(rule.validate('hello, world'))

    def test_type_num(self):
        rule = HXLSchemaRule(dataType=HXLSchemaRule.TYPE_NUM)
        self.assertTrue(rule.validate(10))
        self.assertTrue(rule.validate(' -10.1  '))
        self.assertFalse(rule.validate('ten'))

    def test_type_url(self):
        rule = HXLSchemaRule(dataType=HXLSchemaRule.TYPE_URL)
        self.assertTrue(rule.validate('http://www.example.org'))
        self.assertFalse(rule.validate('hello, world'))

    def test_type_email(self):
        rule = HXLSchemaRule(dataType=HXLSchemaRule.TYPE_EMAIL)
        self.assertTrue(rule.validate('somebody@example.org'))
        self.assertFalse(rule.validate('hello, world'))

    def test_type_phone(self):
        rule = HXLSchemaRule(dataType=HXLSchemaRule.TYPE_PHONE)
        self.assertTrue(rule.validate('+1-613-555-1111 x1234'))
        self.assertTrue(rule.validate('(613) 555-1111'))
        self.assertFalse(rule.validate('123'))
        self.assertFalse(rule.validate('123456789abc'))

    def test_value_range(self):
        rule = HXLSchemaRule(minValue=3.5, maxValue=4.5)
        self.assertTrue(rule.validate(4.0))
        self.assertTrue(rule.validate('4'))
        self.assertFalse(rule.validate('3.49'))
        self.assertFalse(rule.validate('5.0'))

    def test_value_pattern(self):
        rule = HXLSchemaRule(valuePattern='^a+b$')
        self.assertTrue(rule.validate('ab'))
        self.assertTrue(rule.validate('aab'))
        self.assertFalse(rule.validate('bb'))

    def test_value_enumeration(self):
        rule = HXLSchemaRule(valueEnumeration=['aa', 'bb', 'cc'])
        self.assertTrue(rule.validate('bb'))
        self.assertFalse(rule.validate('BB'))
        self.assertFalse(rule.validate('dd'))

        rule.caseSensitive = False
        self.assertTrue(rule.validate('bb'))
        self.assertTrue(rule.validate('BB'))
        self.assertFalse(rule.validate('dd'))

# end
