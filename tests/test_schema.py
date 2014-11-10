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
        pass

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
        pass

    def test_type_none(self):
        rule = HXLSchemaRule('#sector',)
        self.assertTrue(rule.validate(''))
        self.assertTrue(rule.validate(10))
        self.assertTrue(rule.validate('hello, world'))

    def test_type_text(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_TEXT)
        self.assertTrue(rule.validate(''))
        self.assertTrue(rule.validate(10))
        self.assertTrue(rule.validate('hello, world'))

    def test_type_num(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_NUMBER)
        self.assertTrue(rule.validate(10))
        self.assertTrue(rule.validate(' -10.1  '))
        self.assertFalse(rule.validate('ten'))

    def test_type_url(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_URL)
        self.assertTrue(rule.validate('http://www.example.org'))
        self.assertFalse(rule.validate('hello, world'))

    def test_type_email(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_EMAIL)
        self.assertTrue(rule.validate('somebody@example.org'))
        self.assertFalse(rule.validate('hello, world'))

    def test_type_phone(self):
        rule = HXLSchemaRule('#sector',dataType=HXLSchemaRule.TYPE_PHONE)
        self.assertTrue(rule.validate('+1-613-555-1111 x1234'))
        self.assertTrue(rule.validate('(613) 555-1111'))
        self.assertFalse(rule.validate('123'))
        self.assertFalse(rule.validate('123456789abc'))

    def test_value_range(self):
        rule = HXLSchemaRule('#sector',minValue=3.5, maxValue=4.5)
        self.assertTrue(rule.validate(4.0))
        self.assertTrue(rule.validate('4'))
        self.assertFalse(rule.validate('3.49'))
        self.assertFalse(rule.validate('5.0'))

    def test_value_pattern(self):
        rule = HXLSchemaRule('#sector',valuePattern='^a+b$')
        self.assertTrue(rule.validate('ab'))
        self.assertTrue(rule.validate('aab'))
        self.assertFalse(rule.validate('bb'))

    def test_value_enumeration(self):
        rule = HXLSchemaRule('#sector',valueEnumeration=['aa', 'bb', 'cc'])
        self.assertTrue(rule.validate('bb'))
        self.assertFalse(rule.validate('BB'))
        self.assertFalse(rule.validate('dd'))

        rule.caseSensitive = False
        self.assertTrue(rule.validate('bb'))
        self.assertTrue(rule.validate('BB'))
        self.assertFalse(rule.validate('dd'))

    def test_callback(self):
        errors = []
        def error_callback(error):
            errors.append(error)
        rule = HXLSchemaRule('#sector', dataType=HXLSchemaRule.TYPE_NUMBER, callback=error_callback)
        self.assertFalse(rule.validate('abc'))
        self.assertEqual(len(errors), 1)

    def test_row_restrictions(self):
        row = HXLRow([HXLColumn(hxlTag='#sector'), HXLColumn(hxlTag='#subsector'), HXLColumn(hxlTag='#sector')]);
        row.values = ['WASH', '', ''];

        rule = HXLSchemaRule('#sector',minOccur=1)
        self.assertTrue(rule.validateRow(row))

        rule = HXLSchemaRule('#sector',minOccur=2)
        self.assertFalse(rule.validateRow(row))

        rule = HXLSchemaRule('#sector',maxOccur=1)
        self.assertTrue(rule.validateRow(row))

        rule = HXLSchemaRule('#sector',maxOccur=0)
        self.assertFalse(rule.validateRow(row))

# end
