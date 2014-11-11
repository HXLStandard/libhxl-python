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
        self.try_rule('')
        self.try_rule(10)
        self.try_rule('hello, world')

    def test_type_text(self):
        self.rule.dataType = HXLSchemaRule.TYPE_TEXT
        self.try_rule('')
        self.try_rule(10)
        self.try_rule('hello, world')

    def test_type_num(self):
        self.rule.dataType = HXLSchemaRule.TYPE_NUMBER
        self.try_rule(10)
        self.try_rule(' -10.1 ');
        self.try_rule('ten', 1)

    def test_type_url(self):
        self.rule.dataType = HXLSchemaRule.TYPE_URL
        self.try_rule('http://www.example.org')
        self.try_rule('hello, world', 1)

    def test_type_email(self):
        self.rule.dataType = HXLSchemaRule.TYPE_EMAIL;
        self.try_rule('somebody@example.org')
        self.try_rule('hello, world', 1)

    def test_type_phone(self):
        self.rule.dataType = HXLSchemaRule.TYPE_PHONE
        self.try_rule('+1-613-555-1111 x1234')
        self.try_rule('(613) 555-1111')
        self.try_rule('123', 1)
        self.try_rule('123456789abc', 1)

    def test_value_range(self):
        self.rule.minValue = 3.5
        self.rule.maxValue = 4.5
        self.try_rule(4.0)
        self.try_rule('4')
        self.try_rule('3.49', 1)
        self.try_rule(5.0, 1)

    def test_value_pattern(self):
        self.rule.valuePattern = '^a+b$'
        self.try_rule('ab', 0)
        self.try_rule('aab', 0)
        self.try_rule('bb', 1)

    def test_value_enumeration(self):
        self.rule.valueEnumeration=['aa', 'bb', 'cc']

        self.rule.caseSensitive = True
        self.try_rule('bb')
        self.try_rule('BB', 1)
        self.try_rule('dd', 1)

        self.rule.caseSensitive = False
        self.try_rule('bb')
        self.try_rule('BB')
        self.try_rule('dd', 1)

    def test_row_restrictions(self):
        row = HXLRow(columns=[HXLColumn(hxlTag='#x_test'), HXLColumn(hxlTag='#subsector'), HXLColumn(hxlTag='#x_test')]);
        row.values = ['WASH', '', ''];

        self.rule.minOccur = 1
        self.try_rule(row)

        self.rule.minOccur = 2
        self.try_rule(row, 1)

        self.rule.minOccur = None

        self.rule.maxOccur = 1
        self.try_rule(row)

        self.rule.maxOccur = 0
        self.try_rule(row, 1)

    def try_rule(self, value, errors_expected = 0):
        """
        Validate a single value with a HXLSchemaRule
        """
        if isinstance(value, HXLRow):
            result = self.rule.validateRow(value)
        else:
            result = self.rule.validate(value)
        if errors_expected == 0:
            self.assertTrue(result)
        else:
            self.assertFalse(result)
        self.assertEqual(len(self.errors), errors_expected)
        self.errors = [] # clear errors for the next run

# end
