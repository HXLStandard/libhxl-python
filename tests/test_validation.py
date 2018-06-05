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
from hxl.validation import HXLValidationException, Schema, SchemaRule

from . import resolve_path


class TestTests(unittest.TestCase):
    """Test individual tests for a rule."""

    def test_required(self):
        def t():
            return hxl.validation.RequiredTest(min_occurs=1, max_occurs=2)

        # successful dataset tests
        self.assertTrue(t().validate_dataset(make_dataset(['#org', '#sector']), tag_pattern='#sector'))
        self.assertTrue(t().validate_dataset(make_dataset(['#org', '#sector', '#sector']), tag_pattern='#sector'))

        # failed dataset tests
        self.assertFalse(t().validate_dataset(make_dataset(['#org']), tag_pattern='#sector'))

        # successful row tests
        self.assertTrue(t().validate_row(make_row(['aaa', 'xxx'], ['#org', '#sector']), tag_pattern='#sector'))
        self.assertTrue(t().validate_row(make_row(['aaa', 'xxx', 'yyy'], ['#org', '#sector', '#sector']), tag_pattern='#sector'))

        # failed row tests
        self.assertFalse(t().validate_row(make_row(['xxx', ''], ['#org', '#sector']), tag_pattern='#sector'))
        self.assertFalse(t().validate_row(make_row(['xxx', 'yyy', 'zzz'], ['#sector', '#sector', '#sector']), tag_pattern='#sector'))

    def test_datatype(self):
        def t(datatype='text'):
            return hxl.validation.DatatypeTest(datatype)

        # check for bad datatype
        with self.assertRaises(hxl.HXLException):
            t('xxx')

        # successful tests (OK to leave row and column as None)
        self.assertTrue(t().validate_cell('xxx', None, None))
        self.assertTrue(t('number').validate_cell('20.1', None, None))
        self.assertTrue(t('url').validate_cell('http://example.org', None, None))
        self.assertTrue(t('email').validate_cell('nobody@example.org', None, None))
        self.assertTrue(t('phone').validate_cell('123-456-7890', None, None))
        self.assertTrue(t('date').validate_cell('2018-05-01', None, None))

        # failed tests
        self.assertFalse(t('number').validate_cell('xxx', None, None))
        self.assertFalse(t('url').validate_cell('/example.org', None, None))
        self.assertFalse(t('email').validate_cell('nobody@@example.org', None, None))
        self.assertFalse(t('phone').validate_cell('123-456-A890', None, None))
        self.assertFalse(t('date').validate_cell('2018-05-32', None, None))

    def test_range(self):
        def t(min_value=None, max_value=None):
            return hxl.validation.RangeTest(min_value, max_value)

        column = hxl.model.Column.parse('#x_test')
        date_column = hxl.model.Column.parse('#date')

        # dates
        self.assertTrue(t(min_value='2018-01-01').validate_cell('2018-01-01', None, date_column))
        self.assertTrue(t(min_value='2018-01-01').validate_cell('Jan-2/18', None, date_column))
        self.assertTrue(t(max_value='2018-01-01').validate_cell('2017-01-01', None, date_column))
        self.assertTrue(t(max_value='2018-01-01').validate_cell('Jan-2/17', None, date_column))
        self.assertTrue(t(min_value='2018-01-01', max_value='2018-02-01').validate_cell('3 January 2018', None, date_column))

        self.assertFalse(t(min_value='2018-01-01').validate_cell('2017-01-01', None, date_column))
        self.assertFalse(t(min_value='2018-01-01').validate_cell('Jan-2/17', None, date_column))
        self.assertFalse(t(max_value='2018-01-01').validate_cell('2019-01-01', None, date_column))
        self.assertFalse(t(max_value='2018-01-01').validate_cell('Jan-2/18', None, date_column))
        self.assertFalse(t(min_value='2018-01-01', max_value='2018-02-01').validate_cell('3 January 2019', None, date_column))

        # numbers
        self.assertTrue(t(min_value=200).validate_cell('200', None, column))
        self.assertTrue(t(min_value=200).validate_cell('1000', None, column))
        self.assertTrue(t(max_value=300).validate_cell('300', None, column))
        self.assertTrue(t(max_value=300).validate_cell('40', None, column))
        self.assertTrue(t(min_value=200, max_value=300).validate_cell('250', None, column))

        self.assertFalse(t(min_value=200).validate_cell('30', None, column))
        self.assertFalse(t(max_value=300).validate_cell('2000', None, column))
        self.assertFalse(t(min_value=200, max_value=300).validate_cell('301', None, column))

        # lexical comparison
        self.assertTrue(t(min_value='c').validate_cell('c', None, column))
        self.assertTrue(t(min_value='c').validate_cell('Ddd', None, column))
        self.assertTrue(t(min_value='c').validate_cell(' Ccc', None, column))
        self.assertTrue(t(max_value='e').validate_cell('e', None, column))
        self.assertTrue(t(max_value='e').validate_cell('DaD', None, column))
        self.assertTrue(t(max_value='e').validate_cell(' BaB', None, column))
        self.assertTrue(t(min_value='c', max_value='e').validate_cell('d', None, column))

        self.assertFalse(t(min_value='c').validate_cell('b', None, column))
        self.assertFalse(t(min_value='c').validate_cell('Bbb', None, column))
        self.assertFalse(t(min_value='c').validate_cell(' Aaa', None, column))
        self.assertFalse(t(max_value='e').validate_cell('ee', None, column))
        self.assertFalse(t(max_value='e').validate_cell('F', None, column))
        self.assertFalse(t(max_value='e').validate_cell(' EaE', None, column))
        self.assertFalse(t(min_value='c', max_value='e').validate_cell('ee', None, column))

    def test_whitespace(self):
        def t():
            return hxl.validation.WhitespaceTest()

        self.assertTrue(t().validate_cell('xxx', None, None))
        self.assertTrue(t().validate_cell('xxx yyy', None, None))

        self.assertFalse(t().validate_cell(' xxx', None, None)) # leading space not allowed
        self.assertFalse(t().validate_cell('xxx ', None, None)) # trailing space not allowed
        self.assertFalse(t().validate_cell('xxx  yyy', None, None)) # multiple internal spaces not allowed
        self.assertFalse(t().validate_cell("xxx\tyyy", None, None)) # tabs not allowed

        self.assertTrue(t().validate_cell(3, None, None)) # Exception if it's not a string?

    def test_regex(self):
        def t(pattern):
            return hxl.validation.RegexTest(pattern)

        self.assertTrue(t('.').validate_cell('xxx', None, None))
        self.assertTrue(t('c').validate_cell('abcd', None, None))
        self.assertTrue(t('^a').validate_cell('abcd', None, None))
        self.assertTrue(t('d$').validate_cell('abcd', None, None))

        self.assertFalse(t('e').validate_cell('abcd', None, None))
        self.assertFalse(t('^b').validate_cell('abcd', None, None))
        self.assertFalse(t('c$').validate_cell('abcd', None, None))

    def test_unique_value(self):
        t = hxl.validation.UniqueValueTest()
        t.start() # to set up context
        self.assertTrue(t.validate_cell('a', None, None))
        self.assertFalse(t.validate_cell('a', None, None))
        self.assertFalse(t.validate_cell('  A  ', None, None))
        self.assertTrue(t.validate_cell('b', None, None))
        self.assertTrue(t.end())

    def test_unique_row(self):
        COLUMNS = ['#adm1', '#sector', '#org']
        t = hxl.validation.UniqueRowTest('org,sector')
        t.start() # to set up context
        self.assertTrue(t.validate_row(make_row(['Coast', 'WASH', 'Org A'], COLUMNS)))
        self.assertFalse(t.validate_row(make_row(['Coast', 'WASH', 'Org A'], COLUMNS))) # pure repeat
        self.assertFalse(t.validate_row(make_row(['Plains', 'WASH', 'Org A'], COLUMNS))) # adm1 is not in the key
        self.assertFalse(t.validate_row(make_row(['Coast', '  wash', 'Org A'], COLUMNS))) # space-/case-insensitive
        self.assertTrue(t.validate_row(make_row(['Coast', 'WASH', 'Org B'], COLUMNS))) # org is in the key
        self.assertTrue(t.end())

    def test_enumeration(self):
        def t(allowed_values=['aaa', 'BBB', 'ccc'], case_sensitive=False):
            return hxl.validation.EnumerationTest(allowed_values, case_sensitive)

        self.assertTrue(t().validate_cell('aaa', None, None))
        self.assertTrue(t().validate_cell('bBb', None, None)) # case-insensitive
        self.assertTrue(t(case_sensitive=True).validate_cell('BBB', None, None)) # case-sensitive, but OK
        self.assertTrue(t().validate_cell('   ccc   ', None, None)) # whitespace doesn't matter

        self.assertFalse(t().validate_cell('ddd', None, None))
        self.assertFalse(t(case_sensitive=True).validate_cell('ddd', None, None))
        self.assertFalse(t(case_sensitive=True).validate_cell('bBb', None, None)) # case-sensitive

    def test_enumeration_suggested_values(self):
        
        def make_callback(suggested_value):
            def callback(e):
                self.assertEqual(suggested_value, e.suggested_value)
            return callback

        t = hxl.validation.EnumerationTest(allowed_values=['Aaaa', 'bbbb', 'cccc'])
        t.callback = make_callback('Aaaa')
        self.assertFalse(t.validate_cell('aaa', None, None))
        t.callback = make_callback('bbbb')
        self.assertFalse(t.validate_cell('BBB', None, None))
        t.callback = make_callback(None) # no close matches
        self.assertFalse(t.validate_cell('xxx', None, None))

        t = hxl.validation.EnumerationTest(allowed_values=['Aaa', 'bbb', 'ccc'], case_sensitive=True)
        t.callback = make_callback('Aaa')
        self.assertFalse(t.validate_cell('aaa', None, None))

    def test_correlation(self):

        COLUMNS = ['#adm1+name', '#adm1+code']

        def callback(e):
            # expect 'xxx' as the suggested value
            self.assertEqual('xxx', e.suggested_value)

        t = hxl.validation.CorrelationTest('#adm1+code')
        t.callback = callback

        t.start()
        self.assertTrue(t.validate_row(make_row(['xxx', 'yyy'], COLUMNS), tag_pattern='#adm1+name'))
        self.assertTrue(t.validate_row(make_row(['aaa', 'bbb'], COLUMNS), tag_pattern='#adm1+name'))
        self.assertTrue(t.validate_row(make_row(['aaa', 'bbb'], COLUMNS), tag_pattern='#adm1+name'))
        self.assertTrue(t.validate_row(make_row(['xxx', 'yyy'], COLUMNS), tag_pattern='#adm1+name'))
        # bad row:
        self.assertTrue(t.validate_row(make_row(['zzz', 'yyy'], COLUMNS), tag_pattern='#adm1+name'))
        self.assertTrue(t.validate_row(make_row(['xxx', 'yyy'], COLUMNS), tag_pattern='#adm1+name'))
        self.assertFalse(t.end())

    def test_spelling_case_sensitive(self):
        COLUMN = hxl.model.Column.parse('#xxx')

        errors_seen = 0

        def callback(e):
            nonlocal errors_seen
            errors_seen += 1
            self.assertEqual('xxxx', e.suggested_value)

        t = hxl.validation.SpellingTest(case_sensitive=True)
        t.callback = callback

        t.start()
        for i in range(0, 100):
            t.validate_cell('xxxx', None, COLUMN)
            t.validate_cell('yyyy', None, COLUMN)
        t.validate_cell('xxx', None, COLUMN) # expect an error
        t.validate_cell('Xxxx', None, COLUMN) # expect an error
        self.assertFalse(t.end()) # errors detected at end of parse

        self.assertEqual(2, errors_seen)

    def test_spelling_case_insensitive(self):
        COLUMN = hxl.model.Column.parse('#xxx')

        errors_seen = 0

        def callback(e):
            nonlocal errors_seen
            errors_seen += 1
            self.assertEqual('xxxx', e.suggested_value)

        t = hxl.validation.SpellingTest(case_sensitive=False)
        t.callback = callback

        t.start()
        for i in range(0, 100):
            t.validate_cell('xxxx', None, COLUMN)
            t.validate_cell('yyyy', None, COLUMN)
        t.validate_cell('xxx', None, COLUMN) # expect an error
        t.validate_cell('Xxxx', None, COLUMN) # *not* an error (case-insensitive)
        self.assertFalse(t.end()) # errors detected at end of parse

        self.assertEqual(1, errors_seen)

    def test_consistent_datatypes(self):
        COLUMN = hxl.model.Column.parse('#x_test')
        GOOD_VALUES = ['12', '34', '56', '78', '90']
        BAD_VALUES = ['xxx', 'yyy']

        seen_callback = False
        
        def callback(e):
            nonlocal seen_callback
            seen_callback = True
            self.assertTrue(e.value in BAD_VALUES)

        t = hxl.validation.ConsistentDatatypesTest()
        t.callback = callback

        # consistent numbers
        t.start()
        for value in GOOD_VALUES + BAD_VALUES:
            t.scan_cell(value, None, COLUMN)
        t.end_scan()
        for value in GOOD_VALUES:
            self.assertTrue(t.validate_cell(value, None, COLUMN))
        for value in BAD_VALUES:
            self.assertFalse(t.validate_cell(value, None, COLUMN))
        self.assertTrue(seen_callback)
        
    def test_outliers(self):
        COLUMN = hxl.model.Column.parse('#x_test')
        GOOD_VALUES = ['9000', '10000', '11000']
        BAD_VALUES = ['5000000', '1']

        seen_callback = False
        
        def callback(e):
            nonlocal seen_callback
            seen_callback = True
            self.assertTrue(e.value in BAD_VALUES)

        t = hxl.validation.NumericOutlierTest()
        t.callback = callback

        # consistent numbers
        t.start()
        for i in range(1, 1000):
            for value in GOOD_VALUES:
                t.scan_cell(value, None, COLUMN)
        for value in BAD_VALUES:
            t.scan_cell(value, None, COLUMN)
        t.end_scan()
        for value in GOOD_VALUES:
            self.assertTrue(t.validate_cell(value, None, COLUMN))
        for value in BAD_VALUES:
            self.assertFalse(t.validate_cell(value, None, COLUMN))
        self.assertTrue(seen_callback)


class TestRule(unittest.TestCase):
    """Test the hxl.validation.SchemaRule class.
    Most of the tests just ensure that the AbstractRuleTest objects
    get applied properly.
    """

    COLUMNS = ['#x_test']

    def setUp(self):
        self.rule = SchemaRule('#x_test', callback=lambda error: self.errors.append(error), severity="warning")

    def test_datatype(self):
        self.rule.tests = [hxl.validation.DatatypeTest('number')]
        self._try_rule(10)
        self._try_rule(' -10.1 ');
        self._try_rule('ten', 1)

    def test_whitespace(self):
        self.rule.tests = [hxl.validation.WhitespaceTest()]
        self._try_rule('xxx', 0)
        self._try_rule(' xxx', 1)

    def test_range(self):
        self.rule.tests = [hxl.validation.RangeTest(3.5, 4.5)]
        self._try_rule(4.0)
        self._try_rule('3.49', 1)

    def test_regex(self):
        self.rule.tests = [hxl.validation.RegexTest('^a+b$')]
        self._try_rule('ab', 0)
        self._try_rule('aab', 0)
        self._try_rule('bb', 1)

    def test_value_enumeration(self):
        self.rule.tests = [hxl.validation.EnumerationTest(allowed_values=['aa', 'bb', 'cc'], case_sensitive=True)]
        self.rule.start()
        self._try_rule('bb')
        self._try_rule('BB', 1)
        self._try_rule('dd', 1)

        self.rule.tests = [hxl.validation.EnumerationTest(allowed_values=['aa', 'bb', 'cc'], case_sensitive=False)]
        self.rule.start()
        self._try_rule('bb')
        self._try_rule('BB')
        self._try_rule('dd', 1)

    def test_row_restrictions(self):
        """Check tests at the rule level"""

        test = hxl.validation.RequiredTest(self.rule.tag_pattern)
        self.rule.tests.append(test)
        
        row = make_row(['WASH', '', ''], ['#x_test', '#subsector', '#x_test'])

        test.min_occurs = 1
        self._try_rule(row)

        test.min_occurs = 2
        self._try_rule(row, 1)

        test.min_occurs = None
        test.max_occurs = 1
        self._try_rule(row)

        test.max_occurs = 0
        self._try_rule(row, 1)


    def _try_rule(self, value, errors_expected = 0):
        """Helper: Validate a single value with a SchemaRule"""
        errors = []

        def callback(e):
            errors.append(e)

        self.rule.callback = callback
        self.rule.start() # make sure the rule is initialised
            
        if isinstance(value, Row):
            row = value
        else:
            row = make_row([value], ['#x_test'])

        if errors_expected == 0:
            self.assertTrue(self.rule.validate_row(row))
        else:
            self.assertFalse(self.rule.validate_row(row))

        self.assertEqual(len(errors), errors_expected)

        
class TestValidateColumns(unittest.TestCase):
    """Test validation at the dataset level"""

    def test_required(self):
        """Test if a column is present to potentially satisfy #valid_required"""
        SCHEMA_VALUES = [
            ['#valid_tag', '#valid_required'],
            ['#adm1+code', 'true']
        ]
        self.assertColumnErrors(['#org', '#adm1+code'], 0, schema_values=SCHEMA_VALUES)
        self.assertColumnErrors(['#org', '#adm2+code'], 1, schema_values=SCHEMA_VALUES)
        self.assertColumnErrors(['#org', '#adm1+name'], 1, schema_values=SCHEMA_VALUES)

    def test_min_occurs(self):
        """Test if enough columns are present to potentially satisfy #valid_required+min"""
        SCHEMA_VALUES = [
            ['#valid_tag', '#valid_required+min'],
            ['#org', '2']
        ]
        self.assertColumnErrors(['#org', '#adm1', '#org'], 0, schema_values=SCHEMA_VALUES)
        self.assertColumnErrors(['#org', '#adm1', '#org', '#org'], 0, schema_values=SCHEMA_VALUES)
        self.assertColumnErrors(['#org', '#adm1'], 1, schema_values=SCHEMA_VALUES)

    def test_bad_value_url(self):
        """Test for an error with an unresolvable #valid_value+url"""
        SCHEMA = [
            ["#valid_tag", "#valid_value+url"],
            ["#adm1+code", "http://example.org/non-existant-link.csv"]
        ]
        self.assertColumnErrors(['#adm1+code'], 1, SCHEMA)

    def assertColumnErrors(self, column_values, errors_expected, schema_values):
        """Set up a list of HXL columns and count the errors"""
        errors = []

        def callback(error):
            errors.append(error)

        schema = hxl.schema(schema_values, callback=callback)
        dataset = make_dataset(column_values)

        schema.start()
        if errors_expected == 0:
            self.assertTrue(schema.validate_dataset(dataset))
        else:
            self.assertFalse(schema.validate_dataset(dataset))
        self.assertEqual(len(errors), errors_expected)


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
        SCHEMA_VALUES = [
            ['#valid_tag', '#valid_datatype'],
            ['#date', 'date'],
        ]
        self.assertRowErrors(['2017-01-01'], 0, columns=COLUMNS, schema_values=SCHEMA_VALUES)
        self.assertRowErrors(['1/1/17'], 0, columns=COLUMNS, schema_values=SCHEMA_VALUES)
        self.assertRowErrors(['13/13/17'], 1, columns=COLUMNS, schema_values=SCHEMA_VALUES)

    def test_url(self):
        COLUMNS = ['#meta+url']
        SCHEMA = [
            ['#valid_tag', '#valid_datatype'],
            ['#meta+url', 'url'],
        ]
        self.assertRowErrors(['http://example.org'], 0, columns=COLUMNS, schema_values=SCHEMA)
        self.assertRowErrors(['example.org'], 1, columns=COLUMNS, schema_values=SCHEMA)

    def test_email(self):
        COLUMNS = ['#contact+email']
        SCHEMA = [
            ['#valid_tag', '#valid_datatype'],
            ['#contact+email', 'email'],
        ]
        self.assertRowErrors(['nobody@example.org'], 0, columns=COLUMNS, schema_values=SCHEMA)
        self.assertRowErrors(['nobody@@example.org'], 1, columns=COLUMNS, schema_values=SCHEMA)

    def assertRowErrors(self, row_values, errors_expected, schema_values=None, columns=None):
        """Set up a HXL row and count the errors in it"""
        errors = []

        def callback(error):
            errors.append(error)

        if schema_values is None:
            schema = hxl.schema(hxl.data(self.DEFAULT_SCHEMA), callback=callback)
        else:
            schema = hxl.schema(hxl.data(schema_values), callback=callback)

        if columns is None:
            columns = self.DEFAULT_COLUMNS

        row = Row(
            values=row_values,
            columns=[Column.parse(tag) for tag in columns]
        )

        schema.start()

        if errors_expected == 0:
            self.assertTrue(schema.validate_row(row))
        else:
            self.assertFalse(schema.validate_row(row))
        self.assertEqual(len(errors), errors_expected)


class TestValidateDataset(unittest.TestCase):
    """Test dataset-wide validation"""

    SCHEMA = [
        ['#valid_tag', '#valid_unique'],
        ['#meta+id', 'true'],
    ]

    def test_default_schema(self):
        """Test the built-in schema"""
        DATASET = [
            ['#affected', '#date'],
            ['100', '2018-01-01'],   # OK
            ['200', 'xxx'],          # bad date
            ['xxx', '2018-03-01'],   # bad number
            ['100', ' 2018-04-01 '], # extra whitespace
        ]
        errors_seen = 0
        def callback(e):
            nonlocal errors_seen
            errors_seen += 1
        self.assertFalse(hxl.schema(callback=callback).validate(hxl.data(DATASET)))
        self.assertEqual(3, errors_seen)

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

    def test_outliers(self):
        BAD_VALUES = ['1', '1000000']

        raw_data = [
            ['#affected'],
            ['1'],
            ['1000000']
        ]

        for i in range(0, 10):
            raw_data += [
                ['100'],
                ['200'],
                ['800']
            ]
            
        seen_callback = False

        def callback(e):
            nonlocal seen_callback
            seen_callback = True
            self.assertTrue(e.value in BAD_VALUES)

        schema = hxl.schema([
            ['#valid_tag', '#valid_value+outliers'],
            ['#affected', 'true']
        ], callback=callback)

        data = hxl.data(raw_data)

        self.assertFalse(schema.validate(data))
        self.assertTrue(seen_callback)

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

    def test_double_correlation(self):
        """Test correlation when more than one column has same tagspec"""
        SCHEMA = [
            ['#valid_tag', '#description', '#valid_correlation', '#valid_value+list'],
            ['#adm1+code', 'xxxxx', '#adm1+name', 'X001|X002']
        ]
        DATASET = [
            ['#adm1+name', '#adm1+code', '#adm1+code'],
            ['Coast', 'X001', 'X001'],
            ['Plains', 'X002', 'X02'],
            ['Plains', 'X002', 'X02'],
            ['Plains', 'X002', 'X02'],
            ['Plains', 'X002', 'X02'],
        ]
        report = hxl.validate(DATASET, SCHEMA)
        self.assertEquals(4, report['stats']['total'])

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

    def test_different_indicator_datatypes(self):
        """One rule, but three different indicators with different tagspecs and datatypes"""
        SCHEMA = [
            ['#valid_tag', '#valid_datatype+consistent'],
            ['#indicator', 'true']
        ]
        GOOD_DATA = [
            ['#indicator+xxx', '#indicator+yyy', '#indicator+zzz'],
            ['100', 'aaa', '100'],
            ['200', '', '200'],
            ['300', '', '300'],
            ['400', '', '400'],
            ['500', '', '500']
        ]
        BAD_DATA = [
            ['#indicator+xxx', '#indicator+yyy', '#indicator+zzz'],
            ['100', 'aaa', '100'],
            ['200', '2', '200'],
            ['300', '3', '300'],
            ['400', '4', '400'],
            ['500', '5', '500']
        ]

        report = hxl.validate(GOOD_DATA, SCHEMA)
        self.assertTrue(report['is_valid'])
        self.assertEqual(0, report['stats']['total'])

        report = hxl.validate(BAD_DATA, SCHEMA)
        self.assertFalse(report['is_valid'])
        self.assertEqual(1, report['stats']['total'])

    def test_spellings(self):
        SCHEMA = [
            ['#valid_tag', '#valid_value+spelling'],
            ['#indicator', 'true']
        ]
        DATA = [['#indicator']] + [['aaaaa'] for n in range(0,50)] + [['bbbbb'] for n in range(0,50)] + [['aabaa']]
        report = hxl.validate(DATA, SCHEMA)
        self.assertFalse(report['is_valid'])
        self.assertEqual(1, report['stats']['total'])
        
    def test_spellings_multiple(self):
        SCHEMA = [
            ['#valid_tag', '#valid_value+spelling'],
            ['#indicator', 'true']
        ]
        DATA = [['#indicator+xxx', '#indicator+yyy']] + [['aaaaa', 'aaaab'] for n in range(0,50)] + [['aaaab', 'aaaaa']]
        report = hxl.validate(DATA, SCHEMA)
        self.assertFalse(report['is_valid'])
        self.assertEqual(2, report['stats']['total'])
        
    def assertDatasetErrors(self, dataset, errors_expected, schema=None):
        errors = []

        def callback(error):
            errors.append(error)

        if schema is None:
            schema = self.SCHEMA
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


class TestJSONSchema(unittest.TestCase):

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

class TestJSONReport(unittest.TestCase):

    DATA = [
        ['#xxx', '#yyy', '#zzz'],
        ['100', 'abc', '2018-01-01'],
        ['200', '300', '2018-01-02'],
    ]

    def test_top_level(self):
        """Use the package-level alias"""
        report = hxl.validate(self.DATA)

    def test_default(self):
        """Test successful validation against a default schema"""
        report = hxl.validation.validate(self.DATA)
        self.assertTrue(report['is_valid'])
        for severity in ('total', 'info', 'warning', 'error'):
            self.assertEqual(0, report['stats'][severity])

    def test_errors(self):
        """Test unsuccessful validation against an explicit schema"""
        SCHEMA = [
            ['#valid_tag', '#valid_datatype'],
            ['#yyy', 'number'],
            ['#xxx', 'url'],
        ]
        report = hxl.validation.validate(self.DATA, SCHEMA)
        self.assertFalse(report['is_valid'])
        self.assertEqual(3, report['stats']['total'])
        self.assertEqual(2, len(report['issues']))

#
# Functions
#

def make_dataset(hashtags, values=[]):
    return hxl.data([hashtags] + values)
    

def make_row(values, hashtags):
    columns = [hxl.model.Column.parse(hashtag) for hashtag in hashtags]
    return hxl.model.Row(columns, values=values)

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
