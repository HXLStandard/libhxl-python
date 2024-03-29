"""
Unit tests for the hxl.datatypes module
David Megginson
April 2018

License: Public Domain
"""

import hxl.datatypes, unittest


class TestStrings(unittest.TestCase):

    def test_empty(self):
        self.assertTrue(hxl.datatypes.is_empty(None))
        self.assertTrue(hxl.datatypes.is_empty(''))
        self.assertTrue(hxl.datatypes.is_empty('  '))
        self.assertTrue(hxl.datatypes.is_empty(" \t\r\n    "))

    def test_not_empty(self):
        self.assertFalse(hxl.datatypes.is_empty(0))
        self.assertFalse(hxl.datatypes.is_empty('0'))
        self.assertFalse(hxl.datatypes.is_empty('   x    '))

    def test_normalise(self):
        self.assertEqual('', hxl.datatypes.normalise_string(None))
        self.assertEqual('', hxl.datatypes.normalise_string('    '))
        self.assertEqual('3.0', hxl.datatypes.normalise_string(3.0))
        self.assertEqual('foo', hxl.datatypes.normalise_string('  FoO  '))
        self.assertEqual('foo bar', hxl.datatypes.normalise_string("  FOO  \r\n bAr  "))

class TestNumbers(unittest.TestCase):

    def test_is_number(self):
        self.assertTrue(hxl.datatypes.is_number(1))
        self.assertTrue(hxl.datatypes.is_number(' 1 '))
        self.assertTrue(hxl.datatypes.is_number(1.1))
        self.assertTrue(hxl.datatypes.is_number(' 1.1 '))
        self.assertTrue(hxl.datatypes.is_number(-1))
        self.assertTrue(hxl.datatypes.is_number('-1'))
        self.assertTrue(hxl.datatypes.is_number('2.1e10'))

    def test_not_number(self):
        self.assertFalse(hxl.datatypes.is_number('1x'))

    def test_normalise(self):
        self.assertEqual(1, hxl.datatypes.normalise_number(1.0))
        self.assertEqual(1, hxl.datatypes.normalise_number('1.0'))
        self.assertEqual(1.1, hxl.datatypes.normalise_number(1.1))
        self.assertEqual(1.1, hxl.datatypes.normalise_number('1.1'))

    def test_normalise_exception(self):
        seen_exception = False
        try:
            hxl.datatypes.normalise_number('foo')
        except ValueError:
            seen_exception = True
        self.assertTrue(seen_exception)

class TestDates(unittest.TestCase):

    def test_is_iso_date(self):
        self.assertTrue(hxl.datatypes.is_date('2018'))
        self.assertTrue(hxl.datatypes.is_date('   2018  '))
        self.assertTrue(hxl.datatypes.is_date('2018W2'))
        self.assertTrue(hxl.datatypes.is_date('2018-03'))
        self.assertTrue(hxl.datatypes.is_date('2018-03-01'))

        # ISO edge cases
        self.assertFalse(hxl.datatypes.is_date('2018-04-31'))
        self.assertFalse(hxl.datatypes.is_date('2018-13-01'))
        self.assertFalse(hxl.datatypes.is_date('2018W54'))

    def test_iso_datetime(self):
        self.assertTrue(hxl.datatypes.is_date("2011-01-01T00:00:00.000Z"))
        self.assertEqual('2011-01-01', hxl.datatypes.normalise_date("2011-01-01T00:00:00.000Z"))

    def test_sql_datetime(self):
        self.assertTrue(hxl.datatypes.is_date('2017-12-01 00:00:00'))
        self.assertEqual('2017-12-01', hxl.datatypes.normalise_date('2017-12-01 00:00:00'))

    def test_rfc822_datetime(self):
        self.assertTrue(hxl.datatypes.is_date("30 May 2018 02:57:50 GMT"))
        self.assertTrue(hxl.datatypes.is_date("Thu, 30 May 2018 02:57:50 GMT"))
        self.assertEqual('2018-05-30', hxl.datatypes.normalise_date("Thu, 30 May 2018 02:57:50 GMT"))

    def test_normalise_date_dayfirst(self):
        self.assertEqual('2018-11-10', hxl.datatypes.normalise_date('10-11-18', dayfirst=True))
        self.assertEqual('2018-10-11', hxl.datatypes.normalise_date('10-11-18', dayfirst=False))

    def test_is_quarter(self):
        self.assertTrue(hxl.datatypes.is_date('2018Q2'))
        self.assertFalse(hxl.datatypes.is_date('2018Q5'))

    def test_is_non_iso_date(self):
        self.assertTrue(hxl.datatypes.is_date('Feb 2/17'))
        self.assertTrue(hxl.datatypes.is_date('Feb 2 17'))
        self.assertTrue(hxl.datatypes.is_date('Feb 2 2017'))
        self.assertTrue(hxl.datatypes.is_date('12 June 2017'))

    def test_not_date(self):
        self.assertFalse(hxl.datatypes.is_date('Feb Feb 2017'))
        self.assertFalse(hxl.datatypes.is_date('13.13.2017'))

    def test_normalise_iso_date(self):
        self.assertEqual('2008', hxl.datatypes.normalise_date('2008'))
        self.assertEqual('2008-01', hxl.datatypes.normalise_date('2008-01'))
        self.assertEqual('2008-01', hxl.datatypes.normalise_date('2008-1'))
        self.assertEqual('2008-01-01', hxl.datatypes.normalise_date('2008-01-01'))
        self.assertEqual('2008-01-01', hxl.datatypes.normalise_date('2008-1-1'))
        self.assertEqual('2008W01', hxl.datatypes.normalise_date('2008w1'))
        self.assertEqual('2008Q1', hxl.datatypes.normalise_date('2008q1'))

    def test_normalise_other_date(self):
        self.assertEqual('2008-01-20', hxl.datatypes.normalise_date('Jan 20, 2008'))
        self.assertEqual('2008-01-20', hxl.datatypes.normalise_date('01-20-2008'))
        self.assertEqual('2008-01-20', hxl.datatypes.normalise_date('20-01-2008'))
        self.assertEqual('2008-01', hxl.datatypes.normalise_date('Jan 2008'))

    def test_partial_dates(self):
        # Year alone is OK
        self.assertTrue(hxl.datatypes.normalise_date('2018'))
        
        # Month alone is OK (supply current year)
        self.assertTrue(hxl.datatypes.normalise_date('July'))

        # Day alone is not OK
        with self.assertRaises(ValueError):
            hxl.datatypes.normalise_date('30')
        
    def test_epoch_seconds(self):
        """ Seconds since epoch """
        self.assertEqual('2022-09-26', hxl.datatypes.normalise_date('1664212110'))

    def test_epoch_days(self):
        """ Days since epoch """
        self.assertEqual('2016-07-17', hxl.datatypes.normalise_date('17000'))


class TestFlatten(unittest.TestCase):

    def test_none(self):
        self.assertEqual('', hxl.datatypes.flatten(None))

    def test_number(self):
        self.assertEqual("3", hxl.datatypes.flatten(3))

    def test_string(self):
        self.assertEqual("xxx", hxl.datatypes.flatten("xxx"))

    def test_list(self):
        input = ['a', 'b', ['c', 'd'], 'e']
        output = '["a", "b", ["c", "d"], "e"]'
        self.assertEqual(output, hxl.datatypes.flatten(input))

    def test_non_json(self):
        input = ['a', 'b', ['c', 'd'], 'e']
        output = 'a | b | c | d | e'
        self.assertEqual(output, hxl.datatypes.flatten(input, use_json=False))

    def test_dict(self):
        input = {'a': 'b', 'c': ['d', 'e']}
        output = '{"a": "b", "c": ["d", "e"]}'
        self.assertEqual(output, hxl.datatypes.flatten(input))
