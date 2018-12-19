"""Unit tests for hxl.equation.functions
"""

import unittest
import hxl.equation.functions

class TestOperators(unittest.TestCase):

    TAGS = ["#org", "#adm1", "#affected+f+children", "#affected+m+children", "#affected+f+adults", "#affected+m+adults"]
    DATA = ["Org A", "Coast Region", "100", "200", "300", "400"]

    def setUp(self):
        columns = [hxl.model.Column.parse(tag) for tag in self.TAGS]
        self.row = hxl.model.Row(columns=columns, values=self.DATA)

    def test_add(self):

        # integers
        result = hxl.equation.functions.add(self.row, ['2', '3'])
        self.assertEqual(5, result)

        # float and integer
        result = hxl.equation.functions.add(self.row, ['2', '3.5'])
        self.assertEqual(5.5, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = hxl.equation.functions.add(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+f', '#affected+m'])
        )
        self.assertEqual(300, result)

        # tag pattern and integer
        result = hxl.equation.functions.add(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '150'
        ])
        self.assertEqual(250, result)

        # ignore strings
        result = hxl.equation.functions.add(self.row, [
            hxl.model.TagPattern.parse('#org'),
            '150'
        ])
        self.assertEqual(150, result)

    def test_subtract(self):

        # integers
        result = hxl.equation.functions.subtract(self.row, ['2', '3'])
        self.assertEqual(-1, result)

        # float and integer
        result = hxl.equation.functions.subtract(self.row, ['4', '3.5'])
        self.assertEqual(0.5, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = hxl.equation.functions.subtract(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+m', '#affected+f'])
        )
        self.assertEqual(100, result)

        # tag pattern and integer
        result = hxl.equation.functions.subtract(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '50'
        ])
        self.assertEqual(50, result)

    def test_multiply(self):

        # integers
        result = hxl.equation.functions.multiply(self.row, ['2', '3'])
        self.assertEqual(6, result)

        # float and integer
        result = hxl.equation.functions.multiply(self.row, ['4', '3.5'])
        self.assertEqual(14, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = hxl.equation.functions.multiply(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+m', '#affected+f'])
        )
        self.assertEqual(20000, result)

        # tag pattern and integer
        result = hxl.equation.functions.multiply(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '50'
        ])
        self.assertEqual(5000, result)

    def test_divide(self):

        # integers
        result = hxl.equation.functions.divide(self.row, ['4', '2'])
        self.assertEqual(2, result)

        # float and integer
        result = hxl.equation.functions.divide(self.row, ['6', '1.5'])
        self.assertEqual(4, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = hxl.equation.functions.divide(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+m', '#affected+f'])
        )
        self.assertEqual(2, result)

        # tag pattern and integer
        result = hxl.equation.functions.divide(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '50'
        ])
        self.assertEqual(2, result)

        # avoid DIV0
        result = hxl.equation.functions.divide(self.row, ['100', '0'])
        self.assertEqual(100, result)

        # ignore strings
        result = hxl.equation.functions.divide(self.row, [
            '150',
            hxl.model.TagPattern.parse('#org')
        ])
        self.assertEqual(150, result)

    def test_modulo(self):

        # integers
        result = hxl.equation.functions.modulo(self.row, ['4', '2'])
        self.assertEqual(0, result)

        # float and integer
        result = hxl.equation.functions.modulo(self.row, ['5', '1.5'])
        self.assertEqual(0.5, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = hxl.equation.functions.modulo(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+adults', '#affected+m'])
        )
        self.assertEqual(100, result) # 300 % 200

        # tag pattern and integer
        result = hxl.equation.functions.modulo(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '70'
        ])
        self.assertEqual(30, result) # 100 % 70

        # avoid DIV0
        result = hxl.equation.functions.modulo(self.row, ['100', '0'])
        self.assertEqual(100, result) # 100 % 0 - ignore the 0

        # ignore strings
        result = hxl.equation.functions.modulo(self.row, [
            '150',
            hxl.model.TagPattern.parse('#org')
        ])
        self.assertEqual(150, result) # 150 % "Org A" - ignore the string

    def test_sum(self):
        
        # should take all matches for each tag pattern
        result = hxl.equation.functions.sum(
            self.row,
            [hxl.model.TagPattern.parse('#affected'), '100']
        )
        self.assertEqual(1100, result)

    def test_embedded(self):

        result = hxl.equation.functions.multiply(self.row, [
            [hxl.equation.functions.add, ['1', '2']],
            '3'
        ])
        self.assertEqual(9, result)
