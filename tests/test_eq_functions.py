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
