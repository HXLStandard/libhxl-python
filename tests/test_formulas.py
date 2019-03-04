"""Unit tests for hxl.formulas.functions
"""

import unittest
import hxl.model
import hxl.formulas.functions as f, hxl.formulas.parser as p, hxl.formulas.lexer as l, hxl.formulas.eval as e


class TestFunctions(unittest.TestCase):
    """Test the hxl.formulas.functions class"""

    TAGS = ["#org", "#adm1", "#affected+f+children", "#affected+m+children", "#affected+f+adults", "#affected+m+adults"]
    DATA = ["Org A", "Coast Region", "100", "200", "300", "400"]

    def setUp(self):
        columns = [hxl.model.Column.parse(tag) for tag in self.TAGS]
        self.row = hxl.model.Row(columns=columns, values=self.DATA)

    def test_add(self):

        # integers
        result = f.add(self.row, ['2', '3'])
        self.assertEqual(5, result)

        # float and integer
        result = f.add(self.row, ['2', '3.5'])
        self.assertEqual(5.5, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = f.add(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+f', '#affected+m'])
        )
        self.assertEqual(300, result)

        # tag pattern and integer
        result = f.add(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '150'
        ])
        self.assertEqual(250, result)

        # ignore strings
        result = f.add(self.row, [
            hxl.model.TagPattern.parse('#org'),
            '150'
        ])
        self.assertEqual(150, result)

    def test_subtract(self):

        # integers
        result = f.subtract(self.row, ['2', '3'])
        self.assertEqual(-1, result)

        # float and integer
        result = f.subtract(self.row, ['4', '3.5'])
        self.assertEqual(0.5, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = f.subtract(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+m', '#affected+f'])
        )
        self.assertEqual(100, result)

        # tag pattern and integer
        result = f.subtract(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '50'
        ])
        self.assertEqual(50, result)

    def test_multiply(self):

        # integers
        result = f.multiply(self.row, ['2', '3'])
        self.assertEqual(6, result)

        # float and integer
        result = f.multiply(self.row, ['4', '3.5'])
        self.assertEqual(14, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = f.multiply(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+m', '#affected+f'])
        )
        self.assertEqual(20000, result)

        # tag pattern and integer
        result = f.multiply(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '50'
        ])
        self.assertEqual(5000, result)

    def test_divide(self):

        # integers
        result = f.divide(self.row, ['4', '2'])
        self.assertEqual(2, result)

        # float and integer
        result = f.divide(self.row, ['6', '1.5'])
        self.assertEqual(4, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = f.divide(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+m', '#affected+f'])
        )
        self.assertEqual(2, result)

        # tag pattern and integer
        result = f.divide(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '50'
        ])
        self.assertEqual(2, result)

        # avoid DIV0
        result = f.divide(self.row, ['100', '0'])
        self.assertEqual('NaN', result)

        # ignore strings
        result = f.divide(self.row, [
            '150',
            hxl.model.TagPattern.parse('#org')
        ])
        self.assertEqual('NaN', result)

    def test_modulo(self):

        # integers
        result = f.modulo(self.row, ['4', '2'])
        self.assertEqual(0, result)

        # float and integer
        result = f.modulo(self.row, ['5', '1.5'])
        self.assertEqual(0.5, result)

        # two tag patterns
        # should take only first match for each tag pattern
        result = f.modulo(
            self.row,
            map(hxl.model.TagPattern.parse, ['#affected+adults', '#affected+m'])
        )
        self.assertEqual(100, result) # 300 % 200

        # tag pattern and integer
        result = f.modulo(self.row, [
            hxl.model.TagPattern.parse('#affected+f'),
            '70'
        ])
        self.assertEqual(30, result) # 100 % 70

        # avoid DIV0
        result = f.modulo(self.row, ['100', '0'])
        self.assertEqual(100, result) # 100 % 0 - ignore the 0

        # ignore strings
        result = f.modulo(self.row, [
            '150',
            hxl.model.TagPattern.parse('#org')
        ])
        self.assertEqual(150, result) # 150 % "Org A" - ignore the string

    def test_sum(self):
        
        # should take all matches for each tag pattern
        result = f.FUNCTIONS['sum'](
            self.row,
            [hxl.model.TagPattern.parse('#affected'), '100'],
            True
        )
        self.assertEqual(1100, result)

    def test_product(self):
        
        # should take all matches for each tag pattern
        result = f.FUNCTIONS['product'](
            self.row,
            [hxl.model.TagPattern.parse('#affected'), '100'],
            True
        )
        self.assertEqual(240000000000, result)

    def test_min(self):
        result = f.FUNCTIONS['min'](
            self.row,
            [hxl.model.TagPattern.parse('#affected')]
        )
        self.assertEqual(100, result)

    def test_max(self):
        result = f.FUNCTIONS['max'](
            self.row,
            [hxl.model.TagPattern.parse('#affected')]
        )
        self.assertEqual(400, result)

    def test_average(self):
        result = f.FUNCTIONS['average'](
            self.row,
            [hxl.model.TagPattern.parse('#affected')]
        )
        self.assertEqual(250, result)

    def test_join(self):
        result = f.FUNCTIONS['join'](
            self.row,
            ['|', hxl.model.TagPattern.parse('#affected')]
        )
        self.assertEqual('100|200|300|400', result)

    def test_datedif(self):
        F = f.FUNCTIONS['datedif']
        self.assertEqual(1, F(
            self.row,
            ['2018-01-01', '2019-03-01', 'Y']
        ))
        self.assertEqual(2, F(
            self.row,
            ['2018-01-01', '2018-03-01', 'M']
        ))
        self.assertEqual(5, F(
            self.row,
            ['2018-01-01', '2018-02-05', 'W']
        ))
        self.assertEqual(31, F(
            self.row,
            ['2018-01-01', '2018-02-01', 'D']
        ))

    def test_embedded(self):

        result = f.multiply(self.row, [
            [f.add, ['1', '2']],
            '3'
        ])
        self.assertEqual(9, result)

        
class TestParser(unittest.TestCase):
    """Test the hxl.formulas.lexer class"""

    def setUp(self):
        pass

    def parse(self, s):
        return p.parser.parse(s, lexer=l.lexer)

    def test_constants(self):
        self.assertEquals([f.const, [1]], self.parse("1"))
        self.assertEquals([f.const, [1.1]], self.parse("1.1"))
        self.assertEquals([f.const, ['foo']], self.parse('"foo"'))
        self.assertEquals([f.const, ["foo\tfoo"]], self.parse('"foo\\tfoo"'))

    def test_simple_math(self):
        self.assertEquals([f.add, [[f.const, [1]], [f.const, [1]]]], self.parse("1 + 1"))

    def test_groups(self):
        self.assertEquals(
            [f.multiply, [[f.const, [2]], [f.add, [[f.const, [1]], [f.const, [1]]]]]],
            self.parse("2 * (1 + 1)")
        )

    def test_functions(self):
        self.assertEquals(
            [f.function, ['sum', [f.const, [1]], [f.const, [2]], [f.const, [3]]]],
            self.parse("sum(1, 2, 3)")
        )

class TestEval(unittest.TestCase):

    TAGS = ["#org", "#adm1", "#affected+f+children", "#affected+m+children", "#affected+f+adults", "#affected+m+adults"]
    DATA = ["Org A", "Coast Region", "100", "200", "300", "400"]

    def setUp(self):
        columns = [hxl.model.Column.parse(tag) for tag in self.TAGS]
        self.row = hxl.model.Row(columns=columns, values=self.DATA)

    def test_constant(self):
        self.assertEqual(10, e.eval(self.row, '10'))

    def test_simple(self):
        self.assertEqual(2, e.eval(self.row, '1 + 1'))

    def test_non_existant_tag(self):
        # non-existant should be zero in numeric calculations
        self.assertEqual(0, e.eval(self.row, "#xxx * 100"))

    def test_string_in_calc(self):
        self.assertEqual(100, e.eval(self.row, "#org + #affected+f+children"))
        self.assertEqual(0, e.eval(self.row, "#org * #affected+f+children"))

    def test_div0(self):
        self.assertEqual('NaN', e.eval(self.row, '#affected+m+children / 0'))
        self.assertEqual('NaN', e.eval(self.row, '#affected+m+children / #org'))

    def test_order_of_operations(self):
        self.assertEqual(7, e.eval(self.row, '1 + 2 * 3'))
        self.assertEqual(20100, e.eval(self.row, '#affected+f+children + #affected+m+children * 100'))
        self.assertEqual(30000, e.eval(self.row, '(#affected+f+children + #affected+m+children) * 100'))

    def test_complex_results(self):
        self.assertEqual(50, e.eval(self.row, '#affected+m+children / #affected+m+adults * 100'))

    def test_tagpatterns(self):
        self.assertEqual(300, e.eval(self.row, '#affected+f+children + #affected+m+children'))

    def test_min_function(self):
        self.assertEqual(100, e.eval(self.row, 'min(#affected)'))

    def test_max_function(self):
        self.assertEqual(400, e.eval(self.row, 'max(#affected)'))

    def test_round_function(self):
        self.assertEqual(3, e.eval(self.row, 'round(3.4)'))
        self.assertEqual(66.7, e.eval(self.row, 'round(#affected+m+children / #affected+f+adults * 1000) / 10'))

    def test_datedif_function(self):
        columns = [hxl.model.Column.parse(tag) for tag in ['#date+start', '#date+end']]
        row = hxl.model.Row(columns=columns, values=['2018-01-01', '2018-02-03'])
        self.assertEqual(5, e.eval(row, 'datedif(#date+start, #date+end, "W")'))

    def test_nested_functions(self):
        self.assertEqual(5, e.eval(self.row, 'round(round(3.4) + round(1.9))'))



