"""
Unit tests for the hxl.filters module
David Megginson
December 2014

License: Public Domain
"""

import unittest
import os
import sys
import subprocess
import filecmp
import difflib
import tempfile

import hxl.filters.hxlcount
import hxl.filters.hxlcut
import hxl.filters.hxlfilter
import hxl.filters.hxlmerge
import hxl.filters.hxlnorm
import hxl.filters.hxlvalidate

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))


########################################################################
# Test classes
########################################################################


class TestCount(unittest.TestCase):
    """
    Test the hxlcount command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlcount.run

    def test_simple(self):
        self.assertTrue(try_script(self.function, ['-t', 'org,adm1'], 'input-simple.csv', 'count-output-simple.csv'))

    def test_aggregated(self):
        self.assertTrue(try_script(self.function, ['-t', 'org,adm1', '-a', 'targeted_num'], 'input-simple.csv', 'count-output-aggregated.csv'))


class TestCut(unittest.TestCase):
    """
    Test the hxlcut command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlcut.run

    def test_whitelist(self):
        self.assertTrue(try_script(self.function, ['-I', 'sector,org,adm1'], 'input-simple.csv', 'cut-output-whitelist.csv'))
        self.assertTrue(try_script(self.function, ['--include', 'sector,org,adm1'], 'input-simple.csv', 'cut-output-whitelist.csv'))

    def test_blacklist(self):
        self.assertTrue(try_script(self.function, ['-X', 'sex,targeted_num'], 'input-simple.csv', 'cut-output-blacklist.csv'))
        self.assertTrue(try_script(self.function, ['--exclude', 'sex,targeted_num'], 'input-simple.csv', 'cut-output-blacklist.csv'))


class TestFilter(unittest.TestCase):
    """
    Test the hxlfilter command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlfilter.run

    def test_eq(self):
        self.assertTrue(try_script(self.function, ['-f', 'sector=WASH'], 'input-simple.csv', 'filter-output-eq.csv'))

    def test_ne(self):
        self.assertTrue(try_script(self.function, ['-f', 'sector!=WASH'], 'input-simple.csv', 'filter-output-ne.csv'))

    def test_lt(self):
        self.assertTrue(try_script(self.function, ['-f', 'targeted_num<200'], 'input-simple.csv', 'filter-output-lt.csv'))

    def test_le(self):
        self.assertTrue(try_script(self.function, ['-f', 'targeted_num<=100'], 'input-simple.csv', 'filter-output-le.csv'))

    def test_gt(self):
        self.assertTrue(try_script(self.function, ['-f', 'targeted_num>100'], 'input-simple.csv', 'filter-output-gt.csv'))

    def test_ge(self):
        self.assertTrue(try_script(self.function, ['-f', 'targeted_num>=100'], 'input-simple.csv', 'filter-output-ge.csv'))

    def test_inverse(self):
        self.assertTrue(try_script(self.function, ['-v', '-f', 'sector=WASH'], 'input-simple.csv', 'filter-output-inverse.csv'))

    def test_multiple(self):
        self.assertTrue(try_script(self.function, ['-f', 'sector=WASH', '-f', 'sector=Salud'], 'input-simple.csv', 'filter-output-multiple.csv'))


class TestMerge(unittest.TestCase):
    """
    Test the hxlmerge command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlmerge.run

    def test_basic(self):
        self.assertTrue(
            try_script(
                self.function,
                ['-t', 'org,sector', resolve_file('input-simple.csv'), resolve_file('input-simple.csv')],
                'input-simple.csv',
                'merge-output-basic.csv'
                )
            )


class TestNorm(unittest.TestCase):
    """
    Test the hxlnorm command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlnorm.run

    def test_noheaders(self):
        self.assertTrue(try_script(self.function, [], 'input-simple.csv', 'norm-output-noheaders.csv'))

    def test_headers(self):
        self.assertTrue(try_script(self.function, ['-H'], 'input-simple.csv', 'norm-output-headers.csv'))

    def test_compact(self):
        self.assertTrue(try_script(self.function, [], 'input-compact.csv', 'norm-output-compact.csv'))


class TestValidate(unittest.TestCase):
    """
    Test the hxlvalidate command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlvalidate.run

    def test_valid(self):
        self.assertTrue(
            try_script(
                self.function,
                ['-s', resolve_file('validate-schema-valid.csv')],
                'input-simple.csv',
                'validate-output-valid.txt'
                )
            )

    def test_number(self):
        self.assertTrue(
            try_script(
                self.function,
                ['-s', resolve_file('validate-schema-num.csv')],
                'input-simple.csv',
                'validate-output-num.txt'
                )
            )


########################################################################
# Support functions
########################################################################

def resolve_file(name):
    """
    Resolve a file name in the test directory.
    """
    return os.path.join(root_dir, 'tests', 'files', 'test_filters', name)

def try_script(script_function, args, input_file, expected_output_file):
    """
    Test run a script in its own subprocess.
    @param args A list of arguments, including the script name first
    @param input_file The name of the input HXL file in ./files/test_filters/
    @param expected_output_file The name of the expected output HXL file in ./files/test_filters
    @return True if the actual output matches the expected output
    """

    input = open(resolve_file(input_file), 'r')
    output = tempfile.NamedTemporaryFile(delete=False)
    try:
        script_function(args, stdin=input, stdout=output)
        output.close()
        result = diff(output.name, resolve_file(expected_output_file))
    finally:
        # Not using with, because Windows won't allow file to be opened twice
        os.remove(output.name)
    return result

def diff(file1, file2):
    """
    Compare two files, ignoring line end differences

    If there are differences, print them to stderr in unified diff format.

    @param file1 The full pathname of the first file to compare
    @param file2 The full pathname of the second file to compare
    @return True if the files are the same, o
    """
    with open(file1, 'rt') as input1:
        with open(file2, 'rt') as input2:
            diffs = difflib.unified_diff(
                input1.read().splitlines(),
                input2.read().splitlines()
                )
    no_diffs = True
    for diff in diffs:
        no_diffs = False
        print >>sys.stderr, diff
    return no_diffs

# end
