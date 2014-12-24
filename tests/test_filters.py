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

class BaseTest(unittest.TestCase):
    """
    Base for test classes
    """

    def assertOutput(self, options, output_file, input_file=None):
        if not input_file:
            input_file = self.input_file
        self.assertTrue(
            try_script(
                self.function,
                options,
                input_file,
                output_file
                )
            )


class TestCount(BaseTest):
    """
    Test the hxlcount command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlcount.run
        self.input_file = 'input-simple.csv'

    def test_simple(self):
        self.assertOutput(['-t', 'org,adm1'], 'count-output-simple.csv')
        self.assertOutput(['--tags', 'org,adm1'], 'count-output-simple.csv')

    def test_aggregated(self):
        self.assertOutput(['-t', 'org,adm1', '-a', 'targeted_num'], 'count-output-aggregated.csv')


class TestCut(BaseTest):
    """
    Test the hxlcut command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlcut.run
        self.input_file = 'input-simple.csv'

    def test_whitelist(self):
        self.assertOutput(['-i', 'sector,org,adm1'], 'cut-output-whitelist.csv')
        self.assertOutput(['--include', 'sector,org,adm1'], 'cut-output-whitelist.csv')

    def test_blacklist(self):
        self.assertOutput(['-x', 'sex,targeted_num'], 'cut-output-blacklist.csv')
        self.assertOutput(['--exclude', 'sex,targeted_num'], 'cut-output-blacklist.csv')


class TestFilter(BaseTest):
    """
    Test the hxlfilter command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlfilter.run
        self.input_file = 'input-simple.csv'

    def test_eq(self):
        self.assertOutput(['-f', 'sector=WASH'], 'filter-output-eq.csv')
        self.assertOutput(['--filter', 'sector=WASH'], 'filter-output-eq.csv')

    def test_ne(self):
        self.assertOutput(['-f', 'sector!=WASH'], 'filter-output-ne.csv')

    def test_lt(self):
        self.assertOutput(['-f', 'targeted_num<200'], 'filter-output-lt.csv')

    def test_le(self):
        self.assertOutput(['-f', 'targeted_num<=100'], 'filter-output-le.csv')

    def test_gt(self):
        self.assertOutput(['-f', 'targeted_num>100'], 'filter-output-gt.csv')

    def test_ge(self):
        self.assertOutput(['-f', 'targeted_num>=100'], 'filter-output-ge.csv')

    def test_inverse(self):
        self.assertOutput(['-v', '-f', 'sector=WASH'], 'filter-output-inverse.csv')
        self.assertOutput(['--invert', '--filter', 'sector=WASH'], 'filter-output-inverse.csv')

    def test_multiple(self):
        self.assertOutput(['-f', 'sector=WASH', '-f', 'sector=Salud'], 'filter-output-multiple.csv')


class TestMerge(BaseTest):
    """
    Test the hxlmerge command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlmerge.run
        self.input_file = 'input-simple.csv'

    def test_basic(self):
        file = resolve_file('input-simple.csv')
        self.assertOutput(['-t', 'org,sector', file, file], 'merge-output-basic.csv')
        self.assertOutput(['--tags', 'org,sector', file, file], 'merge-output-basic.csv')


class TestNorm(BaseTest):
    """
    Test the hxlnorm command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlnorm.run
        self.input_file = 'input-simple.csv'

    def test_noheaders(self):
        self.assertOutput([], 'norm-output-noheaders.csv')

    def test_headers(self):
        self.assertOutput(['-H'], 'norm-output-headers.csv')
        self.assertOutput(['--headers'], 'norm-output-headers.csv')

    def test_compact(self):
        self.assertOutput([], 'norm-output-compact.csv')


class TestValidate(BaseTest):
    """
    Test the hxlvalidate command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.hxlvalidate.run
        self.input_file = 'input-simple.csv'

    def test_valid(self):
        schema = resolve_file('validate-schema-valid.csv')
        self.assertOutput(['-s', schema], 'validate-output-valid.txt')
        self.assertOutput(['--schema', schema], 'validate-output-valid.txt')

    def test_number(self):
        schema = resolve_file('validate-schema-num.csv')
        self.assertOutput(['-s', schema], 'validate-output-num.txt')


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
