"""
Unit tests for the hxl.filters module
David Megginson
December 2014

License: Public Domain
"""
from __future__ import print_function

import unittest
import os
import sys
import subprocess
import filecmp
import difflib
import tempfile

import hxl.filters.add
import hxl.filters.count
import hxl.filters.cut
import hxl.filters.merge
import hxl.filters.clean
import hxl.filters.rename
import hxl.filters.select
import hxl.filters.sort
import hxl.filters.validate

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

class TestAdd(BaseTest):
    """
    Test the hxladd command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.add.run
        self.input_file = 'input-simple.csv'

    def test_default(self):
        self.assertOutput(['-v', 'report_date=2015-03-31'], 'add-output-default.csv')
        self.assertOutput(['--value', 'report_date=2015-03-31'], 'add-output-default.csv')

    def test_headers(self):
        self.assertOutput(['-v', 'Report Date#report_date=2015-03-31'], 'add-output-headers.csv')
        self.assertOutput(['--value', 'Report Date#report_date=2015-03-31'], 'add-output-headers.csv')

    def test_before(self):
        self.assertOutput(['-b', '-v', 'report_date=2015-03-31'], 'add-output-before.csv')
        self.assertOutput(['--before', '--value', 'report_date=2015-03-31'], 'add-output-before.csv')

class TestCount(BaseTest):
    """
    Test the hxlcount command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.count.run
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
        self.function = hxl.filters.cut.run
        self.input_file = 'input-simple.csv'

    def test_whitelist(self):
        self.assertOutput(['-i', 'sector,org,adm1'], 'cut-output-whitelist.csv')
        self.assertOutput(['--include', 'sector,org,adm1'], 'cut-output-whitelist.csv')

    def test_blacklist(self):
        self.assertOutput(['-x', 'sex,targeted_num'], 'cut-output-blacklist.csv')
        self.assertOutput(['--exclude', 'sex,targeted_num'], 'cut-output-blacklist.csv')


class TestMerge(BaseTest):
    """
    Test the hxlmerge command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.merge.run
        self.input_file = 'input-simple.csv'

    def test_merge(self):
        self.assertOutput(['-k', 'sector', '-t', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-basic.csv')
        self.assertOutput(['--keys', 'sector', '--tags', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-basic.csv')

    #def test_replace(self):
    #    self.input_file = 'input-status.csv'
    #    self.assertOutput(['-r', '-k', 'sector', '-t', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-replace.csv')
    #    self.assertOutput(['--replace', '-k', 'sector', '-t', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-replace.csv')

class TestClean(BaseTest):
    """
    Test the hxlclean command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.clean.run
        self.input_file = 'input-simple.csv'

    def test_noheaders(self):
        self.assertOutput(['-r'], 'clean-output-noheaders.csv')
        self.assertOutput(['--remove-headers'], 'clean-output-noheaders.csv')

    def test_headers(self):
        self.assertOutput([], 'clean-output-headers.csv')

    def test_compact(self):
        self.assertOutput([], 'clean-output-compact.csv')

    def test_whitespace(self):
        self.assertOutput(['-W'], 'clean-output-whitespace-all.csv', 'input-whitespace.csv')
        self.assertOutput(['-w', 'subsector'], 'clean-output-whitespace-tags.csv', 'input-whitespace.csv')

    def test_case(self):
        self.assertOutput(['-u', 'sector,subsector'], 'clean-output-upper.csv')
        self.assertOutput(['-l', 'sector,subsector'], 'clean-output-lower.csv')

    # TODO: test dates and numbers

class TestRename(BaseTest):
    """
    Test the hxlrename command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.rename.run
        self.input_file = 'input-simple.csv'

    def test_single(self):
        self.assertOutput(['-r', 'targeted_num:affected_num'], 'rename-output-single.csv')
        self.assertOutput(['--rename', 'targeted_num:affected_num'], 'rename-output-single.csv')

    def test_header(self):
        self.assertOutput(['-r', 'targeted_num:Affected#affected_num'], 'rename-output-header.csv')

    def test_multiple(self):
        self.assertOutput(['-r', 'targeted_num:affected_num', '-r', 'org:funding'], 'rename-output-multiple.csv')


class TestSelect(BaseTest):
    """
    Test the hxlselect command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.select.run
        self.input_file = 'input-simple.csv'

    def test_eq(self):
        self.assertOutput(['-q', 'sector=WASH'], 'select-output-eq.csv')
        self.assertOutput(['--query', 'sector=WASH'], 'select-output-eq.csv')

    def test_ne(self):
        self.assertOutput(['-q', 'sector!=WASH'], 'select-output-ne.csv')

    def test_lt(self):
        self.assertOutput(['-q', 'targeted_num<200'], 'select-output-lt.csv')

    def test_le(self):
        self.assertOutput(['-q', 'targeted_num<=100'], 'select-output-le.csv')

    def test_gt(self):
        self.assertOutput(['-q', 'targeted_num>100'], 'select-output-gt.csv')

    def test_ge(self):
        self.assertOutput(['-q', 'targeted_num>=100'], 'select-output-ge.csv')

    def test_re(self):
        self.assertOutput(['-q', 'sector~^W..H'], 'select-output-re.csv')

    def test_nre(self):
        self.assertOutput(['-q', 'sector!~^W..H'], 'select-output-nre.csv')

    def test_reverse(self):
        self.assertOutput(['-r', '-q', 'sector=WASH'], 'select-output-reverse.csv')
        self.assertOutput(['--reverse', '--query', 'sector=WASH'], 'select-output-reverse.csv')

    def test_multiple(self):
        self.assertOutput(['-q', 'sector=WASH', '-q', 'sector=Salud'], 'select-output-multiple.csv')


class TestSort(BaseTest):
    """
    Test the hxlsort command-line tool,.
    """

    def setUp(self):
        self.function = hxl.filters.sort.run
        self.input_file = 'input-simple.csv'

    def test_default(self):
        self.assertOutput([], 'sort-output-default.csv')

    def test_tags(self):
        self.assertOutput(['-t', 'country'], 'sort-output-tags.csv')
        self.assertOutput(['--tags', 'country'], 'sort-output-tags.csv')

    def test_numeric(self):
        self.assertOutput(['-t', 'targeted_num'], 'sort-output-numeric.csv')

    def test_date(self):
        self.input_file = 'input-date.csv'
        self.assertOutput(['-t', 'report_date'], 'sort-output-date.csv')

    def test_reverse(self):
        self.assertOutput(['-r'], 'sort-output-reverse.csv')
        self.assertOutput(['--reverse'], 'sort-output-reverse.csv')


class TestValidate(BaseTest):
    """
    Test the hxlvalidate command-line tool.
    """

    def setUp(self):
        self.function = hxl.filters.validate.run
        self.input_file = 'input-simple.csv'

    def test_valid(self):
        schema = resolve_file('validate-schema-valid.csv')
        self.assertOutput(['-s', schema], 'validate-output-valid.csv')
        self.assertOutput(['--schema', schema], 'validate-output-valid.csv')

    def test_all(self):
        schema = resolve_file('validate-schema-valid.csv')
        self.assertOutput(['-s', schema, '-a'], 'validate-output-all.csv')
        self.assertOutput(['-s', schema, '--all'], 'validate-output-all.csv')

    def test_number(self):
        schema = resolve_file('validate-schema-num.csv')
        self.assertOutput(['-s', schema], 'validate-output-num.csv')

    def test_taxonomy(self):
        # good taxonomy
        schema = resolve_file('validate-schema-taxonomy-valid.csv')
        self.assertOutput(['-s', schema], 'validate-output-taxonomy-valid.csv')

        # bad taxonomy
        schema = resolve_file('validate-schema-taxonomy-invalid.csv')
        self.assertOutput(['-s', schema], 'validate-output-taxonomy-invalid.csv')


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

    with open(resolve_file(input_file), 'r') as input:
        if sys.version_info[0] > 2:
            output = tempfile.NamedTemporaryFile(mode='w', newline='', delete=False)
        else:
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
    with open(file1, 'r') as input1:
        with open(file2, 'r') as input2:
            diffs = difflib.unified_diff(
                input1.read().splitlines(),
                input2.read().splitlines()
                )
    no_diffs = True
    for diff in diffs:
        no_diffs = False
        print(diff, file=sys.stderr)
    return no_diffs

# end
