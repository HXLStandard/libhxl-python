"""
Unit tests for the hxl.scripts module
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

import hxl
import hxl.scripts

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
                expected_output_file = output_file
                )
            )

    def assertExitStatus(self, options, exit_status=hxl.scripts.EXIT_OK, input_file=None):
        if not input_file:
            input_file = self.input_file
        self.assertTrue(
            try_script(
                self.function,
                options,
                input_file,
                expected_exit_status = exit_status
            )
        )


class TestAdd(BaseTest):
    """
    Test the hxladd command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxladd_main
        self.input_file = 'input-simple.csv'

    def test_default(self):
        self.assertOutput(['-s', 'date+reported=2015-03-31'], 'add-output-default.csv')
        self.assertOutput(['--spec', 'date+reported=2015-03-31'], 'add-output-default.csv')

    def test_headers(self):
        self.assertOutput(['-s', 'Report Date#date+reported=2015-03-31'], 'add-output-headers.csv')
        self.assertOutput(['--spec', 'Report Date#date+reported=2015-03-31'], 'add-output-headers.csv')

    def test_before(self):
        self.assertOutput(['-b', '-s', 'date+reported=2015-03-31'], 'add-output-before.csv')
        self.assertOutput(['--before', '--spec', 'date+reported=2015-03-31'], 'add-output-before.csv')


class TestClean(BaseTest):
    """
    Test the hxlclean command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxlclean_main
        self.input_file = 'input-simple.csv'

    def test_noheaders(self):
        self.assertOutput(['-r'], 'clean-output-noheaders.csv')
        self.assertOutput(['--remove-headers'], 'clean-output-noheaders.csv')

    def test_headers(self):
        self.assertOutput([], 'clean-output-headers.csv')

    def test_whitespace(self):
        self.assertOutput(['-w', 'subsector'], 'clean-output-whitespace-tags.csv', 'input-whitespace.csv')

    def test_case(self):
        self.assertOutput(['-u', 'sector,subsector'], 'clean-output-upper.csv')
        self.assertOutput(['-l', 'sector,subsector'], 'clean-output-lower.csv')

    # TODO: test dates and numbers


class TestCount(BaseTest):
    """
    Test the hxlcount command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxlcount_main
        self.input_file = 'input-simple.csv'

    def test_simple(self):
        self.assertOutput(['-t', 'org,adm1'], 'count-output-simple.csv')
        self.assertOutput(['--tags', 'org,adm1'], 'count-output-simple.csv')

    def test_aggregated(self):
        self.assertOutput(['-t', 'org,adm1', '-a', 'targeted'], 'count-output-aggregated.csv')

    def test_count_colspec(self):
        self.assertOutput(['-t', 'org,adm1', '-C', 'Activities#output+activities'], 'count-output-colspec.csv')


class TestCut(BaseTest):
    """
    Test the hxlcut command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxlcut_main
        self.input_file = 'input-simple.csv'

    def test_whitelist(self):
        self.assertOutput(['-i', 'sector,org,adm1'], 'cut-output-whitelist.csv')
        self.assertOutput(['--include', 'sector,org,adm1'], 'cut-output-whitelist.csv')

    def test_blacklist(self):
        self.assertOutput(['-x', 'population+sex,targeted'], 'cut-output-blacklist.csv')
        self.assertOutput(['--exclude', 'population+sex,targeted'], 'cut-output-blacklist.csv')


class TestMerge(BaseTest):
    """
    Test the hxlmerge command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxlmerge_main
        self.input_file = 'input-simple.csv'

    def test_merge(self):
        self.assertOutput(['-k', 'sector', '-t', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-basic.csv')
        self.assertOutput(['--keys', 'sector', '--tags', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-basic.csv')

    def test_replace(self):
        self.input_file = 'input-status.csv'
        self.assertOutput(['-r', '-k', 'sector', '-t', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-replace.csv')
        self.assertOutput(['--replace', '-k', 'sector', '-t', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-replace.csv')

    def test_overwrite (self):
        self.input_file = 'input-status.csv'
        self.assertOutput(['-O', '-r', '-k', 'sector', '-t', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-overwrite.csv')
        self.assertOutput(['--overwrite', '--replace', '-k', 'sector', '-t', 'status', '-m', resolve_file('input-merge.csv')], 'merge-output-overwrite.csv')

class TestRename(BaseTest):
    """
    Test the hxlrename command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxlrename_main
        self.input_file = 'input-simple.csv'

    def test_single(self):
        self.assertOutput(['-r', 'targeted:affected'], 'rename-output-single.csv')
        self.assertOutput(['--rename', 'targeted:affected'], 'rename-output-single.csv')

    def test_header(self):
        self.assertOutput(['-r', 'targeted:Affected#affected'], 'rename-output-header.csv')

    def test_multiple(self):
        self.assertOutput(['-r', 'targeted:affected', '-r', 'org:funding'], 'rename-output-multiple.csv')


class TestSelect(BaseTest):
    """
    Test the hxlselect command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxlselect_main
        self.input_file = 'input-simple.csv'

    def test_eq(self):
        self.assertOutput(['-q', 'sector=WASH'], 'select-output-eq.csv')
        self.assertOutput(['--query', 'sector=WASH'], 'select-output-eq.csv')

    def test_ne(self):
        self.assertOutput(['-q', 'sector!=WASH'], 'select-output-ne.csv')

    def test_lt(self):
        self.assertOutput(['-q', 'targeted<200'], 'select-output-lt.csv')

    def test_le(self):
        self.assertOutput(['-q', 'targeted<=100'], 'select-output-le.csv')

    def test_gt(self):
        self.assertOutput(['-q', 'targeted>100'], 'select-output-gt.csv')

    def test_ge(self):
        self.assertOutput(['-q', 'targeted>=100'], 'select-output-ge.csv')

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
        self.function = hxl.scripts.hxlsort_main
        self.input_file = 'input-simple.csv'

    def test_default(self):
        self.assertOutput([], 'sort-output-default.csv')

    def test_tags(self):
        self.assertOutput(['-t', 'country'], 'sort-output-tags.csv')
        self.assertOutput(['--tags', 'country'], 'sort-output-tags.csv')

    def test_numeric(self):
        self.assertOutput(['-t', 'targeted'], 'sort-output-numeric.csv')

    def test_date(self):
        self.input_file = 'input-date.csv'
        self.assertOutput(['-t', 'date+reported'], 'sort-output-date.csv')

    def test_reverse(self):
        self.assertOutput(['-r'], 'sort-output-reverse.csv')
        self.assertOutput(['--reverse'], 'sort-output-reverse.csv')


class TestTag(BaseTest):
    """
    Test the hxltag command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxltag_main
        self.input_file = 'input-untagged.csv'

    def test_full(self):
        """Use full header text for tagging."""
        self.assertOutput([
            '-m', 'Organisation#org',
            '-m', 'Cluster#sector',
            '-m', 'Country#country',
            '-m', 'Subdivision#adm1'
        ], 'tag-output-full.csv')


    def test_substrings(self):
        """Use header substrings for tagging."""
        self.assertOutput([
            '-m', 'org#org',
            '-m', 'cluster#sector',
            '-m', 'ntry#country',
            '-m', 'div#adm1'
        ], 'tag-output-full.csv')

    def test_partial(self):
        """Try tagging only one row."""
        self.assertOutput([
            '--map', 'cluster#sector'
        ], 'tag-output-partial.csv')


    def test_ambiguous(self):
        """Use an ambiguous header for the second one."""
        self.assertOutput([
            '-m', 'organisation#org',
            '-m', 'is#adm1'
        ], 'tag-output-ambiguous.csv')

    def test_default_tag(self):
        """Supply a default tag."""
        self.assertOutput([
            '-m', 'organisation#org',
            '-d', '#meta'
        ], 'tag-output-default.csv')


class TestValidate(BaseTest):
    """
    Test the hxltag command-line tool.
    """

    def setUp(self):
        self.function = hxl.scripts.hxlvalidate_main
        self.input_file = 'input-simple.csv'

    def test_default_valid_status(self):
        self.assertExitStatus([])

    def test_bad_hxl_status(self):
        self.input_file = 'input-untagged.csv'
        def try_script():
            self.assertExitStatus([], exit_status = hxl.scripts.EXIT_ERROR),
        # from the command line, this will get intercepted
        self.assertRaises(hxl.io.HXLTagsNotFoundException, try_script)

    def test_default_valid_status(self):
        self.assertExitStatus([
            '--schema', resolve_file('validation-schema-valid.csv')
        ], hxl.scripts.EXIT_OK)
        self.assertExitStatus([
            '-s', resolve_file('validation-schema-valid.csv')
        ], hxl.scripts.EXIT_OK)

    def test_default_invalid_status(self):
        self.assertExitStatus([
            '--schema', resolve_file('validation-schema-invalid.csv')
        ], hxl.scripts.EXIT_ERROR)
        self.assertExitStatus([
            '-s', resolve_file('validation-schema-invalid.csv')
        ], hxl.scripts.EXIT_ERROR)


########################################################################
# Support functions
########################################################################


def resolve_file(name):
    """
    Resolve a file name in the test directory.
    """
    return os.path.join(root_dir, 'tests', 'files', 'test_scripts', name)

def try_script(script_function, args, input_file, expected_output_file=None, expected_exit_status=hxl.scripts.EXIT_OK):
    """
    Test run a script in its own subprocess.
    @param args A list of arguments, including the script name first
    @param input_file The name of the input HXL file in ./files/test_scripts/
    @param expected_output_file The name of the expected output HXL file in ./files/test_scripts
    @return True if the actual output matches the expected output
    """

    with open(resolve_file(input_file), 'rb') as input:
        if expected_output_file is None:
            output = sys.stdout
        if sys.version_info[0] > 2:
            output = tempfile.NamedTemporaryFile(mode='w', newline='', delete=False)
        else:
            output = tempfile.NamedTemporaryFile(delete=False)
        try:
            status = script_function(args, stdin=input, stdout=output)
            if status == expected_exit_status:
                result = True
                if expected_output_file:
                    output.close()
                    result = diff(output.name, resolve_file(expected_output_file))
            else:
                print("Script exit status: {}".format(status))
                result = False
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
