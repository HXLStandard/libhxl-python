"""
Unit tests for the hxl.commands module
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

import hxl.commands.hxlcount
import hxl.commands.hxlfilter

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))

class TestCountCommand(unittest.TestCase):

    def setUp(self):
        self.function = hxl.commands.hxlcount.run

    def test_simple(self):
        self.assertTrue(try_script(self.function, ['-t', 'org,adm1'], 'sample-input.csv', 'count-output-simple.csv'))

class TestFilterCommand(unittest.TestCase):

    def setUp(self):
        self.function = hxl.commands.hxlfilter.run

    def test_eq(self):
        self.assertTrue(try_script(self.function, ['-f', 'sector=WASH'], 'sample-input.csv', 'filter-output-eq.csv'))

    def test_ne(self):
        self.assertTrue(try_script(self.function, ['-f', 'sector!=WASH'], 'sample-input.csv', 'filter-output-ne.csv'))

    def test_lt(self):
        self.assertTrue(try_script(self.function, ['-f', 'targeted_num<200'], 'sample-input.csv', 'filter-output-lt.csv'))

    def test_le(self):
        self.assertTrue(try_script(self.function, ['-f', 'targeted_num<=100'], 'sample-input.csv', 'filter-output-le.csv'))

    def test_gt(self):
        self.assertTrue(try_script(self.function, ['-f', 'targeted_num>100'], 'sample-input.csv', 'filter-output-gt.csv'))

    def test_ge(self):
        self.assertTrue(try_script(self.function, ['-f', 'targeted_num>=100'], 'sample-input.csv', 'filter-output-ge.csv'))

    def test_inverse(self):
        self.assertTrue(try_script(self.function, ['-v', '-f', 'sector=WASH'], 'sample-input.csv', 'filter-output-inverse.csv'))

    def test_multiple(self):
        self.assertTrue(try_script(self.function, ['-f', 'sector=WASH', '-f', 'sector=Salud'], 'sample-input.csv', 'filter-output-multiple.csv'))

def try_script(script_function, args, input_file, expected_output_file):
    """
    Test run a script in its own subprocess.
    @param args A list of arguments, including the script name first
    @param input_file The name of the input HXL file in ./files/test_commands/
    @param expected_output_file The name of the expected output HXL file in ./files/test_commands
    @return True if the actual output matches the expected output
    """

    def resolve(name):
        """
        Resolve a file name in the test directory.
        """
        return os.path.join(root_dir, 'tests', 'files', 'test_commands', name)

    input = open(resolve(input_file), 'r')
    output = tempfile.NamedTemporaryFile(delete=False)
    try:
        script_function(args, stdin=input, stdout=output)
        output.close()
        result = diff(output.name, resolve(expected_output_file))
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
