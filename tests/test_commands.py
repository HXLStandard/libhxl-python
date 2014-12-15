"""
Unit tests for the hxl.commands module
David Megginson
December 2014

License: Public Domain
"""

import unittest
import os
from hxl.commands.hxlvalidate import hxlvalidate

def open_file(name):
    path = os.path.join(os.path.dirname(__file__), 'files', 'test_commands', name);
    return open(path, 'r')

class TestValidateCommand(unittest.TestCase):

    def setUp(self):
        self.null = open(os.devnull, 'w')
        pass

    def test_pattern(self):

        # data matches pattern
        self.assertTrue(hxlvalidate(
                input=open_file('pattern-data-01a.csv'),
                output=self.null,
                schema_input=open_file('pattern-schema-01.csv')
            ))

        # data doesn't match pattern
        self.assertFalse(hxlvalidate(
                input=open_file('pattern-data-01b.csv'),
                output=self.null,
                schema_input=open_file('pattern-schema-01.csv')
            ))

    def test_default(self):

        # data has correct numeric field in default schema
        self.assertTrue(hxlvalidate(
                input=open_file('default-data-02a.csv'),
                output=self.null
                ))

        # data does not have correct numeric field
        self.assertFalse(hxlvalidate(
                input=open_file('default-data-02b.csv'),
                output=self.null
                ))


# end
