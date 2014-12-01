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
        pass

    def test_pattern(self):
        self.assertTrue(hxlvalidate(
                input=open_file('pattern-data-01a.csv'),
                schema_input=open_file('pattern-schema-01.csv')
            ))
        self.assertFalse(hxlvalidate(
                input=open_file('pattern-data-01b.csv'),
                schema_input=open_file('pattern-schema-01.csv')
            ))

# end
