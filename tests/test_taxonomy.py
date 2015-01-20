"""
Unit tests for the hxl.taxonomy module
David Megginson
January 2015

License: Public Domain
"""

import os
import unittest
from hxl.parser import HXLReader
from hxl.taxonomy import HXLTaxonomy, HXLTerm, readTaxonomy

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))

class TestTaxonomy(unittest.TestCase):
    
    def test_ok(self):
        file = resolve_file("taxonomy_good.csv")
        taxonomy = readTaxonomy(HXLReader(open(file, 'r')))
        def callback(message, term):
            print message + term.code
        self.assertTrue(taxonomy.is_valid(callback=callback))

class TestTerm(unittest.TestCase):

    def test_properties(self):
        term = HXLTerm('XXX', 'YYY', 3)
        self.assertEquals('XXX', term.code)
        self.assertEquals('YYY', term.parent_code)
        self.assertEquals(3, term.level)

def resolve_file(name):
    """
    Resolve a file name in the test directory.
    """
    return os.path.join(root_dir, 'tests', 'files', 'test_taxonomy', name)
    
