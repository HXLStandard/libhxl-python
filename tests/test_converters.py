# coding=utf-8
"""
Unit tests for converters
David Megginson
June 2016

License: Public Domain
"""

import unittest
import hxl
from . import resolve_path


class TaggerTest(unittest.TestCase):
    """Unit tests for hxl.converters.Tagger"""

    UNTAGGED = [
        ['Country Name', 'Country Code', '2016', '2015', '2014', '2013', '2012'],
        ['Sudan', 'SUD', '10000', '8500', '9000', '7500', '6000'],
        ['Syria', 'SYR', '100000', '85000', '90000', '75000', '60000'],
        ['Yemen', 'YEM', '50000', '43000', '45000', '38000', '30000']
    ]

    EXPECTED_TAGS_SIMPLE = ['#country+name', '#country+code', '', '', '', '', '']

    EXPECTED_TAGS_DEFAULT = ['#country+name', '#country+code', '#targeted', '#targeted', '#targeted', '#targeted', '#targeted']

    def setUp(self):
        pass

    def test_basic(self):
        """Basic tagging operation."""
        tagging_specs = [('Country Name', '#country+name'), ('Country Code', '#country+code')]
        source = hxl.tagger(self.UNTAGGED, tagging_specs)
        self.assertEqual(self.EXPECTED_TAGS_SIMPLE, source.display_tags)

    def test_case_insensitive(self):
        """Test that the tagger is case-insensitive."""
        tagging_specs = [('country name', '#country+name'), ('code', '#country+code')]
        source = hxl.data(hxl.converters.Tagger(self.UNTAGGED, tagging_specs))
        self.assertEqual(self.EXPECTED_TAGS_SIMPLE, source.display_tags)

    def test_space_insensitive(self):
        """Test that the tagger is whitespace-insensitive."""
        tagging_specs = [('  Country  Name', '#country+name'), ('Country    Code  ', '#country+code')]
        source = hxl.data(hxl.converters.Tagger(self.UNTAGGED, tagging_specs))
        self.assertEqual(self.EXPECTED_TAGS_SIMPLE, source.display_tags)

    def test_partial_match(self):
        """Test for substrings."""
        tagging_specs = [('name', '#country+name'), ('code', '#country+code')]
        source = hxl.data(hxl.converters.Tagger(self.UNTAGGED, tagging_specs))
        self.assertEqual(self.EXPECTED_TAGS_SIMPLE, source.display_tags)
        
    def test_full_match(self):
        """Test for full match."""
        tagging_specs = [('country name', '#country+name'), ('code', '#country+code')]
        source = hxl.data(hxl.converters.Tagger(self.UNTAGGED, tagging_specs, match_all=True))
        self.assertEqual(['#country+name', '', '', '', '', '', ''], source.display_tags)

    def test_default_tag(self):
        """Test for default tag."""
        tagging_specs = [('Country Name', '#country+name'), ('Country Code', '#country+code')]
        source = hxl.data(hxl.converters.Tagger(self.UNTAGGED, tagging_specs, default_tag='#targeted'))
        self.assertEqual(self.EXPECTED_TAGS_DEFAULT, source.display_tags)

    def test_wide_data(self):
        """Test for very wide data"""
        tagging_specs = [
            ('cod_wardsr', '#adm3+code',),
            ('food_monthly', '#value+expenditure+food_monthly',),
        ]
        filename = resolve_path("files/test_converters/wide-tagging-test.csv")
        source = hxl.data(hxl.converters.Tagger(hxl.input.make_input(filename, allow_local=True), tagging_specs)).cache()
        self.assertTrue('#value+expenditure+food_monthly' in source.display_tags)
                     
