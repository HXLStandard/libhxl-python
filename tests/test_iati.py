"""
Unit tests for the hxl.iati module
David Megginson
May 2018

License: Public Domain
"""

import io, unittest
import hxl.iati

from . import resolve_path

class TestParse(unittest.TestCase):
    """Test the TagPattern class."""

    def setUp(self):
        self.file = resolve_path('files/test_iati/iati-basic.xml')

    def test_raw_input(self):
        with io.open(self.file, 'r') as input:
            iati_input = hxl.iati.IATIInput(input)
            rows = [row for row in iati_input]
        self.assertEqual(5, len(rows))
        for value in [
                'XX-1-00000-001',
                'Donor',
                'Activity 1',
        ]:
            self.assertTrue(value in rows[2], value)

# end
