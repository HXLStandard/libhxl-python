"""
Unit tests for the hxl.iati module
David Megginson
May 2018

License: Public Domain
"""

import hxl, io, unittest

from . import resolve_path

class TestIATIInput(unittest.TestCase):
    """Test the TagPattern class."""

    def setUp(self):
        self.file = resolve_path('files/test_iati/iati-basic.xml')

    def test_raw_input(self):
        with io.open(self.file, 'r') as input:
            iati_input = hxl.io.IATIInput(input)
            rows = [row for row in iati_input]
        self.assertEqual(5, len(rows))
        for value in [
                'XX-1-00000-001',
                'Role 1 participating org',
                'Activity 1',
        ]:
            self.assertTrue(value in rows[2], value)

    def test_cooked_input(self):
        source = hxl.data(self.file, allow_local=True)
        tags = [column.tag for column in source.columns]
        rows = [row.values for row in source]
        self.assertTrue('#org' in tags)
        
# end
