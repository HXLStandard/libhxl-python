"""
Unit tests for the hxl.writer module
David Megginson
October 2014

License: Public Domain
"""

import unittest
from StringIO import StringIO
from hxl.model import HXLRow, HXLColumn, HXLValue
from hxl.writer import HXLWriter

class TestWriter(unittest.TestCase):

    COLUMN_HEADERS = ['Organisation', 'Cluster/Sector', 'Country']
    COLUMN_TAGS = ['#org', '#sector', '#country']
    ROW_VALUES = ['UNICEF', 'Education', 'Somalia']

    def setUp(self):
        self.output = StringIO()
        self.writer = HXLWriter(self.output)
        self.row = self._makeRow()

    def test_headers(self):
        self.writer.writeHeaders(self.row)
        self.assertEquals("Organisation,Cluster/Sector,Country\r\n", self.output.getvalue())

    def test_tags(self):
        self.writer.writeTags(self.row)
        self.assertEquals("#org,#sector,#country\r\n", self.output.getvalue())

    def test_row(self):
        self.writer.writeData(self.row)
        self.assertEquals("UNICEF,Education,Somalia\r\n", self.output.getvalue())

    def _makeRow(self):
        row = HXLRow(0, 0)
        for columnNumber, tag in enumerate(TestWriter.COLUMN_TAGS):
            row.append(HXLValue(HXLColumn(tag, None, TestWriter.COLUMN_HEADERS[columnNumber]), TestWriter.ROW_VALUES[columnNumber], columnNumber, columnNumber))
        return row
