"""
Unit tests for the hxl.model module
David Megginson
October 2014

License: Public Domain
"""

import unittest
from hxl.model import HXLColumn, HXLRow, HXLValue

class TestColumn(unittest.TestCase):

    HXL_TAG = '#foo'
    LANGUAGE_CODE = 'en'
    HEADER_TEXT = 'Foo header'
    COLUMN_NUMBER = 5
    SOURCE_COLUMN_NUMBER = 7

    def setUp(self):
        self.column = HXLColumn(TestColumn.COLUMN_NUMBER, TestColumn.SOURCE_COLUMN_NUMBER,
                                TestColumn.HXL_TAG, TestColumn.LANGUAGE_CODE, TestColumn.HEADER_TEXT)

    def test_variables(self):
        self.assertEquals(TestColumn.COLUMN_NUMBER, self.column.columnNumber)
        self.assertEquals(TestColumn.SOURCE_COLUMN_NUMBER, self.column.sourceColumnNumber)
        self.assertEquals(TestColumn.HXL_TAG, self.column.hxlTag)
        self.assertEquals(TestColumn.LANGUAGE_CODE, self.column.languageCode)
        self.assertEquals(TestColumn.HEADER_TEXT, self.column.headerText)

    def test_display_tag(self):
        self.assertEquals(TestColumn.HXL_TAG + '/' + TestColumn.LANGUAGE_CODE, self.column.getDisplayTag())
        self.column.languageCode = None
        self.assertEquals(TestColumn.HXL_TAG, self.column.getDisplayTag())

class TestRow(unittest.TestCase):

    ROW_NUMBER = 5
    SOURCE_ROW_NUMBER = 4
    TAGS = ['#sector', '#org', '#country']
    CONTENT = ['Health', 'WFP', 'Liberia'];

    def setUp(self):
        columns = []
        for columnNumber, hxlTag in enumerate(TestRow.TAGS):
            columns.append(HXLColumn(columnNumber, -1, hxlTag))
        self.row = HXLRow(columns, TestRow.ROW_NUMBER, TestRow.SOURCE_ROW_NUMBER)
        for content in TestRow.CONTENT:
            self.row.append(content)

    def test_data(self):
        self.assertEquals(TestRow.CONTENT, self.row.values)

    def test_variables(self):
        self.assertEquals(TestRow.ROW_NUMBER, self.row.rowNumber)
        self.assertEquals(TestRow.SOURCE_ROW_NUMBER, self.row.sourceRowNumber)

    def test_iteration(self):
        expectedLength = len(TestRow.TAGS)
        actualLength = 0;
        for value in self.row:
            actualLength = actualLength + 1
        self.assertEquals(expectedLength, actualLength)

    def test_append(self):
        columnNumber = len(TestRow.TAGS)
        oldLength = len(self.row.values)
        value = HXLValue(HXLColumn(columnNumber, -1, '#adm1'), 'Lofa County')
        self.row.append(value)
        self.assertEquals(oldLength + 1, len(self.row.values))
        self.assertEquals('#adm1', self.row.values[oldLength].column.hxlTag)

    def test_get(self):
        self.assertEquals('WFP', self.row.get('#org').content)

    def test_getAll(self):
        result = self.row.getAll('#org')
        self.assertTrue(type(result) is list)
        self.assertEquals(1, len(result))
        self.assertEquals('#org', result[0].column.hxlTag)

class TestValue(unittest.TestCase):

    HXL_TAG = '#sector'
    CONTENT = 'Health'
    COLUMN_NUMBER = 5
    SOURCE_COLUMN_NUMBER = 6

    def setUp(self):
        self.value = HXLValue(
            HXLColumn(TestValue.COLUMN_NUMBER, TestValue.SOURCE_COLUMN_NUMBER, TestValue.HXL_TAG),
            TestValue.CONTENT
        )

    def test_variables(self):
        self.assertEquals(TestValue.HXL_TAG, self.value.column.hxlTag)
        self.assertEquals(TestValue.CONTENT, self.value.content)
        self.assertEquals(TestValue.COLUMN_NUMBER, self.value.column.columnNumber)
        self.assertEquals(TestValue.SOURCE_COLUMN_NUMBER, self.value.column.sourceColumnNumber)

# end
