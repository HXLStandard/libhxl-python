"""
Parsing library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import csv

class HXLColSpec:
    """
    Column specification for parsing a HXL CSV file

    This class captures the way a column is encoded in the input CSV
    file, which might be different from the logical structure of the
    HXL data. Used only during parsing.
    """
    sourceColumnNumber = -1
    column = None
    fixedColumn = None
    fixedValue = None
    def __init__(self, column, fixedColumn, fixedValue):
        self.column = column
        self.fixedColumn = fixedColumn
        self.fixedValue = fixedValue

class HXLColumn:
    """
    The definition of a logical column in the HXL data.
    """ 
    hxlTag = None
    languageCode = None
    headerText = None

    def __init__(self, hxlTag, languageCode, headerText):
        self.hxlTag = hxlTag
        self.languageCode = languageCode
        self.headerText = headerText

    def getDisplayTag(self):
        if (self.hxlTag):
            if (self.languageCode):
                return self.hxlTag + '/' + self.languageCode
            else:
                return self.hxlTag
        else:
            return None

class HXLRow:
    """
    A row of data in a HXL dataset.

    Implements the iterator convention.
    """
    data = []
    rowNumber = -1
    sourceRowNumber = -1
    iteratorIndex = -1

    def __init__(self, rowNumber, sourceRowNumber):
        self.rowNumber = rowNumber
        self.sourceRowNumber = sourceRowNumber

    def next(self):
        ++self.iteratorIndex
        return self.data[iteratorIndex]

    def __iter__(self):
        return self

class HXLTableSpec:
    colSpecs = []

    def add(self, colSpec):
        self.colSpecs.append(colSpec)

    def getDisaggregationCount(self):
        n = 0;
        for colSpec in self.colSpecs:
            if colSpec.fixedColumn:
                ++n
        return n

    def getFixedPos(self, n):
        pos = 0
        for colSpec in self.colSpecs:
            if n == 0:
                return pos
            else:
                --n
            ++pos
        return -1

class HXLValue:
    """
    A single HXL value at the intersection of a row and column
    """
    column = None
    content = None
    columnNumber = -1
    sourceColumnNumber = -1

    def __init__(self, column, content, columnNumber, sourceColumnNumber):
        self.column = column
        self.content = content
        self.columnNumber = columnNumber
        self.sourceColumnNumber = sourceColumnNumber

class HXLReader:
    """Read HXL data from a file"""

    def __init__(self, source):
        self.csvreader = csv.reader(source)

    def next(self):
        return self.csvreader.next()

    def __iter__(self):
        return self;
