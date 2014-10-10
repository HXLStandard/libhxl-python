"""
Parsing library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import csv

from model import HXLColumn, HXLRow, HXLValue

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

class HXLReader:
    """
    Read HXL data from a file
    """

    source = None
    tableSpec = None
    sourceRowNumber = -1
    rowNumber = -1
    lastHeaderRow = None
    currentRow = None

    rawData = None
    disaggregationCount = 0
    disaggregationPosition = -1

    def __init__(self, source):
        self.csvreader = csv.reader(source)

    def next(self):
        return self.csvreader.next()

    def __iter__(self):
        return self;

    
