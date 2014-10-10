"""
Parsing library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import csv
import re

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

    def __iter__(self):
        return self;

    def next(self):
        if self.tableSpec == None:
            self.tableSpec = self.parseTableSpec()
            self.disaggregationCount = self.tableSpec.getDisaggregationCount()
            self.disaggregationPosition = 0

        if self.disaggregationPosition >= self.disaggregationCount or not self.rawData:
            self.rawData = self.parseSourceRow()
            if (self.rawData == None):
                return None
            self.disaggregationPosition = 0
        ++self.rowNumber

        data = []
        columnNumber = -1
        seenFixed = 0
        n = 0
        for content in self.rawData:
            colSpec = self.tableSpec.colSpecs[n]

            if not colSpec.column.hxlTag:
                continue

            if colSpec.fixedColumn:
                if not seenFixed:
                    ++columnNumber
                    fixedPosition = self.tableSpec.getFixedPosition(self.disaggregationPosition)
                    data.append(HXLValue(
                            self.tableSpec.colSpecs[fixedPosition].fixedColumn,
                            self.tableSpec.colSpecs[fixedPosition].fixedValue,
                            columnNumber,
                            sourceColumnNumber
                            ))
                    ++columnNumber
                    data.append(HXLValue(
                            self.tableSpec.colSpecs[fixedPosition].column,
                            self.rawData[fixedPosition],
                            columnNumber,
                            sourceColumnNumber
                            ))
                    seenFixed = true
                else:
                    ++columnNumber
                    data.append(HXLValue(
                            self.tableSpec.colSpecs[sourceColumnNumber].column,
                            self.rawData[sourceColumnNumber],
                            columnNumber,
                            sourceColumnNumber
                            ))
                ++n

        ++self.disaggregationPosition
        return HXLRow(data, self.rowNumber, self.sourceRowNumber)

    def parseSourceRow(self):
        ++self.sourceRowNumber
        return self.csvreader.next()

    def parseTableSpec(self):
        rawData = self.parseSourceRow()
        while rawData:
            tableSpec = self.parseHashtagRow(rawData)
            if (tableSpec != None):
                return tableSpec
            else:
                self.lastHeaderRow = rawData
            rawData = self.parseSourceRow()
        raise Exception("HXL hashtag row not found")

    
    def parseHashtagRow(self, rawDataRow):
        tableSpec = HXLTableSpec()
        seenHeader = 0
        sourceColumnNumber = 0
        for rawString in rawDataRow:
            rawString = rawString.strip()
            if rawString:
                colSpec = self.parseHashtag(sourceColumnNumber, rawString)
                if (colSpec):
                    seenHeader = 1
                    if (colSpec.fixedColumn):
                        colSpec.fixedColumn.headerText = self.prettyTag(colSpec.fixedColumn.hxlTag)
                        colSpec.column.headerText = self.prettyTag(colSpec.column.hxlTag)
                        colSpec.fixedValue = self.lastHeaderRow[sourceColumnNumber]
                    else:
                        colSpec.column.headerText = self.lastHeaderRow[sourceColumnNumber]
                else:
                    return None
            else:
                colSpec = HXLColSpec(sourceColumnNumber, None, None)
                colSpec.column = HXLColumn(None, None, None)
            tableSpec.add(colSpec)
            ++sourceColumnNumber

        if seenHeader:
            return tableSpec
        else:
            return None

    def parseHashtag(self, sourceColumnNumber, rawString):
        print "Checking " + rawString + " for a hashtag"
        tagRegexp = '(#[a-zA-z0-9_]+)(?:\/([a-zA-Z]{2}))?'
        fullRegexp = '^\s*' + tagRegexp + '(?:\s*\+\s*' + tagRegexp + ')?\s*$';
        result = re.match(fullRegexp, rawString)
        if result != None:
            print "Match!"
            exit
            col1 = HXLColumn(result.group(1), result.group(2))
            col2 = Null
            if result.group(3):
                col2 = HXLColumn(result.group(3), result.group(4))
                colSpec = HXLColSpec(sourceColumnNumber, col2, col1)
            else:
                colSpec = HXLColSpec(sourceColumnNumber, col1, None)
            return colSpec
        else:
            print "Failed"
            return False

    def prettyTag(self, hxlTag):
        hxlTag = re.replace('^#', '', hxlTag)
        hxlTag = re.replace('_(date|deg|id|link|num)$', '', hxlTag)
        hxlTag = re.replace('_', ' ', hxlTag)
        return hxlTag.capitalize()
