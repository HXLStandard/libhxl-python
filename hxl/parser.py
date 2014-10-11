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

class HXLTableSpec:
    """
    Table metadata for parsing a HXL dataset
    """

    def __init__(self):
        self.colSpecs = []

    def append(self, colSpec):
        self.colSpecs.append(colSpec)

    def getDisaggregationCount(self):
        n = 0;
        for colSpec in self.colSpecs:
            if colSpec.fixedColumn:
                ++n
        return n

    def getFixedPosition(self, n):
        pos = 0
        for colSpec in self.colSpecs:
            if n == 0:
                return pos
            else:
                --n
            ++pos
        return -1

    def __str__(self):
        s = '<HXLTableSpec';
        for colSpec in self.colSpecs:
            s += "\n  " + re.sub("\n", "\n  ", str(colSpec));
        s += "\n>"
        return s

class HXLColSpec:
    """
    Column metadata for parsing a HXL CSV file

    This class captures the way a column is encoded in the input CSV
    file, which might be different from the logical structure of the
    HXL data. Used only during parsing.
    """

    def __init__(self, sourceColumnNumber, column=None, fixedColumn=None, fixedValue=None):
        self.sourceColumnNumber = sourceColumnNumber
        self.column = column
        self.fixedColumn = fixedColumn
        self.fixedValue = fixedValue

    def __str__(self):
        s = "<HXLColSpec";
        s += "\n  main: " + str(self.column)
        if (self.fixedColumn):
            s += "\n  fixed: " + str(self.fixedColumn)
            s += "\n  fixed value: " + str(self.fixedValue)
        s += "\n  source column number: " + str(self.sourceColumnNumber)
        s += "\n>"
        return s

class HXLReader:
    """
    Read HXL data from a file
    """

    def __init__(self, source):
        self.csvreader = csv.reader(source)
        self.tableSpec = None
        self.sourceRowNumber = -1
        self.rowNumber = -1
        self.lastHeaderRow = None
        self.currentRow = None
        self.rawData = None
        self.disaggregationCount = 0
        self.disaggregationPosition = -1

    def __iter__(self):
        return self;

    def next(self):
        """
        Iterable function to return the next row of HXL values.
        Returns a HXLRow, or raises StopIteration exception at end
        """

        # If we don't have a tableSpec yet (row of HXL tags), scan for one
        if self.tableSpec == None:
            self.tableSpec = self.parseTableSpec()
            self.disaggregationCount = self.tableSpec.getDisaggregationCount()
            self.disaggregationPosition = 0

        # Read more raw data unless we're in the middle of generating virtual rows
        # from compact-disaggregated syntax
        if self.disaggregationPosition >= self.disaggregationCount or not self.rawData:
            self.rawData = self.parseSourceRow()
            if (self.rawData == None):
                return None
            self.disaggregationPosition = 0
        ++self.rowNumber

        row = HXLRow(self.rowNumber, self.sourceRowNumber)
        columnNumber = -1
        seenFixed = False

        for sourceColumnNumber, content in enumerate(self.rawData):
            colSpec = self.tableSpec.colSpecs[sourceColumnNumber]

            if not colSpec.column.hxlTag:
                continue

            if colSpec.fixedColumn:
                if not seenFixed:
                    ++columnNumber
                    fixedPosition = self.tableSpec.getFixedPosition(self.disaggregationPosition)
                    row.append(HXLValue(
                            self.tableSpec.colSpecs[fixedPosition].fixedColumn,
                            self.tableSpec.colSpecs[fixedPosition].fixedValue,
                            columnNumber,
                            sourceColumnNumber
                            ))
                    ++columnNumber
                    row.append(HXLValue(
                            self.tableSpec.colSpecs[fixedPosition].column,
                            self.rawData[fixedPosition],
                            columnNumber,
                            sourceColumnNumber
                            ))
                    seenFixed = True
                else:
                    ++columnNumber
                    row.append(HXLValue(
                            self.tableSpec.colSpecs[sourceColumnNumber].column,
                            self.rawData[sourceColumnNumber],
                            columnNumber,
                            sourceColumnNumber
                            ))

        ++self.disaggregationPosition
        return row

    def parseTableSpec(self):
        """
        Go fishing for the HXL hashtag row.
        Returns a HXLTableSpec on success. Throws an exception on failure.
        """
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
        """
        Try parsing the current raw CSV data row as a HXL hashtag row.
        Returns a HXLTableSpec on success, or None on failure
        """
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
                colSpec = HXLColSpec(sourceColumnNumber)
                colSpec.column = HXLColumn()
            tableSpec.append(colSpec)
            ++sourceColumnNumber

        if seenHeader:
            return tableSpec
        else:
            return None

    def parseHashtag(self, sourceColumnNumber, rawString):
        """
        Attempt to parse a full hashtag specification.
        May include compact disaggregated syntax
        Returns a colspec or None
        """

        # Pattern for a single tag
        tagRegexp = '(#[a-zA-z0-9_]+)(?:\/([a-zA-Z]{2}))?'

        # Pattern for full tag spec (optional second tag following '+')
        fullRegexp = '^\s*' + tagRegexp + '(?:\s*\+\s*' + tagRegexp + ')?$';

        # Try a match
        result = re.match(fullRegexp, rawString)
        if result:
            col1 = HXLColumn(result.group(1), result.group(2))
            col2 = None

            if result.group(3):
                # There were two tags
                col2 = HXLColumn(result.group(3), result.group(4))
                colSpec = HXLColSpec(sourceColumnNumber, col2, col1)
            else:
                # There was just one tag
                colSpec = HXLColSpec(sourceColumnNumber, col1)
            return colSpec
        else:
            return None

    def parseSourceRow(self):
        """
        Parse a row of raw CSV data.
        Returns an array of strings.
        """
        ++self.sourceRowNumber
        return self.csvreader.next()

    def prettyTag(self, hxlTag):
        hxlTag = re.sub('^#', '', hxlTag)
        hxlTag = re.sub('_(date|deg|id|link|num)$', '', hxlTag)
        hxlTag = re.sub('_', ' ', hxlTag)
        return hxlTag.capitalize()
