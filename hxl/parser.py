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
                n += 1
        return n

    def getRawPosition(self, disaggregationPosition):
        for pos, colSpec in enumerate(self.colSpecs):
            if colSpec.fixedColumn:
                disaggregationPosition -= 1
            if disaggregationPosition < 0:
                return pos
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
        s += "\n  column: " + str(self.column)
        if (self.fixedColumn):
            s += "\n  fixedColumn: " + str(self.fixedColumn)
            s += "\n  fixedValue: " + str(self.fixedValue)
        s += "\n  sourceColumnNumberumber: " + str(self.sourceColumnNumber)
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
        self.cachedHeaders = []
        self.cachedTags = []
        self.disaggregationPosition = 0

    def __iter__(self):
        return self;

    @property
    def headers(self):
        """
        Property function to get the row of HXL headers.
        FIXME ridiculously over-complicated, due to the initial design
        """
        if (self.cachedHeaders):
            return self.cachedHeaders
        self.parseTableSpec()
        seenFixed = False
        for colSpec in self.tableSpec.colSpecs:
            if colSpec.column.hxlTag:
                if colSpec.fixedColumn and seenFixed:
                    continue
                if colSpec.fixedColumn:
                    self.cachedHeaders.append(colSpec.fixedColumn.headerText)
                    seenFixed = True
                self.cachedHeaders.append(colSpec.column.headerText)
        return self.cachedHeaders

    @property
    def tags(self):
        """
        Property function to get the row of HXL tags.
        FIXME ridiculously over-complicated, due to the initial design
        """
        if (self.cachedTags):
            return self.cachedTags
        self.parseTableSpec()
        seenFixed = False
        for colSpec in self.tableSpec.colSpecs:
            if colSpec.column.hxlTag:
                if colSpec.fixedColumn and seenFixed:
                    continue
                if colSpec.fixedColumn:
                    self.cachedTags.append(colSpec.fixedColumn.hxlTag)
                    seenFixed = True
                self.cachedTags.append(colSpec.column.hxlTag)
        return self.cachedTags

    def next(self):
        """
        Iterable function to return the next row of HXL values.
        Returns a HXLRow, or raises StopIteration exception at end
        """

        # Won't do anything if it already exists
        self.parseTableSpec()

        # Read more raw data unless we're in the middle of generating virtual rows
        # from compact-disaggregated syntax
        if self.disaggregationPosition >= self.tableSpec.getDisaggregationCount() or not self.rawData:
            self.rawData = self.parseSourceRow()
            if (self.rawData == None):
                return None
            self.disaggregationPosition = 0

        # Next logical row
        self.rowNumber += 1

        # The row we're going to populate
        row = HXLRow(self.rowNumber, self.sourceRowNumber)

        columnNumber = -1
        seenFixed = False

        # Loop through the raw CSV data
        for sourceColumnNumber, content in enumerate(self.rawData):

            # grab the specificationf o
            colSpec = self.tableSpec.colSpecs[sourceColumnNumber]

            # We're reading only columns that have HXL tags
            if not colSpec.column.hxlTag:
                continue

            if colSpec.fixedColumn:
                # There's a fixed column involved
                if not seenFixed:
                    columnNumber += 1
                    rawPosition = self.tableSpec.getRawPosition(self.disaggregationPosition)
                    row.append(HXLValue(
                            self.tableSpec.colSpecs[rawPosition].fixedColumn,
                            self.tableSpec.colSpecs[rawPosition].fixedValue,
                            columnNumber,
                            sourceColumnNumber
                            ))
                    columnNumber += 1
                    row.append(HXLValue(
                            self.tableSpec.colSpecs[rawPosition].column,
                            self.rawData[rawPosition],
                            columnNumber,
                            sourceColumnNumber
                            ))
                    seenFixed = True
                    self.disaggregationPosition += 1
            else:
                # regular column
                columnNumber += 1
                row.append(HXLValue(
                        self.tableSpec.colSpecs[sourceColumnNumber].column,
                        self.rawData[sourceColumnNumber],
                        columnNumber,
                        sourceColumnNumber
                        ))

        return row

    def parseTableSpec(self):
        """
        Go fishing for the HXL hashtag row.
        Returns a HXLTableSpec on success. Throws an exception on failure.
        """

        # If we already have it, return it
        if (self.tableSpec):
            return self.tableSpec

        # OK, need to go fishing ...
        rawData = self.parseSourceRow()
        while rawData:
            tableSpec = self.parseHashtagRow(rawData)
            if (tableSpec != None):
                self.tableSpec = tableSpec
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
        columnNumber = 0
        for sourceColumnNumber, rawString in enumerate(rawDataRow):
            rawString = rawString.strip()
            if rawString:
                colSpec = self.parseHashtag(columnNumber,sourceColumnNumber, rawString)
                if (colSpec):
                    seenHeader = 1
                    if (colSpec.fixedColumn):
                        colSpec.fixedColumn.headerText = self.prettyTag(colSpec.fixedColumn.hxlTag)
                        colSpec.column.headerText = self.prettyTag(colSpec.column.hxlTag)
                        colSpec.fixedValue = self.lastHeaderRow[sourceColumnNumber]
                        columnNumber += 1
                    else:
                        colSpec.column.headerText = self.lastHeaderRow[sourceColumnNumber]
                else:
                    return None
            else:
                colSpec = HXLColSpec(sourceColumnNumber)
                colSpec.column = HXLColumn(columnNumber, sourceColumnNumber)
            columnNumber += 1
            tableSpec.append(colSpec)

        if seenHeader:
            return tableSpec
        else:
            return None

    def parseHashtag(self, columnNumber, sourceColumnNumber, rawString):
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
            col1 = HXLColumn(columnNumber, sourceColumnNumber, result.group(1), result.group(2))
            col2 = None

            if result.group(3):
                # There were two tags
                col2 = HXLColumn(columnNumber, sourceColumnNumber, result.group(3), result.group(4))
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
        self.sourceRowNumber += 1
        return self.csvreader.next()

    def prettyTag(self, hxlTag):
        hxlTag = re.sub('^#', '', hxlTag)
        hxlTag = re.sub('_(date|deg|id|link|num)$', '', hxlTag)
        hxlTag = re.sub('_', ' ', hxlTag)
        return hxlTag.capitalize()
