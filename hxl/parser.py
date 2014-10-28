"""
Parsing library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import csv
import re

from model import HXLColumn, HXLRow

class HXLParseException(Exception):
    """
    A parsing error in a HXL dataset.
    """

    def __init__(self, message, sourceRowNumber = -1, sourceColumnNumber = -1):
        super(Exception, self).__init__(message)
        self.sourceRowNumber = sourceRowNumber
        self.sourceColumnNumber = sourceColumnNumber

    def __str__(self):
        return '<HXLException: ' + str(self.message) + ' @ ' + str(self.sourceRowNumber) + ', ' + str(self.sourceColumnNumber) + '>'

class HXLTableSpec:
    """
    Table metadata for parsing a HXL dataset
    
    This class also contains logic for translating between raw source
    columns and logical output columns (taking into account untagged
    columns and compact-disaggregated syntax. It also caches
    calculated values for later reuse.
    """

    def __init__(self):
        self.colSpecs = []
        self.resetCache()

    def append(self, colSpec):
        """
        Append a new HXLColSpec to the table spec
        """
        self.colSpecs.append(colSpec)
        self.resetCache()

    @property
    def columns(self):
        """
        Fairly-complex function to extract logical columns from virtual ones:

        - remove any column without a HXL hashtag
        - include the *first* compact disaggregated column, expanded to two
        - remove any other compact disaggregated columns

        Use self.cachedColumns to save the result, so that it has to run only
        once, no matter how many times it's called.
        """
        if self.cachedColumns == None:
            self.cachedColumns = []
            seenFixed = False
            for colSpec in self.colSpecs:
                if not colSpec.column.hxlTag or (colSpec.fixedColumn and seenFixed):
                    continue
                if colSpec.fixedColumn:
                    self.cachedColumns.append(colSpec.fixedColumn)
                    seenFixed = True
                self.cachedColumns.append(colSpec.column)
        return self.cachedColumns

    @property
    def headers(self):
        """
        Get a simple list of header strings from the columns.
        """
        if self.cachedHeaders == None:
            self.cachedHeaders = map(lambda column: column.headerText, self.columns)
        return self.cachedHeaders

    @property
    def tags(self):
        """
        Get a simple list of HXL hashtags from the columns.
        """
        if self.cachedTags == None:
            self.cachedTags = map(lambda column: column.hxlTag, self.columns)
        return self.cachedTags

    @property
    def disaggregationCount(self):
        """
        Get the number of columns using compact disaggregated syntax
        
        They will all be merged into one logical column, with repeated rows
        for different values. Cache the result in self.cachedDisaggregationCount.
        """
        if self.cachedDisaggregationCount == None:
            self.cachedDisaggregationCount = 0;
            for colSpec in self.colSpecs:
                if colSpec.fixedColumn:
                    self.cachedDisaggregationCount += 1
        return self.cachedDisaggregationCount

    def getDisaggregatedSourceColumnNumber(self, disaggregationPosition):
        """
        For a logical sequence number in disaggregation, figure out the
        actual source column number.
        """
        for pos, colSpec in enumerate(self.colSpecs):
            if colSpec.fixedColumn:
                disaggregationPosition -= 1
            if disaggregationPosition < 0:
                return pos
        return -1

    def resetCache(self):
        """
        Reset all cached values so that they'll be regenerated.
        """
        self.cachedColumns = None
        self.cachedHeaders = None
        self.cachedTags = None
        self.cachedDisaggregationCount = None

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
        self.disaggregationPosition = 0

    def __iter__(self):
        return self;

    @property
    def headers(self):
        """
        Return a list of header strings (for a spreadsheet row).
        """
        self.setupTableSpec()
        return self.tableSpec.headers

    @property
    def tags(self):
        """
        Return a list of HXL hashtag strings (for a spreadsheet row).
        """
        self.setupTableSpec()
        return self.tableSpec.tags

    @property
    def hasHeaders(self):
        """
        Report whether any non-empty header strings exist.
        """
        for header in self.headers:
            if header:
                return True
        return False

    def next(self):
        """
        Iterable function to return the next row of HXL values.
        Returns a HXLRow, or raises StopIteration exception at end
        """

        # Won't do anything if it already exists
        self.setupTableSpec()

        # Read more raw data unless we're in the middle of generating virtual rows
        # from compact-disaggregated syntax
        if self.disaggregationPosition >= self.tableSpec.disaggregationCount or not self.rawData:
            self.rawData = self.parseSourceRow()
            if (self.rawData == None):
                return None
            self.disaggregationPosition = 0

        # Next logical row
        self.rowNumber += 1

        # The row we're going to populate
        row = HXLRow(self.tableSpec.columns, self.rowNumber, self.sourceRowNumber)

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
                    disaggregatedSourceColumnNumber = self.tableSpec.getDisaggregatedSourceColumnNumber(self.disaggregationPosition)
                    row.append(self.tableSpec.colSpecs[disaggregatedSourceColumnNumber].fixedValue)
                    columnNumber += 1
                    row.append(self.rawData[disaggregatedSourceColumnNumber])
                    seenFixed = True
                    self.disaggregationPosition += 1
            else:
                # regular column
                columnNumber += 1
                row.append(self.rawData[sourceColumnNumber])

        return row

    def setupTableSpec(self):
        """
        Go fishing for the HXL hashtag row.
        Returns a HXLTableSpec on success. Throws an exception on failure.
        """

        # If we already have it, return it
        if (self.tableSpec):
            return self.tableSpec

        # OK, need to go fishing ...
        try:
            rawData = self.parseSourceRow()
            while rawData:
                tableSpec = self.parseHashtagRow(rawData)
                if (tableSpec != None):
                    self.tableSpec = tableSpec
                    return self.tableSpec
                else:
                    self.lastHeaderRow = rawData
                    rawData = self.parseSourceRow()
        except StopIteration:
            raise HXLParseException("HXL hashtag row not found", self.sourceRowNumber)
    
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
        tagRegexp = '(#[a-zA-Z0-9_]+)(?:\/([a-zA-Z]{2}))?'

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
        """
        Hack a human-readable heading from a HXL tag name.
        """
        hxlTag = re.sub('^#', '', hxlTag)
        hxlTag = re.sub('_(date|deg|id|link|num)$', '', hxlTag)
        hxlTag = re.sub('_', ' ', hxlTag)
        return hxlTag.capitalize()

# end
