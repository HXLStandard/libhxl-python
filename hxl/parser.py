"""
Parsing library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import csv
import cgi
import json
import re
from .model import HXLDataProvider, HXLDataset, HXLColumn, HXLRow

class HXLParseException(Exception):
    """
    A parsing error in a HXL dataset.
    """

    def __init__(self, message, sourceRowNumber=None, sourceColumnNumber=None):
        super(Exception, self).__init__(message)
        self.sourceRowNumber = sourceRowNumber
        self.sourceColumnNumber = sourceColumnNumber

    def __str__(self):
        return '<HXLException: ' + str(self.message) + ' @ ' + str(self.sourceRowNumber) + ', ' + str(self.sourceColumnNumber) + '>'


class _TableSpec:
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
        Append a new _ColSpec to the table spec
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
            self.cachedHeaders = list(map(lambda column: column.headerText, self.columns))
        return self.cachedHeaders

    @property
    def tags(self):
        """
        Get a simple list of HXL hashtags from the columns.
        """
        if self.cachedTags == None:
            self.cachedTags = list(map(lambda column: column.hxlTag, self.columns))
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
        s = '<_TableSpec';
        for colSpec in self.colSpecs:
            s += "\n  " + re.sub("\n", "\n  ", str(colSpec));
        s += "\n>"
        return s


class _ColSpec:
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
        s = "<_ColSpec";
        s += "\n  column: " + str(self.column)
        if (self.fixedColumn):
            s += "\n  fixedColumn: " + str(self.fixedColumn)
            s += "\n  fixedValue: " + str(self.fixedValue)
        s += "\n  sourceColumnNumberumber: " + str(self.sourceColumnNumber)
        s += "\n>"
        return s


class HXLReader(HXLDataProvider):
    """
    Read HXL data from a file
    """

    def __init__(self, source):
        # all internal properties
        self._csv_reader = csv.reader(source)
        self._table_spec = None
        self._source_row_number = -1
        self._row_number = -1
        self._last_header_row = None
        self._raw_data = None
        self._disaggregation_pos = 0

    @property
    def columns(self):
        """
        Return a list of HXLColumn objects.
        """
        self._setup_table_spec()
        return self._table_spec.columns

    def __next__(self):
        """
        Iterable function to return the next row of HXL values.
        Returns a HXLRow, or raises StopIteration exception at end
        """

        # Won't do anything if it already exists
        self._setup_table_spec()

        # Read more raw data unless we're in the middle of generating virtual rows
        # from compact-disaggregated syntax
        if self._disaggregation_pos >= self._table_spec.disaggregationCount or not self._raw_data:
            self._raw_data = self._parse_source_row()
            if (self._raw_data == None):
                return None
            self._disaggregation_pos = 0

        # Next logical row
        self._row_number += 1

        # The row we're going to populate
        row = HXLRow(self._table_spec.columns, rowNumber=self._row_number, sourceRowNumber=self._source_row_number)

        columnNumber = -1
        seenFixed = False

        # Loop through the raw CSV data
        for sourceColumnNumber, content in enumerate(self._raw_data):

            if sourceColumnNumber >= len(self._table_spec.colSpecs):
                # no more hashtags
                break

            # grab the specification
            colSpec = self._table_spec.colSpecs[sourceColumnNumber]

            # We're reading only columns that have HXL tags
            if not colSpec.column.hxlTag:
                continue

            if colSpec.fixedColumn:
                # There's a fixed column involved
                if not seenFixed:
                    columnNumber += 1
                    disaggregatedSourceColumnNumber = self._table_spec.getDisaggregatedSourceColumnNumber(self._disaggregation_pos)
                    row.append(self._table_spec.colSpecs[disaggregatedSourceColumnNumber].fixedValue)
                    columnNumber += 1
                    row.append(self._raw_data[disaggregatedSourceColumnNumber])
                    seenFixed = True
                    self._disaggregation_pos += 1
            else:
                # regular column
                columnNumber += 1
                row.append(self._raw_data[sourceColumnNumber])

        return row

    next = __next__

    def _setup_table_spec(self):
        """
        Go fishing for the HXL hashtag row.
        Returns a _TableSpec on success. Throws an exception on failure.
        """

        # If we already have it, return it
        if (self._table_spec):
            return self._table_spec

        # OK, need to go fishing ...
        try:
            _raw_data = self._parse_source_row()
            while _raw_data is not None:
                _table_spec = self._parse_hashtag_row(_raw_data)
                if (_table_spec != None):
                    self._table_spec = _table_spec
                    return self._table_spec
                else:
                    self._last_header_row = _raw_data
                    _raw_data = self._parse_source_row()
        except StopIteration:
            pass
        raise HXLParseException("HXL hashtag row not found", self._source_row_number)
    
    def _parse_hashtag_row(self, rawDataRow):
        """
        Try parsing the current raw CSV data row as a HXL hashtag row.
        Returns a _TableSpec on success, or None on failure
        """
        _table_spec = _TableSpec()
        seenHeader = 0
        columnNumber = 0
        for sourceColumnNumber, rawString in enumerate(rawDataRow):
            rawString = rawString.strip()
            if rawString:
                colSpec = self._parse_hashtag(columnNumber,sourceColumnNumber, rawString)
                if (colSpec):
                    seenHeader = 1
                    if (colSpec.fixedColumn):
                        colSpec.fixedColumn.headerText = self._pretty_tag(colSpec.fixedColumn.hxlTag)
                        colSpec.column.headerText = self._pretty_tag(colSpec.column.hxlTag)
                        colSpec.fixedValue = self._last_header_row[sourceColumnNumber]
                        columnNumber += 1
                    else:
                        if self._last_header_row and sourceColumnNumber < len(self._last_header_row):
                            colSpec.column.headerText = self._last_header_row[sourceColumnNumber]
                else:
                    return None
            else:
                colSpec = _ColSpec(sourceColumnNumber)
                colSpec.column = HXLColumn(columnNumber, sourceColumnNumber)
            columnNumber += 1
            _table_spec.append(colSpec)

        if seenHeader:
            return _table_spec
        else:
            return None

    def _parse_hashtag(self, columnNumber, sourceColumnNumber, rawString):
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
                colSpec = _ColSpec(sourceColumnNumber, col2, col1)
            else:
                # There was just one tag
                colSpec = _ColSpec(sourceColumnNumber, col1)
            return colSpec
        else:
            return None

    def _parse_source_row(self):
        """
        Parse a row of raw CSV data.
        Returns an array of strings.
        """
        self._source_row_number += 1
        return next(self._csv_reader)

    def _pretty_tag(self, hxlTag):
        """
        Hack a human-readable heading from a HXL tag name.
        """
        hxlTag = re.sub('^#', '', hxlTag)
        hxlTag = re.sub('_(date|deg|id|link|num)$', '', hxlTag)
        hxlTag = re.sub('_', ' ', hxlTag)
        return hxlTag.capitalize()

def readHXL(input, url=None):
    """Load an in-memory HXL dataset."""
    dataset = HXLDataset(url)

    parser = HXLReader(input)
    dataset.columns = parser.columns
    for row in parser:
        dataset.rows.append(row)

    return dataset

def writeHXL(output, source, showHeaders=True):
    """Serialize a HXL dataset to an output stream."""
    for line in genHXL(source, showHeaders):
        output.write(line)

def writeJSON(output, source, showHeaders=True):
    """Serialize a dataset to JSON."""
    for line in genJSON(source, showHeaders):
        output.write(line)

def writeHTML(output, source, showHeaders=True):
    """Serialize a dataset to HTML."""
    for line in genHTML(source, showHeaders):
        output.write(line)

def genHXL(source, showHeaders=True):
    """
    Generate HXL output one row at a time.
    """
    class TextOut:
        """Simple string output source to capture CSV"""
        def __init__(self):
            self.data = ''
        def write(self, s):
            self.data += s
        def get(self):
            data = self.data
            self.data = ''
            return data
    output = TextOut()
    writer = csv.writer(output)
    if showHeaders and source.hasHeaders:
        writer.writerow(source.headers)
        yield output.get()
    writer.writerow(source.tags)
    yield output.get()
    for row in source:
        writer.writerow(row.values)
        yield output.get()

def genJSON(source, showHeaders=True):
    """
    Generate JSON output, one line at a time.
    """
    yield "{\n"
    if showHeaders and source.hasHeaders:
        yield "  \"headers\": " + json.dumps(source.headers) + ",\n"
    yield "  \"tags\": " + json.dumps(source.tags) + ",\n"
    yield "  \"data\": [\n"
    is_first = True
    for row in source:
        if is_first:
            yield "    "
            is_first = False
        else:
            yield ",\n    "
        yield json.dumps(row.values)
    yield "\n  ]\n"
    yield "}\n"

def genHTML(source, showHeaders=True):
    """
    Generate HTML output, one line at a time.
    """
    yield "<table class=\"hxl\">\n"
    yield "  <thead>\n"
    if (showHeaders and source.hasHeaders):
        yield "    <tr class=\"headers\">\n"
        for s in source.headers:
            yield "      <th>" + cgi.escape(s) + "</th>\n"
        yield "    </tr>\n"
    yield "    <tr class=\"tags\">\n"
    for s in source.tags:
        yield "      <th>" + cgi.escape(s) + "</th>\n"
    yield "    </tr>\n"
    yield "  </thead>\n"
    yield "  <tbody>\n"
    type = 'odd'
    for row in source:
        yield "    <tr class=\"data " + type + "\">\n"
        if type == 'odd':
            type = 'even'
        else:
            type = 'odd'
        for s in row:
            yield "      <td>" + cgi.escape(s) + "</td>\n"
        yield "    </tr>\n"
    yield "  </tbody>\n"
    yield "</table>\n"

# end




