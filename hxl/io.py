"""
Input/output library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import csv
import json
import re
import urllib
from . import HXLException
from .model import HXLDataProvider, HXLDataset, HXLColumn, HXLRow

# Cut off for fuzzy detection of a hashtag row
# At least this percentage of cells must parse as HXL hashtags
FUZZY_HASHTAG_PERCENTAGE = 0.5

class HXLParseException(HXLException):
    """
    A parsing error in a HXL dataset.
    """

    def __init__(self, message, source_row_number=None, source_column_number=None):
        super(HXLParseException, self).__init__(message)
        self.source_row_number = source_row_number
        self.source_column_number = source_column_number

    def __str__(self):
        return '<HXLException: ' + str(self.message) + ' @ ' + str(self.source_row_number) + ', ' + str(self.source_column_number) + '>'


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
                if not colSpec.column.tag or (colSpec.fixedColumn and seenFixed):
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
            self.cachedHeaders = list(map(lambda column: column.header, self.columns))
        return self.cachedHeaders

    @property
    def tags(self):
        """
        Get a simple list of HXL hashtags from the columns.
        """
        if self.cachedTags == None:
            self.cachedTags = list(map(lambda column: column.tag, self.columns))
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

    def __init__(self, source_column_number, column=None, fixedColumn=None, fixedValue=None):
        self.source_column_number = source_column_number
        self.column = column
        self.fixedColumn = fixedColumn
        self.fixedValue = fixedValue

    def __str__(self):
        s = "<_ColSpec";
        s += "\n  column: " + str(self.column)
        if (self.fixedColumn):
            s += "\n  fixedColumn: " + str(self.fixedColumn)
            s += "\n  fixedValue: " + str(self.fixedValue)
        s += "\n  source_column_numberumber: " + str(self.source_column_number)
        s += "\n>"
        return s


class HXLReader(HXLDataProvider):
    """Read HXL data from a file

    This class acts as both an iterator and a context manager. If
    you're planning to pass a url or filename via the constructor's
    url parameter, it's best to use it in a Python with statement to
    make sure that the file gets closed again.

    """

    def __init__(self, input=None, url=None, rawData=None):
        """Constructor

        The order of preference is to use rawData if supplied; then
        fall back to input (an already-open file object); then fall
        back to opening the resource specified by url (URL or
        filename) if all else fails. In the last case, the object can
        serve as a context manager, and will close the opened file
        resource in its __exit__ method.

        @param input a Python file object.
        @param rawData an iterable over a series of string arrays.
        @param url the URL or filename to open.

        """

        if input is None and url is None and rawData is None:
            raise HXLException("At least one of rawData, input, or url must be supplied.")

        self._opened_file = None
        if rawData is None:
            if not input:
                input = self._opened_file = urllib.urlopen(url, 'r')
            self.rawData = csv.reader(input)
        else:
            self.rawData = rawData
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
        row = HXLRow(self._table_spec.columns, row_number=self._row_number, source_row_number=self._source_row_number)

        column_number = -1
        seenFixed = False

        # Loop through the raw CSV data
        for source_column_number, content in enumerate(self._raw_data):

            if source_column_number >= len(self._table_spec.colSpecs):
                # no more hashtags
                break

            # grab the specification
            colSpec = self._table_spec.colSpecs[source_column_number]

            # We're reading only columns that have HXL tags
            if not colSpec.column.tag:
                continue

            if colSpec.fixedColumn:
                # There's a fixed column involved
                if not seenFixed:
                    column_number += 1
                    disaggregatedSourceColumnNumber = self._table_spec.getDisaggregatedSourceColumnNumber(self._disaggregation_pos)
                    row.append(self._table_spec.colSpecs[disaggregatedSourceColumnNumber].fixedValue)
                    column_number += 1
                    row.append(self._raw_data[disaggregatedSourceColumnNumber])
                    seenFixed = True
                    self._disaggregation_pos += 1
            else:
                # regular column
                column_number += 1
                row.append(self._raw_data[source_column_number])

        return row

    next = __next__

    def _setup_table_spec(self):
        """
        Go fishing for the HXL hashtag row in the first 25 rows.
        Returns a _TableSpec on success. Throws an exception on failure.
        """

        # If we already have it, return it
        if (self._table_spec):
            return self._table_spec

        # OK, need to go fishing ...
        try:
            _raw_data = self._parse_source_row()
            for n in range(0,25):
                if _raw_data is None:
                    break;
                _table_spec = self._parse_hashtag_row(_raw_data)
                if (_table_spec != None):
                    self._table_spec = _table_spec
                    return self._table_spec
                else:
                    self._last_header_row = _raw_data
                    _raw_data = self._parse_source_row()
        except StopIteration:
            pass
        raise HXLParseException("HXL hashtags not found in first 25 rows")
    
    def _parse_hashtag_row(self, rawDataRow):
        """
        Try parsing the current raw CSV data row as a HXL hashtag row.

        Go fuzzy here. If over half of the non-empty cells start with '#', assume that it is meant to be a hashtag row.
        Ref: Postel's Principle.

        @param rawDataRow A raw CSV row as an array.
        @return a _TableSpec on success, or None on failure
        """
        _table_spec = _TableSpec()

        # how many tags we've seen
        tagCount = 0
        nonEmptyCount = 0

        # the logical column number
        column_number = 0

        for source_column_number, rawString in enumerate(rawDataRow):
            # Iterate through the array of raw cell values
            colSpec = None
            rawString = rawString.strip()
            if rawString:
                # If the cell isn't empty, then it should hold a hashtag ...
                colSpec = self._parse_hashtag(column_number,source_column_number, rawString)
                nonEmptyCount += 1
                if (colSpec):
                    # we've seen a tag
                    tagCount += 1
                    if (colSpec.fixedColumn):
                        # special case: compact-disaggregated syntax
                        colSpec.fixedColumn.header = self._pretty_tag(colSpec.fixedColumn.tag)
                        colSpec.column.header = self._pretty_tag(colSpec.column.tag)
                        colSpec.fixedValue = self._last_header_row[source_column_number]
                        column_number += 1
                    else:
                        # normal case
                        if self._last_header_row and source_column_number < len(self._last_header_row):
                            colSpec.column.header = self._last_header_row[source_column_number]

            if colSpec is None:
                # either empty or not a HXL hashtag
                colSpec = _ColSpec(source_column_number)
                colSpec.column = HXLColumn(column_number, source_column_number)

            column_number += 1
            _table_spec.append(colSpec)

        # Have we seen at least FUZZY_HASHTAG_PERCENTAGE
        if (tagCount/float(max(nonEmptyCount, 1))) >= FUZZY_HASHTAG_PERCENTAGE:
            return _table_spec
        else:
            return None

    def _parse_hashtag(self, column_number, source_column_number, rawString):
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
            col1 = HXLColumn(column_number, source_column_number, result.group(1), result.group(2))
            col2 = None

            if result.group(3):
                # There were two tags
                col2 = HXLColumn(column_number, source_column_number, result.group(3), result.group(4))
                colSpec = _ColSpec(source_column_number, col2, col1)
            else:
                # There was just one tag
                colSpec = _ColSpec(source_column_number, col1)
            return colSpec
        else:
            return None

    def _parse_source_row(self):
        """Parse a row of raw CSV data.  Returns an array of strings."""
        self._source_row_number += 1
        return next(self.rawData)

    def _pretty_tag(self, tag):
        """Hack a human-readable heading from a HXL tag name.

        @param tag the HXL hashtag name 
        @return A hacked-up presentation string.

        """
        tag = re.sub('^#', '', tag)
        tag = re.sub('_(date|deg|id|link|num)$', '', tag)
        tag = re.sub('_', ' ', tag)
        return tag.capitalize()

    def __enter__(self):
        """Context-start support."""
        return self

    def __exit__(self):
        """Context-end support."""
        if self._opened_input:
            self._opened_input.close()

def readHXL(input=None, url=None, rawData=None):
    """Load an in-memory HXL dataset.

    At least one of input, url, and rawData must be provided. Order of
    preference is as with HXLReader.

    @param input a Python file object
    @param url a URL or filename to open
    @param rawData an iterator over a sequence of string arrays.
    @return an in-memory HXLDataset

    """
    dataset = HXLDataset(url)

    parser = HXLReader(input, url, rawData)
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

# end




