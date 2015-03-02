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

        self._columns = None
        self._source_row_number = -1
        self._row_number = -1
        self._last_header_row = None
        self._raw_data = None

    @property
    def columns(self):
        """
        Return a list of HXLColumn objects.
        """
        if self._columns is None:
            self._columns = self._setup_table_spec()
        return self._columns

    def __next__(self):
        """
        Iterable function to return the next row of HXL values.
        Returns a HXLRow, or raises StopIteration exception at end
        """
        columns = self.columns
        values = self._parse_source_row()
        self._row_number += 1
        return HXLRow(columns=columns, values=values, source_row_number=self._source_row_number, row_number=self._row_number)

    next = __next__

    def _setup_table_spec(self):
        """
        Go fishing for the HXL hashtag row in the first 25 rows.
        Returns a _TableSpec on success. Throws an exception on failure.
        """
        try:
            for n in range(0,25):
                values = self._parse_source_row()
                columns = self._parse_hashtag_row(values)
                if columns is not None:
                    return columns
        except StopIteration:
            pass
        raise HXLParseException("HXL hashtags not found in first 25 rows")
    
    def _parse_hashtag_row(self, rawDataRow):
        """
        Try parsing the current raw CSV data row as a HXL hashtag row.
        """
        # how many values we've seen
        nonEmptyCount = 0

        # the logical column number
        column_number = 0

        columns = []

        for source_column_number, rawString in enumerate(rawDataRow):
            rawString = rawString.strip()
            if rawString:
                nonEmptyCount += 1
                column = self._parse_hashtag(column_number, source_column_number, rawString)
                if column:
                    columns.append(column)
                    column_number += 1

        # Have we seen at least FUZZY_HASHTAG_PERCENTAGE?
        if (column_number/float(max(nonEmptyCount, 1))) >= FUZZY_HASHTAG_PERCENTAGE:
            return columns
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
            # FIXME - support old compact-disaggregated, plus new attributes
            return HXLColumn(column_number, source_column_number, result.group(1), result.group(2))

    def _parse_source_row(self):
        """Parse a row of raw CSV data.  Returns an array of strings."""
        self._source_row_number += 1
        return next(self.rawData)

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




