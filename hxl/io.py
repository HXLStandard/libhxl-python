"""
Input/output library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import abc
import csv
import json
import re
import sys
if sys.version_info < (3,):
    import urllib
else:
    import urllib.request
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

class AbstractInput(object):
    """Abstract base class for input classes."""

    __metaclass__ = abc.ABCMeta

    def __iter__(self):
        return self

    @abc.abstractmethod
    def __next__(self):
        return

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        pass


class StreamInput(AbstractInput):
    """Read raw input from a file object."""

    def __init__(self, input):
        self._reader = csv.reader(input)

    def __next__(self):
        return next(self._reader)

    next = __next__


class URLInput(AbstractInput):
    """Read raw input from a URL or filename."""

    def __init__(self, url):
        if sys.version_info < (3,):
            self._input = urllib.urlopen(url)
        else:
            try:
                self._input = urllib.request.urlopen(url)
            except:
                # kludge for local files
                self._input = open(url, 'r')
        self._reader = csv.reader(self._input)

    def __next__(self):
        return next(self._reader)

    next = __next__

    def __exit__(self, value, type, traceback):
        self._input.close()


class ArrayInput(AbstractInput):
    """Read raw input from an array."""

    def __init__(self, data):
        self._reader = csv.reader(data)

    def __next__(self):
        return next(self._reader)

    next = __next__


class HXLReader(HXLDataProvider):
    """Read HXL data from a file

    This class acts as both an iterator and a context manager. If
    you're planning to pass a url or filename via the constructor's
    url parameter, it's best to use it in a Python with statement to
    make sure that the file gets closed again.

    """

    def __init__(self, input):
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
        
        self._input = input
        self._columns = None
        self._source_row_number = -1
        self._row_number = -1
        self._raw_data = None

    @property
    def columns(self):
        """
        Return a list of HXLColumn objects.
        """
        if self._columns is None:
            self._columns = self._find_tags()
        return self._columns

    def __next__(self):
        """
        Iterable function to return the next row of HXL values.
        Returns a HXLRow, or raises StopIteration exception at end
        """
        columns = self.columns
        values = self._get_row()
        self._row_number += 1
        return HXLRow(columns=columns, values=values, source_row_number=self._source_row_number, row_number=self._row_number)

    # for compatibility
    next = __next__

    def _find_tags(self):
        """
        Go fishing for the HXL hashtag row in the first 25 rows.
        """
        previous_row = []
        try:
            for n in range(0,25):
                raw_row = self._get_row()
                columns = self._parse_tags(raw_row, previous_row)
                if columns is not None:
                    return columns
                previous_row = raw_row
        except StopIteration:
            pass
        raise HXLParseException("HXL hashtags not found in first 25 rows")
    
    def _parse_tags(self, raw_row, previous_row):
        """
        Try parsing the current raw CSV data row as a HXL hashtag row.
        """
        # how many values we've seen
        nonEmptyCount = 0

        # the logical column number
        column_number = 0

        columns = []

        for source_column_number, rawString in enumerate(raw_row):
            rawString = rawString.strip()
            if source_column_number < len(previous_row):
                header = previous_row[source_column_number]
            else:
                header = None
            if rawString:
                nonEmptyCount += 1
                column = HXLColumn.parse(rawString, column_number=column_number, source_column_number=source_column_number, header=header)
                if column:
                    columns.append(column)
                    column_number += 1
            else:
                columns.append(HXLColumn(column_number, source_column_number, header=header))

        # Have we seen at least FUZZY_HASHTAG_PERCENTAGE?
        if (column_number/float(max(nonEmptyCount, 1))) >= FUZZY_HASHTAG_PERCENTAGE:
            return columns
        else:
            return None

    def _get_row(self):
        """Parse a row of raw CSV data.  Returns an array of strings."""
        self._source_row_number += 1
        return next(self._input)

    def __enter__(self):
        """Context-start support."""
        return self

    def __exit__(self):
        """Context-end support."""
        if self._opened_input:
            self._opened_input.close()

def readHXL(input):
    """Load an in-memory HXL dataset.

    At least one of input, url, and rawData must be provided. Order of
    preference is as with HXLReader.

    @param input a Python file object
    @param url a URL or filename to open
    @param rawData an iterator over a sequence of string arrays.
    @return an in-memory HXLDataset

    """
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
    writer.writerow(source.displayTags)
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




