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
import sys
import re
import xlrd
import six

if sys.version_info < (3,):
    import urllib2
else:
    import urllib.request

from hxl.common import HXLException
from hxl.model import Dataset, Column, Row

# Cut off for fuzzy detection of a hashtag row
# At least this percentage of cells must parse as HXL hashtags
FUZZY_HASHTAG_PERCENTAGE = 0.5

if sys.version_info < (3,):
    def encode(value):
        """Encode a string into UTF-8 for Python2"""
        try:
            return value.encode('utf-8')
        except:
            return value
else:
    def encode(value):
        """Leave a Unicode string as-is for Python3"""
        return value


def make_input(data, allow_local=False):
    """Figure out what kind of input to create."""

    if isinstance(data, AbstractInput):
        return data

    elif hasattr(data, 'read'):
        # it's a file stream
        return StreamInput(data)

    elif hasattr(data, '__len__') and (not isinstance(data, six.string_types)):
        # it's an array
        return ArrayInput(data)

    elif re.match(r'\.xlsx?', data):
        return ExcelInput(data, allow_local)
    
    else:
        return CSVInput(data, allow_local)


def make_stream(origin, allow_local=False):
    """Figure out whether to open a file or a URL."""

    # Pre-filter to get CSV for public Google Sheets
    result = re.match(r'^https?://docs.google.com/.*spreadsheets.*([0-9A-Za-z_-]{44})(?:.*gid=([0-9]+)).*$', origin)
    if result:
        if result.group(2):
            origin = 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv&gid={1}'.format(result.group(1), result.group(2))
        else:
            origin = 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv'.format(result.group(1))

    if re.match(r'^(?:https?|ftp)://', origin):
        if sys.version_info < (3,):
            return urllib2.urlopen(origin)
        else:
            return urllib.request.urlopen(origin)

    elif allow_local:
        return open(origin, 'rt')

    else:
        raise IOError('Only http(s) and ftp URLs allowed.')


def hxl(data, allow_local=False):
    """
    Convenience method for reading a HXL dataset.
    If passed an existing Dataset, simply returns it.
    @param data a HXL data provider, file object, array, or string (representing a URL or file name).
    """

    if isinstance(data, Dataset):
        # it's already HXL data
        return data

    else:
        return HXLReader(make_input(data, allow_local))


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


class CSVInput(AbstractInput):
    """Read raw CSV input from a URL or filename."""

    def __init__(self, origin, allow_local=False):
        self._input = make_stream(origin, allow_local)
        self._reader = csv.reader(self._input)

    def __next__(self):
        return next(self._reader)

    next = __next__

    def __exit__(self, value, type, traceback):
        self._input.close()


class ExcelInput(AbstractInput):
    """Read raw XLS input from a URL or filename."""

    def __init__(self, url, sheet_index=0, allow_local=False):
        try:
            input = make_stream(url, allow_local)
            self._workbook = xlrd.open_workbook(file_contents=input.read())
        finally:
            input.close()
        self._sheet = self._workbook.sheet_by_index(sheet_index)
        self._row_index = 0

    def __next__(self):
        if self._row_index < self._sheet.nrows:
            row = [encode(cell.value) for cell in self._sheet.row(self._row_index)]
            self._row_index += 1
            return row
        else:
            raise StopIteration()

    next = __next__

    def __exit__(self, value, type, traceback):
        pass


class ArrayInput(AbstractInput):
    """Read raw input from an array."""

    def __init__(self, data):
        self._iter = iter(data)

    def __next__(self):
        return next(self._iter)

    next = __next__


class HXLReader(Dataset):
    """Read HXL data from a file

    This class acts as both an iterator and a context manager. If
    you're planning to pass a url or filename via the constructor's
    url parameter, it's best to use it in a Python with statement to
    make sure that the file gets closed again.

    """

    def __init__(self, input):
        """Constructor

        The order of preference is to use raw_data if supplied; then
        fall back to input (an already-open file object); then fall
        back to opening the resource specified by url (URL or
        filename) if all else fails. In the last case, the object can
        serve as a context manager, and will close the opened file
        resource in its __exit__ method.

        @param input a Python file object.
        @param raw_data an iterable over a series of string arrays.
        @param url the URL or filename to open.

        """
        self._input = input
        self._columns = None
        self._source_row_number = -1
        self._row_number = -1
        self._raw_data = None
        self._used_iter = False

    @property
    def columns(self):
        """
        Return a list of Column objects.
        """
        if self._columns is None:
            self._columns = self._find_tags()
        return self._columns

    def __iter__(self):
        if self._used_iter:
            raise HXLException("Cannot read a stream twice")
        else:
            return self

    def __next__(self):
        """
        Iterable function to return the next row of HXL values.
        Returns a Row, or raises StopIteration exception at end
        """
        columns = self.columns
        values = self._get_row()
        self._row_number += 1
        return Row(columns=columns, values=values, row_number=self._row_number)

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

        for source_column_number, raw_string in enumerate(raw_row):
            if source_column_number < len(previous_row):
                header = previous_row[source_column_number]
            else:
                header = None
            if raw_string:
                raw_string = raw_string.strip()
                nonEmptyCount += 1
                column = Column.parse(raw_string, header=header)
                if column:
                    columns.append(column)
                    column_number += 1
                    continue

            columns.append(Column(header=header))

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
        if self._input:
            self._input.__enter__()
        return self

    def __exit__(self, value, type, traceback):
        """Context-end support."""
        if self._input:
            self._input.__exit__(value, type, traceback)

def write_hxl(output, source, show_headers=True, show_tags=True):
    """Serialize a HXL dataset to an output stream."""
    for line in source.gen_csv(show_headers, show_tags):
        output.write(line)

def write_json(output, source, show_headers=True, show_tags=True):
    """Serialize a dataset to JSON."""
    for line in source.gen_json(show_headers, show_tags):
        output.write(line)

# end




