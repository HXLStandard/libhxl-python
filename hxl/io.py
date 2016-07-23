"""
Input/output library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

from __future__ import absolute_import

import abc
import io
import csv
import json
import sys
import re
import xlrd
import six
import requests

import hxl


########################################################################
# Constants
########################################################################

# Cut off for fuzzy detection of a hashtag row
# At least this percentage of cells must parse as HXL hashtags
FUZZY_HASHTAG_PERCENTAGE = 0.5

# Patterns for URL munging
GOOGLE_SHEETS_URL = r'^https?://docs.google.com/.*spreadsheets.*([0-9A-Za-z_-]{44})(?:.*gid=([0-9]+))?.*$'
DROPBOX_URL = r'^https://www.dropbox.com/s/([0-9a-z]{15})/([^?]+)\?dl=[01]$'
CKAN_URL = r'^(https?://[^/]+)/dataset/([^/]+)/resource/([a-z0-9-]{36})$'

# opening signatures for well-known file types
EXCEL_SIGS = [
    b"PK\x03\x04",
    b"\xd0\xcf\x11\xe0"
]
HTML5_SIGS = [
    b"<!DO",
    b"\n<!D"
]


########################################################################
# Exported functions
########################################################################


def data(data, allow_local=False, sheet_index=None):
    """
    Convenience method for reading a HXL dataset.
    If passed an existing Dataset, simply returns it.
    @param data a HXL data provider, file object, array, or string (representing a URL or file name).
    """

    if isinstance(data, hxl.model.Dataset):
        # it's already HXL data
        return data

    else:
        return HXLReader(make_input(data, allow_local, sheet_index))

    
def write_hxl(output, source, show_headers=True, show_tags=True):
    """Serialize a HXL dataset to an output stream."""
    for line in source.gen_csv(show_headers, show_tags):
        output.write(line)

        
def write_json(output, source, show_headers=True, show_tags=True):
    """Serialize a dataset to JSON."""
    for line in source.gen_json(show_headers, show_tags):
        output.write(line)


def munge_url(url):
    """Munge a URL to get at underlying data for well-known types."""

    # Is it a Google URL?
    result = re.match(GOOGLE_SHEETS_URL, url)
    if result and not re.search(r'/pub', url):
        if result.group(2):
            return 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv&gid={1}'.format(result.group(1), result.group(2))
        else:
            return 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv'.format(result.group(1))

    # Is it a Dropbox URL?
    result = re.match(DROPBOX_URL, url)
    if result:
        return 'https://www.dropbox.com/s/{0}/{1}?dl=1'.format(result.group(1), result.group(2))

    # Is it a CKAN resource? (Assumes the v.3 API for now)
    result = re.match(CKAN_URL, url)
    if result:
        ckan_api_query = '{}/api/3/action/resource_show?id={}'.format(result.group(1), result.group(3))
        ckan_api_result = requests.get(ckan_api_query).json()
        return ckan_api_result['result']['url']

    # No changes
    return url


def _encode_py2(value):
    """Encode a string into UTF-8 for Python2"""
    try:
        return value.encode('utf-8')
    except:
        return value

    
def _encode_py3(value):
    """Leave a Unicode string as-is for Python3"""
    return value

if sys.version_info < (3,):
    encode = _encode_py2
else:
    encode = _encode_py3

    
def make_input(raw_source, allow_local=False, sheet_index=None):
    """Figure out what kind of input to create.

    Can detect a URL or filename, an input stream, or an array.
    Will also try to detect HTML and Excel before defaulting to CSV.
    The result is an object that can deliver rows of data for the HXL library to parse.

    @param raw_source: the raw data source (e.g. a URL or input stream).
    @param allow_local: if True, allow opening local files as well as remote URLs (default: False).
    @param sheet_index: if a number, read that sheet from an Excel workbook (default: None).
    @return: an object belonging to a subclass of AbstractInput, returning rows of raw data.
    """

    def wrap_stream(stream):
        if sys.version_info < (3,):
            # Extra work for Python 2.x
            if not hasattr(stream, 'readable'):
                import hxl.py2compat
                stream = hxl.py2compat.InputStreamWrapper(stream)
        if hasattr(stream, 'peek'):
            # already buffered
            return stream
        else:
            return io.BufferedReader(stream)

    if isinstance(raw_source, AbstractInput):
        # already an input source: no op
        return raw_source

    elif hasattr(raw_source, '__len__') and (not isinstance(raw_source, six.string_types)):
        # it's an array
        return ArrayInput(raw_source)

    else:
        if hasattr(raw_source, 'read'):
            # it's an input stream
            input = wrap_stream(raw_source)
        else:
            # assume a URL or filename
            input = wrap_stream(open_url_or_file(raw_source, allow_local=allow_local))

        sig = input.peek(4)[:4]
        if sig in HTML5_SIGS:
            raise hxl.common.HXLException(
                "Received HTML5 markup.\nCheck that the resource (e.g. a Google Sheet) is publicly readable.",
                {'input': input}
            )
        elif sig in EXCEL_SIGS:
            return ExcelInput(input, sheet_index=sheet_index)
        else:
            return CSVInput(input)


def open_url_or_file(url_or_filename, allow_local=False):
    """Try opening a local or remote resource.
    Allows only HTTP(S) and (S)FTP URLs.
    @param url_or_filename: the string to try openining.
    @param allow_local: if True, OK to open local files; otherwise, only remote URLs allowed (default: False).
    @return: an io stream.
    """
    if re.match(r'^(?:https?|s?ftp)://', url_or_filename):
        # It looks like a URL
        response = requests.get(munge_url(url_or_filename), stream=True)
        if response.status_code != 200:
            raise IOError('Received HTTP response code {}'.format(response.status_code))
        return RequestResponseIOWrapper(response)
    elif allow_local:
        # Default to a local file, if allowed
        return io.open(url_or_filename, 'rb')
    else:
        # Forbidden to trye local (allow_local is False), so give up.
        raise IOError("Only http(s) and (s)ftp URLs allowed: {}".format(url_or_filename))


########################################################################
# Exported classes
########################################################################

class HXLParseException(hxl.common.HXLException):
    """
    A parsing error in a HXL dataset.
    """

    def __init__(self, message, source_row_number=None, source_column_number=None):
        super(HXLParseException, self).__init__(message)
        self.source_row_number = source_row_number
        self.source_column_number = source_column_number


class HXLTagsNotFoundException(HXLParseException):
    """
    Specific parsing exception: no HXL tags.
    """

    def __init__(self, message='HXL tags not found in first 25 rows'):
        super(HXLTagsNotFoundException, self).__init__(message)


class RequestResponseIOWrapper(io.RawIOBase):
    """Wrapper for a Response object from the requests library.  Streaming
    in requests is a bit broken: for example, if you're streaming, the
    stream from the raw property doesn't unzip the payload.
    """

    _seen_decode_exception = False
    """Remember if the decode_content param failed for read()."""

    def __init__(self, response):
        self.response = response
        self.iter = response.iter_content(512)

    def read(self, size=-1):
        if not self._seen_decode_exception:
            # Try the decode_content param to handle gzipped content
            try:
                return self.response.raw.read(size, decode_content=True)
            except:
                self._see_decode_exception = True
        return self.response.raw.read(size)

    def readinto(self, b):
        # Can't use readinto, because implementation lacks decode_content
        result = self.read(len(b))
        byte_count = len(result)
        b[0:byte_count] = result[0:byte_count]
        return byte_count

    def readable(self):
        if hasattr(self.response.raw, 'readable'):
            return self.response.raw.readable()
        else:
            return True

    def close(self):
        return self.response.close()

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


class CSVInput(AbstractInput):
    """Read raw CSV input from a URL or filename."""

    def __init__(self, input):
        if sys.version_info < (3,):
            self._input = input
        else:
            if hasattr(input, 'response'):
                # Trick - if this is a wrapper, we can get at the response
                encoding = input.response.encoding
            else:
                encoding = 'utf-8'
            self._input = io.TextIOWrapper(input, encoding=encoding)
                
        self._reader = csv.reader(self._input)

    def __next__(self):
        return next(self._reader)

    next = __next__

    def __exit__(self, value, type, traceback):
        self._input.close()


class ExcelInput(AbstractInput):
    """
    Read raw XLS input from a URL or filename.
    If sheet number is not specified, will scan for the first tab with a HXL tag row.
    """

    def __init__(self, input, sheet_index=None):
        """
        Constructor
        @param url the URL or filename
        @param sheet_index (optional) the 0-based index of the sheet (if unspecified, scan)
        @param allow_local (optional) iff True, allow opening local files
        """
        try:
            self._workbook = xlrd.open_workbook(file_contents=input.read())
        finally:
            input.close()
        if sheet_index is None:
            sheet_index = self._find_hxl_sheet_index()
        self._sheet = self._workbook.sheet_by_index(sheet_index)
        self._row_index = 0

    def __next__(self):
        if self._row_index < self._sheet.nrows:
            row = [self._fix_value(cell) for cell in self._sheet.row(self._row_index)]
            self._row_index += 1
            return row
        else:
            raise StopIteration()

    next = __next__

    def __exit__(self, value, type, traceback):
        pass

    def _fix_value(self, cell):
        if not cell.value:
            return ''
        elif cell.ctype == 3: # FIXME - use constant
            data = xlrd.xldate_as_tuple(cell.value, 0)
            return '{0[0]:04d}-{0[1]:02d}-{0[2]:02d}'.format(data)
        else:
            if sys.version_info < (3,):
                try:
                    return cell.value.encode('utf8')
                except:
                    return cell.value
            else:
                return cell.value

    def _find_hxl_sheet_index(self):
        """Scan for a tab containing a HXL dataset."""
        for sheet_index in range(0, self._workbook.nsheets):
            sheet = self._workbook.sheet_by_index(sheet_index)
            for row_index in range(0, min(25, sheet.nrows)):
                raw_row = [self._fix_value(cell) for cell in sheet.row(row_index)]
                # FIXME nasty violation of encapsulation
                if HXLReader.parse_tags(raw_row):
                    return sheet_index
        # if no sheet has tags, default to the first one for now
        return 0


class ArrayInput(AbstractInput):
    """Read raw input from an array."""

    def __init__(self, data):
        self._iter = iter(data)

    def __next__(self):
        return next(self._iter)

    next = __next__


class HXLReader(hxl.model.Dataset):
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
            raise hxl.common.HXLException("Cannot read a stream twice")
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
        return hxl.model.Row(columns=columns, values=values, row_number=self._row_number)

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
                columns = self.parse_tags(raw_row, previous_row)
                if columns is not None:
                    return columns
                previous_row = raw_row
        except StopIteration:
            pass
        raise HXLTagsNotFoundException()

    @staticmethod
    def parse_tags(raw_row, previous_row=None):
        """
        Try parsing a raw CSV data row as a HXL hashtag row.
        """
        # how many values we've seen
        nonEmptyCount = 0

        # the logical column number
        column_number = 0

        columns = []

        for source_column_number, raw_string in enumerate(raw_row):
            if previous_row and source_column_number < len(previous_row):
                header = previous_row[source_column_number]
            else:
                header = None
            if raw_string:
                raw_string = str(raw_string).strip()
                nonEmptyCount += 1
                column = hxl.model.Column.parse(raw_string, header=header)
                if column:
                    columns.append(column)
                    column_number += 1
                    continue

            columns.append(hxl.model.Column(header=header))

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

# end
