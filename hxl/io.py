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

JSON_MIME_TYPES = [
    'application/json'
]

JSON_FILE_EXTS = [
    'json'
]

EXCEL_MIME_TYPES = [
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
]

EXCEL_FILE_EXTS = [
    'xls',
    'xlsx'
]

EXCEL_SIGS = [
    b"PK\x03\x04",
    b"\xd0\xcf\x11\xe0"
]

HTML5_MIME_TYPES = [
    'text/html'
]

HTML5_SIGS = [
    b"<!DO",
    b"\n<!D"
]


########################################################################
# Exported functions
########################################################################


def data(data, allow_local=False, sheet_index=None, timeout=None, verify_ssl=True):
    """
    Convenience method for reading a HXL dataset.
    If passed an existing Dataset, simply returns it.
    @param data: a HXL data provider, file object, array, or string (representing a URL or file name).
    @param allow_local: if true, allow opening local filenames as well as remote URLs (default: False).
    @param sheet_index: if supplied, use the specified 1-based index to choose a sheet from an Excel workbook (default: None)
    @param timeout: if supplied, time out an HTTP(S) request after the specified number of seconds with no data received (default: None)
    """

    if isinstance(data, hxl.model.Dataset):
        # it's already HXL data
        return data

    elif isinstance(data, dict) and data.get('input'):
        """If it's a JSON-type spec, try parsing it."""
        return hxl.io.from_spec(data)

    else:
        return HXLReader(make_input(data, allow_local=allow_local, sheet_index=sheet_index, timeout=timeout, verify_ssl=True))

    
def tagger(data, specs, default_tag=None, match_all=False, allow_local=False, sheet_index=None, timeout=None, verify_ssl=True):
    """Open an untagged data source and add hashtags."""
    import hxl.converters
    return hxl.data(
        hxl.converters.Tagger(
            input=make_input(data, allow_local=allow_local, sheet_index=sheet_index, timeout=timeout, verify_ssl=verify_ssl),
            specs=specs,
            default_tag=default_tag,
            match_all=match_all
        )
    )

    
def write_hxl(output, source, show_headers=True, show_tags=True):
    """Serialize a HXL dataset to an output stream."""
    for line in source.gen_csv(show_headers, show_tags):
        output.write(line)

        
def write_json(output, source, show_headers=True, show_tags=True):
    """Serialize a dataset to JSON."""
    for line in source.gen_json(show_headers, show_tags):
        output.write(line)


def munge_url(url, verify_ssl=True):
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
        ckan_api_result = requests.get(ckan_api_query, verify=verify_ssl).json()
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

    
def make_input(raw_source, allow_local=False, sheet_index=None, timeout=None, verify_ssl=True):
    """Figure out what kind of input to create.

    Can detect a URL or filename, an input stream, or an array.
    Will also try to detect HTML and Excel before defaulting to CSV.
    The result is an object that can deliver rows of data for the HXL library to parse.

    @param raw_source: the raw data source (e.g. a URL or input stream).
    @param allow_local: if True, allow opening local files as well as remote URLs (default: False).
    @param sheet_index: if a number, read that sheet from an Excel workbook (default: None).
    @param timeout: if supplied, time out an HTTP(S) request after the specified number of seconds with no data received (default: None)
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
        mime_type = None
        file_ext = None
        encoding = None
        
        if hasattr(raw_source, 'read'):
            # it's an input stream
            input = wrap_stream(raw_source)
        else:
            # assume a URL or filename
            (input, mime_type, file_ext, encoding) = open_url_or_file(raw_source, allow_local=allow_local, timeout=timeout, verify_ssl=verify_ssl)
            input = wrap_stream(input)

        sig = input.peek(4)[:4]

        if (mime_type in HTML5_MIME_TYPES) or (sig in HTML5_SIGS):
            raise hxl.common.HXLException(
                "Received HTML5 markup.\nCheck that the resource (e.g. a Google Sheet) is publicly readable.",
                {'input': input}
            )

        elif (mime_type in EXCEL_MIME_TYPES) or (file_ext in EXCEL_FILE_EXTS) or (sig in EXCEL_SIGS):
            return ExcelInput(input, sheet_index=sheet_index)

        elif (mime_type in JSON_MIME_TYPES) or (file_ext in JSON_FILE_EXTS):
            return JSONInput(input)

        else:
            return CSVInput(input)


def open_url_or_file(url_or_filename, allow_local=False, timeout=None, verify_ssl=True):
    """Try opening a local or remote resource.
    Allows only HTTP(S) and (S)FTP URLs.
    @param url_or_filename: the string to try openining.
    @param allow_local: if True, OK to open local files; otherwise, only remote URLs allowed (default: False).
    @param timeout: if supplied, time out an HTTP(S) request after the specified number of seconds with no data received (default: None)
    @return: an io stream.
    """
    mime_type = None
    file_ext = None
    encoding = None

    # Try for file extension
    result = re.search(r'\.([A-Za-z0-9]{1,5})$', url_or_filename)
    if result:
        file_ext = result.group(1).lower()
    
    if re.match(r'^(?:https?|s?ftp)://', url_or_filename):
        # It looks like a URL
        response = requests.get(munge_url(url_or_filename, verify_ssl), stream=True, verify=verify_ssl, timeout=timeout)
        if response.status_code != 200:
            raise IOError('Received HTTP response code {}'.format(response.status_code))

        content_type = response.headers['Content-type']
        if content_type:
            result = re.match(r'^(\S+)\s*;\s*charset=(\S+)$', content_type)
            if result:
                mime_type = result.group(1).lower()
                encoding = result.group(2).lower()
            else:
                mime_type = content_type.lower()

        return (RequestResponseIOWrapper(response), mime_type, file_ext, encoding)

    elif allow_local:
        # Default to a local file, if allowed
        return (io.open(url_or_filename, 'rb'), mime_type, file_ext, encoding)

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

    BUFFER_SIZE = 0x1000
    """Size of input chunk buffer from requests.raw.iter_content"""

    def __init__(self, response):
        """Construct a wrapper around a requests response object
        @param response: the HTTP response from the requests library
        """
        self.response = response
        self.buffer = None
        self.buffer_pos = -1
        self.iter = response.iter_content(self.BUFFER_SIZE) # iterator through the input

    def read(self, size=-1):
        """Read raw byte input from the requests raw.iter_content iterator
        The function will unzip zipped content.
        @param size: the maximum number of bytes to read, or -1 for all available.
        """
        result = bytearray()

        if size == -1:
            # Read all of the content at once
            for chunk in self.iter:
                result += chunk
            self.buffer = None
        else:
            # Read from chunks until we have enough content
            while size > 0:
                if not self.buffer:
                    try:
                        self.buffer = next(self.iter)
                    except StopIteration:
                         # stop if we've run out of input
                        break
                    self.buffer_pos = 0
                avail = min(len(self.buffer)-self.buffer_pos, size) #actually read
                result += self.buffer[self.buffer_pos:self.buffer_pos+avail]
                size -= avail
                self.buffer_pos += avail
                if self.buffer_pos >= len(self.buffer):
                    self.buffer = None

        return bytes(result) # FIXME - how can we avoid a copy?

    def readinto(self, b):
        """Read content into a buffer of some kind.
        This is not very efficient right now -- too much copying.
        @param b: the buffer to read into (will read up to its length)
        """
        result = self.read(len(b))[:len(b)]
        size = len(result)
        b[:size] = result[:size]
        return size

    def readable(self):
        """Flag whether the content is readable."""
        if hasattr(self.response.raw, 'readable'):
            return self.response.raw.readable()
        else:
            return True

    @property
    def content_type(self):
        return self.response.headers.get('Content-type')

    def close(self):
        """Close the streaming response."""
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

    def __init__(self, input, encoding='utf-8'):
        if sys.version_info < (3,):
            self._input = input
        else:
            self._input = io.TextIOWrapper(input, encoding=encoding)
        self._reader = csv.reader(self._input)

    def __next__(self):
        return next(self._reader)

    next = __next__

    def __exit__(self, value, type, traceback):
        self._input.close()


class JSONInput(AbstractInput):
    """Read raw CSV input from a URL or filename."""

    def __init__(self, input, encoding='utf-8'):
        if sys.version_info < (3,):
            self._input = input
        else:
            self._input = io.TextIOWrapper(input, encoding=encoding)
        self._iterator = iter(json.load(self._input, encoding=encoding))
        self._encoding = encoding

    def __next__(self):
        row =  next(self._iterator)
        if sys.version_info < (3,):
            # Restore non-Unicode encoding (blech), because CSV parser doesn't Unicode encode
            row = [cell.encode(self._encoding) for cell in row]
        return row

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
        """Clean up an Excel value for CSV-like representation."""

        if cell.value is None or cell.ctype == xlrd.XL_CELL_EMPTY:
            return ''

        elif cell.ctype == xlrd.XL_CELL_NUMBER:
            # let numbers be integers if possible
            if float(cell.value).is_integer():
                return int(cell.value)
            else:
                return cell.value

        elif cell.ctype == xlrd.XL_CELL_DATE:
            # dates need to be formatted
            data = xlrd.xldate_as_tuple(cell.value, 0)
            return '{0[0]:04d}-{0[1]:02d}-{0[2]:02d}'.format(data)

        elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
            return int(cell.value)

        else: # XL_CELL_TEXT, or anything else
            if sys.version_info < (3,): # kludge for Python 2.x
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


def from_spec(spec):
    """Build a full spec (including source) from a JSON-like data structure."""
    
    if isinstance(spec, six.string_types):
        # a JSON string (parse it first)
        spec = json.loads(spec)

    # source
    input_spec = spec.get('input')
    allow_local = spec.get('allow_local', False)
    sheet_index = spec.get('sheet_index', None)
    timeout = spec.get('timeout', None)
    verify_ssl = spec.get('verify_ssl', True)

    # recipe
    tagger_spec = spec.get('tagger', None)
    recipe_spec = spec.get('recipe', [])

    if not input_spec:
        raise hxl.common.HXLException("No input property specified.")

    # set up the input
    input = make_input(
        raw_source=input_spec,
        allow_local=allow_local,
        sheet_index=sheet_index,
        timeout=timeout,
        verify_ssl=verify_ssl
    )

    # autotag if requested
    if tagger_spec:
        source = hxl.converters.Tagger._load(input, tagger_spec)
    else:
        source = HXLReader(input)

    # compile the main recipe
    return hxl.filters.from_recipe(
        source=source,
        recipe=recipe_spec
    )

# end
