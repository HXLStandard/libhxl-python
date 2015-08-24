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
    # Customisation for Python 2.x
    import urllib2
    open_url = urllib2.urlopen
    def decode(s):
        return str(s)
    def get_status(response):
        return response.getcode()
else:
    # Customisation for Python 3.x
    import urllib.request
    open_url = urllib.request.urlopen
    def get_status(response):
        return response.status
    def decode(s):
        return s.decode('utf-8')

import hxl


########################################################################
# Constants
########################################################################

# Cut off for fuzzy detection of a hashtag row
# At least this percentage of cells must parse as HXL hashtags
FUZZY_HASHTAG_PERCENTAGE = 0.5

# Patterns for URL munging
GOOGLE_SHEETS_URL = r'^https?://docs.google.com/.*spreadsheets.*([0-9A-Za-z_-]{44})(?:.*gid=([0-9]+)).*$'
DROPBOX_URL = r'^https://www.dropbox.com/s/([0-9a-z]{15})/([^?]+)\?dl=[01]$'

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


def data(data, allow_local=False):
    """
    Convenience method for reading a HXL dataset.
    If passed an existing Dataset, simply returns it.
    @param data a HXL data provider, file object, array, or string (representing a URL or file name).
    """

    if isinstance(data, hxl.model.Dataset):
        # it's already HXL data
        return data

    else:
        return HXLReader(make_input(data, allow_local))

    
def write_hxl(output, source, show_headers=True, show_tags=True):
    """Serialize a HXL dataset to an output stream."""
    for line in source.gen_csv(show_headers, show_tags):
        output.write(line)

        
def write_json(output, source, show_headers=True, show_tags=True):
    """Serialize a dataset to JSON."""
    for line in source.gen_json(show_headers, show_tags):
        output.write(line)


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

    
def make_input(data, allow_local=False, sheet_index=None):
    """Figure out what kind of input to create."""

    if isinstance(data, AbstractInput):
        return data

    elif hasattr(data, '__len__') and (not isinstance(data, six.string_types)):
        # it's an array
        return ArrayInput(data)

    else:
        if hasattr(data, 'read'):
            # it's a stream
            input = Sniffer(data)
        else:
            # assume a URL or filename
            input = Sniffer(make_stream(data, allow_local=allow_local))

        if input.sig in HTML5_SIGS:
            raise hxl.common.HXLException("Received HTML5 input.\nCheck that resource (e.g. Google Sheet) is publicly readable.")
        elif input.sig in EXCEL_SIGS:
            return ExcelInput(input, sheet_index=sheet_index)
        else:
            return CSVInput(input)


def make_stream(origin, allow_local=False):
    """Figure out whether to open a file or a URL."""

    is_google = False

    # Pre-filter to get CSV for public Google Sheets or direct download for Dropbox
    result = re.match(GOOGLE_SHEETS_URL, origin)
    if result and not re.match(r'/pub', origin):
        if result.group(2):
            origin = 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv&gid={1}'.format(result.group(1), result.group(2))
        else:
            origin = 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv'.format(result.group(1))
    else:
        result = re.match(DROPBOX_URL, origin)
        if result:
            origin = 'https://www.dropbox.com/s/{0}/{1}?dl=1'.format(result.group(1), result.group(2))

    # Does it look like a url?
    if re.match(r'^(?:https?|ftp)://', origin):
        response = open_url(origin)
        if get_status(response) != 200:
            raise IOError('Received HTTP response code {}'.format(response.status_code))
        return response

    # Are we allowed to open local files?
    elif allow_local:
        return open(origin, 'rb')

    else:
        raise IOError('Only http(s) and ftp URLs allowed.')


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
        self._input = input
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
        raise HXLTagsNotFoundException('Cannot find an Excel sheet with HXL tags in the first 25 rows')


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


class Sniffer:
    """
    Extremely simple class to sniff the first four bytes of a file.

    Supports only the methods required by the CSV and Excel libraries
    That we're using. After wrapping an input source, the sig property
    will hold the file signature until someone invokes the read()
    or __next__() methods.
    """

    BUFFER_SIZE = 4096

    def __init__(self, raw_input):
        """Wrap an existing input source, caching the first four bytes"""
        self.raw_input = raw_input
        # read the first chunk right away, so that we can sniff
        self.buffer = bytearray(raw_input.read(self.BUFFER_SIZE))
        self.sig = self.buffer[:4]

    def read(self):
        """Read the entire contents of the file."""
        file = self.buffer
        chunk = self.raw_input.read(self.BUFFER_SIZE)
        while chunk:
            # use extend to avoid copying big buffers
            file.extend(chunk)
            chunk = self.raw_input.read(self.BUFFER_SIZE)
        return file

    def close(self):
        """Close the wrapped input object."""
        return self.raw_input.close()

    def __iter__(self):
        """Return this object as an iterator."""
        return self

    def __next__(self):
        """Return the next line in the file."""

        # scan for a newline
        pos = self.buffer.find(b"\n")
        while pos == -1:
            # keep reading until we get a newline or end of file
            chunk = self.raw_input.read(self.BUFFER_SIZE)
            if chunk:
                self.buffer.extend(chunk)
                pos = self.buffer.find(b"\n")
            else:
                # we hit the end of the file; stop reading
                break
        if pos == -1:
            # we never found a newline;
            if not self.buffer:
                # didn't read anything, so we're done
                raise StopIteration()
            else:
                # return what we read up to EOF
                s = decode(self.buffer)
                self.buffer = bytearray()
                return s
        else:
            # we found a newline: return the line, and adjust the buffer
            s = decode(self.buffer[:pos])
            self.buffer = self.buffer[pos+1:]
            return s

    next = __next__
            
# end
