"""
Input/output library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import abc, collections, csv, io, io_wrapper, json, logging, re, requests, six, sys, xlrd, xml.sax

import hxl

logger = logging.getLogger(__name__)


########################################################################
# Constants
########################################################################

# Cut off for fuzzy detection of a hashtag row
# At least this percentage of cells must parse as HXL hashtags
FUZZY_HASHTAG_PERCENTAGE = 0.5

# Patterns for URL munging
GOOGLE_DRIVE_URL = r'^https?://drive.google.com/open\?id=([0-9A-Za-z_-]+)$'
GOOGLE_SHEETS_URL = r'^https?://[^/]+google.com/.*[^0-9A-Za-z_-]([0-9A-Za-z_-]{44})(?:.*gid=([0-9]+))?.*$'
GOOGLE_FILE_URL = r'https?://drive.google.com/file/d/([0-9A-Za-z_-]+)/.*$'
DROPBOX_URL = r'^https://www.dropbox.com/s/([0-9a-z]{15})/([^?]+)\?dl=[01]$'
CKAN_URL = r'^(https?://[^/]+)/dataset/([^/]+)(?:/resource/([a-z0-9-]{36}))?$'

# opening signatures for well-known file types

JSON_MIME_TYPES = [
    'application/json'
]

JSON_FILE_EXTS = [
    'json'
]

JSON_SIGS = [
    b'[',
    b' [',
    b'{',
    b' {'
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


def data(data, allow_local=False, sheet_index=None, timeout=None, verify_ssl=True, selector=None):
    """
    Convenience method for reading a HXL dataset.
    If passed an existing Dataset, simply returns it.
    @param data: a HXL data provider, file object, array, or string (representing a URL or file name).
    @param allow_local: if true, allow opening local filenames as well as remote URLs (default: False).
    @param sheet_index: if supplied, use the specified 1-based index to choose a sheet from an Excel workbook (default: None)
    @param timeout: if supplied, time out an HTTP(S) request after the specified number of seconds with no data received (default: None)
    @param verify_ssl: if False, don't verify SSL certificates (e.g. for self-signed certs).
    @param selector: selector property for a JSON file (will later also cover tabs, etc.)
    """

    if isinstance(data, hxl.model.Dataset):
        # it's already HXL data
        return data

    elif isinstance(data, dict) and data.get('input'):
        """If it's a JSON-type spec, try parsing it."""
        return hxl.io.from_spec(data)

    else:
        return HXLReader(make_input(data, allow_local=allow_local, sheet_index=sheet_index, timeout=timeout, verify_ssl=True, selector=selector))

    
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

        
def write_json(output, source, show_headers=True, show_tags=True, use_objects=False):
    """Serialize a dataset to JSON."""
    for line in source.gen_json(show_headers, show_tags, use_objects):
        output.write(line)


def munge_url(url, verify_ssl=True):
    """Munge a URL to get at underlying data for well-known types."""

    #
    # Stage 1: unpack indirect links
    #

    # Is it a CKAN resource? (Assumes the v.3 API for now)
    result = re.match(CKAN_URL, url)
    if result:
        if result.group(3):
            # CKAN resource URL
            ckan_api_query = '{}/api/3/action/resource_show?id={}'.format(result.group(1), result.group(3))
            ckan_api_result = requests.get(ckan_api_query, verify=verify_ssl).json()
            url = ckan_api_result['result']['url']
        else:
            # CKAN dataset (package) URL
            ckan_api_query = '{}/api/3/action/package_show?id={}'.format(result.group(1), result.group(2))
            ckan_api_result = requests.get(ckan_api_query, verify=verify_ssl).json()
            url = ckan_api_result['result']['resources'][0]['url']

    # Is it a Google Drive "open" URL?
    result = re.match(GOOGLE_DRIVE_URL, url)
    if result:
        response = requests.head(url)
        if response.is_redirect:
            url = response.headers['Location']

    #
    # Stage 2: munge to get direct-download links
    #

    # Is it a Google Drive *file*?
    result = re.match(GOOGLE_FILE_URL, url)
    if result:
        return 'https://drive.google.com/uc?export=download&id={}'.format(result.group(1))

    # Is it a Google *Sheet*?
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

    # No changes
    return url


def make_input(raw_source, allow_local=False, sheet_index=None, timeout=None, verify_ssl=True, selector=None):
    """Figure out what kind of input to create.

    Can detect a URL or filename, an input stream, or an array.
    Will also try to detect HTML and Excel before defaulting to CSV.
    The result is an object that can deliver rows of data for the HXL library to parse.

    @param raw_source: the raw data source (e.g. a URL or input stream).
    @param allow_local: if True, allow opening local files as well as remote URLs (default: False).
    @param sheet_index: if a number, read that sheet from an Excel workbook (default: None).
    @param timeout: if supplied, time out an HTTP(S) request after the specified number of seconds with no data received (default: None)
    @param verify_ssl: if False, don't try to verify SSL certificates (e.g. for self-signed certs).
    @param selector: a property to select the data in a JSON record (may later extend to spreadsheet tabs).
    @return: an object belonging to a subclass of AbstractInput, returning rows of raw data.
    """

    def wrap_stream(stream):
        if hasattr(stream, 'peek'):
            # already buffered
            return stream
        else:
            stream = io_wrapper.RawIOWrapper(stream)
            return io.BufferedReader(io_wrapper.RawIOWrapper(stream))

    def match_sigs(sig, sigs):
        for s in sigs:
            if sig.startswith(s):
                return True
        return False

    if isinstance(raw_source, AbstractInput):
        # already an input source: no op
        return raw_source

    elif hasattr(raw_source, '__len__') and (not isinstance(raw_source, six.string_types)):
        # it's an array
        logger.debug('Making input from an array')
        return ArrayInput(raw_source)

    else:
        mime_type = None
        file_ext = None
        encoding = None
        
        if hasattr(raw_source, 'read'):
            # it's an input stream
            logger.debug('Making input from a stream')
            input = wrap_stream(raw_source)
        else:
            # assume a URL or filename
            logger.debug('Opening source %s as a URL or file', raw_source)
            (input, mime_type, file_ext, encoding) = open_url_or_file(raw_source, allow_local=allow_local, timeout=timeout, verify_ssl=verify_ssl)
            input = wrap_stream(input)

        sig = input.peek(4)[:4]

        if (mime_type in HTML5_MIME_TYPES) or match_sigs(sig, HTML5_SIGS):
            logger.exception(hxl.HXLException(
                "Received HTML5 markup.\nCheck that the resource (e.g. a Google Sheet) is publicly readable.",
                {
                    'input': input,
                    'source': raw_source,
                    'munged': munge_url(raw_source) if str(raw_source).startswith('http') else None
                }
            ))

        elif (mime_type in EXCEL_MIME_TYPES) or (file_ext in EXCEL_FILE_EXTS) or match_sigs(sig, EXCEL_SIGS):
            logger.debug('Making input from an Excel file')
            return ExcelInput(input, sheet_index=sheet_index)

        elif (mime_type in JSON_MIME_TYPES) or (file_ext in JSON_FILE_EXTS) or match_sigs(sig, JSON_SIGS):
            logger.debug('Trying to make input as JSON')
            return JSONInput(input, selector=selector)

        # fall back to CSV if all else fails
        logger.debug('Making input from CSV')
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
        try:
            response = requests.get(munge_url(url_or_filename, verify_ssl), stream=True, verify=verify_ssl, timeout=timeout)
            response.raise_for_status()
        except Exception as e:
            logger.exception("Cannot open URL %s (%s)", url_or_filename, str(e))
            raise e

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
        try:
            return (io.open(url_or_filename, 'rb'), mime_type, file_ext, encoding)
        except Exception as e:
            logger.exception("Cannot open local HXL file %s (%s)", url_or_filename, str(e))
            raise e

    else:
        # Forbidden to try local (allow_local is False), so give up.
        logger.critical('Tried to open local file %s with allow_local set to False', url_or_filename)
        raise IOError("Only http(s) and (s)ftp URLs allowed: {}".format(url_or_filename))


########################################################################
# Exported classes
########################################################################

class HXLParseException(hxl.HXLException):
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
        try:
            return self.response.raw.readable()
        except:
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

    def __init__(self):
        super().__init__()
        self.is_repeatable = False

    @abc.abstractmethod
    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        pass


class CSVInput(AbstractInput):
    """Read raw CSV input from a URL or filename."""

    DELIMITERS = {",", "\t", ";", ":", "|"}
    """Field delimiters allowed"""

    def __init__(self, input, encoding='utf-8'):
        super().__init__()

        # guess the delimiter
        delimiter = CSVInput.detect_delimiter(input, encoding)
        
        self._input = io.TextIOWrapper(input, encoding=encoding)
        self._reader = csv.reader(self._input, delimiter=delimiter)

    def __exit__(self, value, type, traceback):
        self._input.close()

    def __iter__(self):
        return self._reader

    @staticmethod
    def detect_delimiter(input, encoding):
        """Detect the CSV delimiter in use
        Grab the first 16K bytes, split into lines, then try splitting
        each line with each delimiter. The first one that yields a well-formed
        HXL hashtag row wins. If there is no winner, then choose the delimiter
        that appeared most often in the sample.
        @param input: the input byte stream (with a peek() method)
        @param encoding: the character encoding to use
        @returns a \L{csv.Dialect} object or string.
        """
        
        sample = input.peek(16384).decode(encoding)
        lines = re.split(r'\r?\n', sample)

        # first, try for a hashtag row
        for line in lines:
            if '#' in line:
                for delim in CSVInput.DELIMITERS:
                    fields = next(csv.reader([line], delimiter=delim))
                    if HXLReader.parse_tags(fields):
                        return delim

        # if that fails, return the delimiter that appears most often
        most_common_delim = ','
        max_count = -1
        for delim in CSVInput.DELIMITERS:
            count = sample.count(delim)
            if count > max_count:
                max_count = count
                most_common_delim = delim
                    
        return most_common_delim
    
class JSONInput(AbstractInput):
    """Iterable: Read raw CSV input from an input stream.
    The iterable values will be arrays usable as raw input for HXL.
    """

    def __init__(self, input, encoding='utf-8', selector=None):
        """Constructor
        The selector is used only if the top-level JSON is an object rather than an array.
        @param input: an input stream
        @param encoding: the default encoding to use (defaults to "utf-8")
        @param selector: the default dictionary key for the HXL data (defaults to "hxl")
        """
        super().__init__()

        # values to be set by _scan_data_element
        self.type = None
        self.headers = []
        self.show_headers = False

        # read the JSON data from the stream
        with io.TextIOWrapper(input, encoding=encoding) as _input:
            self.json_data = json.load(_input, encoding=encoding, object_pairs_hook=collections.OrderedDict)

        if selector is not None and isinstance(self.json_data, dict):
            if not selector in self.json_data:
                raise HXLParseException("Selector {} not found at top level of JSON data")
            # top level is a JSON object (dict); use the key provided to find HXL data
            if not self._scan_data_element(self.json_data[selector]):
                raise HXLParseException("Selected JSON data is not usable as HXL input (must be array of objects or array of arrays).")
        else:
            # top level is a JSON array; see if we can find HXL data in it
            if not self._scan_data_element(self.json_data):
                self.json_data = self._search_data(self.json_data)
            if self.json_data is None:
                raise HXLParseException("Could not usable JSON data (need array of objects or array of arrays)")

    def __iter__(self):
        """@returns: an iterator over raw HXL data (arrays of scalar values)"""
        return JSONInput.JSONIter(self)

    def _scan_data_element(self, data_element):
        """Scan a data sequence to see if it's a list of lists or list of arrays.
        @param data_element: JSON item to scan
        @returns: True if this is usable as HXL input
        """

        # JSON data must be an array at the top level
        if not hxl.datatypes.is_list(data_element):
            return False

        # scan the array to see if its elements are consistently arrays or objects
        for item in data_element:
            if isinstance(item, dict):
                if self.type == 'array':
                    # detect mixed values (array and object)
                    return False
                else:
                    # looking at objects
                    self.type = 'object'
                    self.show_headers = True
                    for key in item:
                        if not key in self.headers:
                            self.headers.append(key)
            elif isinstance(item, collections.Sequence) and not isinstance(item, six.string_types):
                if self.type == 'object':
                    #detect mixed values (object and array)
                    return False
                else:
                    # looking at array
                    self.type = 'array'
            else:
                # scalar value always fails (we need a JSON list of arrays or objects)
                return False
        return True # if we haven't failed yet, then let's use this

    def _search_data(self, data):
        """Recursive, breadth-first search for usable tabular data (JSON array of arrays or array of objects)
        @param data: top level of the JSON data to search
        @returns: the 
        """

        if hxl.datatypes.is_list(data):
            data_in = data
        elif isinstance(data, dict):
            data_in = data.values()
        else:
            return None

        # search the current level
        for item in data_in:
            if self._scan_data_element(item):
                return item

        # recursively search the children
        for item in data_in:
            data_out = self._search_data(item)
            if data_out is not None:
                return data_out

        return None # didn't find anything

    class JSONIter:
        """Iterator over JSON data"""

        def __init__(self, outer):
            self.outer = outer
            self._iterator = iter(self.outer.json_data)
            
        def __next__(self):
            """Return the next row in a tabular view of the data."""
            if self.outer.show_headers:
                # Add the header row first if reading an array of JSON objects
                self.outer.show_headers = False
                row = self.outer.headers
            elif self.outer.type == 'object':
                # Construct a row in an array of JSON objects
                obj = next(self._iterator)
                row = [hxl.datatypes.flatten(obj.get(header)) for header in self.outer.headers]
            elif self.outer.type == 'array':
                # Simply dump a row in an array of JSON arrays
                row =  [hxl.datatypes.flatten(value) for value in next(self._iterator)]
            return row


class ExcelInput(AbstractInput):
    """Iterable: Read raw XLS input from a URL or filename.
    If sheet number is not specified, will scan for the first tab with a HXL tag row.
    """

    def __init__(self, input, sheet_index=None):
        """
        Constructor
        @param url the URL or filename
        @param sheet_index (optional) the 0-based index of the sheet (if unspecified, scan)
        @param allow_local (optional) iff True, allow opening local files
        """
        super().__init__()
        self.is_repeatable = True
        try:
            self._workbook = xlrd.open_workbook(file_contents=input.read())
        finally:
            input.close()
        if sheet_index is None:
            sheet_index = self._find_hxl_sheet_index()
        self._sheet = self._workbook.sheet_by_index(sheet_index)

    def __iter__(self):
        return ExcelInput.ExcelIter(self)

    def _find_hxl_sheet_index(self):
        """Scan for a tab containing a HXL dataset."""
        for sheet_index in range(0, self._workbook.nsheets):
            sheet = self._workbook.sheet_by_index(sheet_index)
            for row_index in range(0, min(25, sheet.nrows)):
                raw_row = [ExcelInput._fix_value(cell) for cell in sheet.row(row_index)]
                # FIXME nasty violation of encapsulation
                if HXLReader.parse_tags(raw_row):
                    return sheet_index
        # if no sheet has tags, default to the first one for now
        return 0

    @staticmethod
    def _fix_value(cell):
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
            return cell.value

    class ExcelIter:
        """Internal iterator class for reading through an Excel sheet multiple times."""

        def __init__(self, outer):
            self.outer = outer
            self._row_index = 0
            
        def __next__(self):
            if self._row_index < self.outer._sheet.nrows:
                row = [ExcelInput._fix_value(cell) for cell in self.outer._sheet.row(self._row_index)]
                self._row_index += 1
                return row
            else:
                raise StopIteration()


class ArrayInput(AbstractInput):
    """Iterable: read raw input from an array."""

    def __init__(self, data):
        super().__init__()
        self.data = data
        self.is_repeatable = True

    def __iter__(self):
        return iter(self.data)


class HXLReader(hxl.model.Dataset):
    """Read HXL data from a raw input source
    This class is an iterable.
    @see: L{hxl.io.AbstractInput}
    """

    def __init__(self, input):
        """Constructor
        @param input: a child class of L{hxl.io.AbstractInput}
        """
        self._input = input
        # TODO - for repeatable raw input, start a new iterator each time
        # TODO - need to figure out how to handle columns in a repeatable situation
        self._iter = iter(self._input)
        self._columns = None
        self._source_row_number = -1 # TODO this belongs in the iterator
        
    def __enter__(self):
        """Context-start support."""
        if self._input:
            self._input.__enter__()
        return self

    def __exit__(self, value, type, traceback):
        """Context-end support."""
        if self._input:
            self._input.__exit__(value, type, traceback)

    def __iter__(self):
        return HXLReader.HXLIter(self)

    @property
    def is_cached(self):
        """If the low-level input is repeatable, then the data is cached."""
        #return self._input.is_repeatable
        return False # FIXME until we know that HXLReader is repeatable

    @property
    def columns(self):
        """Return a list of Column objects.
        """
        if self._columns is None:
            self._columns = self._find_tags()
        return self._columns

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
        hashtags_found = 0

        columns = []
        failed_hashtags = []

        for source_column_number, raw_string in enumerate(raw_row):
            if previous_row and source_column_number < len(previous_row):
                header = previous_row[source_column_number]
            else:
                header = None
            if not hxl.datatypes.is_empty(raw_string):
                raw_string = hxl.datatypes.normalise_string(raw_string)
                nonEmptyCount += 1
                column = hxl.model.Column.parse(raw_string, header=header, column_number=source_column_number)
                if column:
                    columns.append(column)
                    hashtags_found += 1
                    continue
                else:
                    failed_hashtags.append('"' + raw_string + '"')

            columns.append(hxl.model.Column(header=header, column_number=source_column_number))

        # Have we seen at least FUZZY_HASHTAG_PERCENTAGE?
        if (nonEmptyCount > 0) and ((hashtags_found/float(nonEmptyCount)) >= FUZZY_HASHTAG_PERCENTAGE):
            if len(failed_hashtags) > 0:
                logger.error('Skipping column(s) with malformed hashtag specs: %s', ', '.join(failed_hashtags))
            return columns
        else:
            return None

    def _get_row(self):
        """Parse a row of raw CSV data.  Returns an array of strings."""
        self._source_row_number += 1
        return next(self._iter)

    class HXLIter:
        """Internal iterator class"""
        
        def __init__(self, outer):
            self.outer = outer
            self.row_number = -1

        def __next__(self):
            """ Iterable function to return the next row of HXL values.
            @returns: a L{hxl.model.Row}
            @exception StopIterationException: at the end of the dataset
            """
            columns = self.outer.columns
            values = self.outer._get_row()
            self.row_number += 1
            return hxl.model.Row(columns=columns, values=values, row_number=self.row_number, source_row_number=self.outer._source_row_number)


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
        raise hxl.HXLException("No input property specified.")

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
