"""Input/output library for the Humanitarian Exchange Language (HXL) v1.0

This module handles all contact with the outside world (reading and
writing data in different formats through different channels).

Examples:
    ```
    # Read a HXL-hashtagged dataset
    dataset = hxl.input.data("http://example.org/hxl-example.csv")

    # Read a non-HXL dataset and add hashtags
    specs = [['Cluster', '#sector'], ["Province", "#adm1+name"]]
    tagged_data = hxl.input.tagger("http://example.org/non-hxl-example.csv", specs)

    # Write out a dataset as JSON
    hxl.input.write_json(sys.stdout, dataset)

    # Write out a dataset as CSV
    hxl.input.write_csv(sys.stdout, dataset)
    ```

Author:
    David Megginson

License:
    Public Domain

"""

import hxl, hxl.filters

from hxl.util import logup

import abc, collections, csv, datetime, dateutil.parser, hashlib, \
    io, io_wrapper, json, jsonpath_ng.ext, logging, mmap, \
    os.path, re, requests, requests_cache, shutil, six, sys, \
    tempfile, time, urllib.parse, xlrd3 as xlrd, zipfile

logger = logging.getLogger(__name__)

__all__ = (
    "data",
    "tagger",
    "write_hxl",
    "write_json",
    "make_input",
    "HXLIOException",
    "HXLAuthorizationException",
    "HXLParseException",
    "HXLTagsNotFoundException",
    "AbstractInput",
    "CSVInput",
    "JSONInput",
    "ExcelInput",
    "ArrayInput",
    "InputOptions",
    "HXLReader",
    "from_spec",
)


########################################################################
# Constants
########################################################################

# Numeric constants
EXCEL_MEMORY_CUTOFF = 0x1000000 # max 16MB to load an Excel file into memory

# Patterns for URL munging
GOOGLE_DRIVE_URL = r'^https?://drive.google.com/open\?id=([0-9A-Za-z_-]+)$'
GOOGLE_SHEETS_URL = r'^https?://[^/]+google.com/.*[^0-9A-Za-z_-]([0-9A-Za-z_-]{44})(?:.*gid=([0-9]+))?.*$'
GOOGLE_SHEETS_XLSX_URL = r'^https?://[^/]+google.com/.*[^0-9A-Za-z_-]([0-9A-Za-z_-]{33})(?:.*gid=([0-9]+))?.*$'
GOOGLE_FILE_URL = r'https?://drive.google.com/file/d/([0-9A-Za-z_-]+)/.*$'
DROPBOX_URL = r'^https://www.dropbox.com/s/([0-9a-z]{15})/([^?]+)\?dl=[01]$'
CKAN_URL = r'^(https?://[^/]+)/dataset/([^/]+)(?:/resource/([a-z0-9-]{36}))?$'
HXL_PROXY_SAVED_URL = r'^(https?://[^/]*proxy.hxlstandard.org)/data/([a-zA-Z0-9_]{6})[^?]*(\?.*)?$'
HXL_PROXY_ARGS_URL = r'^(https?://[^/]*proxy.hxlstandard.org)/data.*\?(.+)$'
KOBO_URL = r'^https://kobo.humanitarianresponse.info/#/forms/([A-Za-z0-9]{16,32})/'

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

ZIP_FILE_EXTS = [
    'zip'
]

ZIP_MIME_TYPES = [
    'application/zip'
]

ZIP_SIGS = [
    b"PK\x03\x04",
]

XLSX_MIME_TYPES = [
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
]

XLSX_FILE_EXTS = [
    'xlsx'
]

XLSX_SIGS = [
    b"PK\x03\x04",
]

XLS_MIME_TYPES = [
    'application/vnd.ms-excel',
]

XLS_FILE_EXTS = [
    'xls'
]

XLS_SIGS = [
    b"\xd0\xcf\x11\xe0",
]

HTML5_MIME_TYPES = [
    'text/html'
]

HTML5_SIGS = [
    b"<!DO",
    b"<!do",
    b"\n<!D",
    b"\n<!d",
    b"<HTM",
    b"<htm",
    b"\n<HT",
    b"\n<ht",
    b"<BOD",
    b"<bod",
    b"\n<BO",
    b"\n<bo",
]


########################################################################
# Exported functions
########################################################################


def data(data, input_options=None):
    """
    Convenience method for reading a HXL dataset.

    If passed an existing Dataset, simply returns it. All args exception "data" are optional.

    Args:
        data: a HXL data provider, file object, array, or string (representing a URL or file name).
        input_options (InputOptions): options for reading a dataset.

    Returns:
        hxl.model.Dataset: a data-access object

    Raises:
        IOError: if there's an error loading the data.
        hxl.HXLException: if there's a structural error in the data.
        hxl.input.HXLAuthorizationException: if the source requires some kind of authorisation (possibly fixable by adding an Authorization: header to the ``http_headers`` arg.

    """

    logger.debug("HXL data from %s", str(data))

    if isinstance(data, hxl.model.Dataset):
        # it's already HXL data
        return data

    elif isinstance(data, dict) and data.get('input'):
        """If it's a JSON-type spec, try parsing it."""
        return hxl.input.from_spec(data, allow_local_ok=input_options is not None and input_options.allow_local)

    else:

        # kludge: if it's a CKAN dataset URL without a resource,
        # try to find a resource with hashtags (only if requested)
        if input_options and input_options.scan_ckan_resources:
            result = re.match(CKAN_URL, str(data))
            if result and not result.group(3): # no resource
                logup(f"Using CKAN API to dereference", {"url": data})
                resource_urls = _get_ckan_urls(result.group(1), result.group(2), result.group(3), input_options)
                for resource_url in resource_urls:
                    try:
                        source = hxl.data(resource_url, input_options)
                        source.columns # force HXLTagsNotFoundException if not HXLated
                        return source
                    except:
                        pass

        return HXLReader(make_input(data, input_options))


def tagger(data, specs, input_options=None, default_tag=None, match_all=False):
    """Open an untagged data source and add hashtags.

    The specs are a list of pairs in the format ["header", "#hashtag"], e.g.

    ```
    specs = [
        ["Province", "#adm1+name"],
        ["P-code", "#adm1+code"],
        ["Organisation", "#org+name"],
        ["Cluster", "#sector+cluster"]
    ]
    ```

    It is not necessary to match the headers in all columns, and if
    the "match_all" arg is False (the default), the header strings
    will match partial as well as complete header strings. Matching is
    always case- and whitespace-insensitive. Other args are identical
    to the data() function.

    Args:
        data: a HXL data provider, file object, array, or string (representing a URL or file name).
        specs (list): a list of mapping pairs of headers and hashtags.
        match_all (bool): if True, match the complete header string; otherwise, allow partial matches (default)
        input_options (InputOptions): options for reading a dataset.

    Returns:
        hxl.converters.Tagger: a data-access object subclassed from hxl.model.Dataset

    Raises:
        IOError: if there's an error loading the data.
        hxl.HXLException: if there's a structural error in the data.
        hxl.input.HXLAuthorizationException: if the source requires some kind of authorisation (possibly fixable by adding an Authorization: header to the ``http_headers`` arg.

    """
    import hxl.converters
    return hxl.data(
        hxl.converters.Tagger(
            input=make_input(data, input_options),
            specs=specs,
            default_tag=default_tag,
            match_all=match_all
        )
    )


def write_hxl(output, source, show_headers=True, show_tags=True):
    """Serialize a HXL dataset to an output stream in CSV format.

    The output will be comma-separated CSV, in UTF-8 character encoding.

    Args:
        output (io.IOBase): an output byte stream
        source (hxl.model.Dataset): a HXL data-access object
        show_headers (bool): if True (default), include text header row.
        show_tags (bool): if True (default), include the HXL hashtag row.

    Raises:
        IOError: if there's a problem writing the output

    """
    for line in source.gen_csv(show_headers, show_tags):
        output.write(line)


def write_json(output, source, show_headers=True, show_tags=True, use_objects=False):
    """Serialize a HXL dataset to an output stream.

    The output will be JSON in one of two styles.

    Row-style JSON (default):
    ```
    [
        ["Province", "Organisation", "Cluster"],
        ["#adm1+name", "#org", "#sector+cluster"],
        ["Coast", "Org A", "Health"],
        ["Plains", "Org B", "Education"],
        ["Mountains", "Org A", "Nutrition"]
    ]
    ```

    Object-style JSON:
    ```
    [
        {
            "#adm1+name": "Coast",
            "#org": "Org A",
            "#sector+cluster": "Health"
        },
        {
            "#adm1+name": "Plains",
            "#org": "Org B",
            "#sector+cluster": "Education"
        },
        {
            "#adm1+name": "Mountains",
            "#org": "Org A",
            "#sector+cluster": "Nutrition"
        }
    ]
    ```

    Args:
        output (io.IOBase): an output byte stream
        source (hxl.model.Dataset): a HXL data-access object
        show_headers (bool): if True (default), include text header row.
        show_tags (bool): if True (default), include the HXL hashtag row.
        use_objects (bool): if True, produce object-style JSON; otherwise, produce row-style JSON (default).

    Raises:
        IOError: if there's a problem writing the output

    """
    for line in source.gen_json(show_headers, show_tags, use_objects):
        output.write(line)


def make_input(raw_source, input_options=None):
    """Figure out what kind of input to create.

    This is a lower-level I/O function that sits beneath ``data()``
    and ``tagger()``. It figures out how to create row-by-row data
    from various sources (e.g. XLSX, Google Sheets, CSV, JSON), and
    returns an object for iterating through the rows.

    Example:
    ```
    input = make_input("data.xlsx", InputOptions(allow_local=True))
    for raw_row in input:
        process_row(raw_row) # each row will be a list of values
    ```

    The raw source can be a URL or filename, an input stream, or an array.

    Args:
        raw_source: a HXL data provider, file object, array, or string (representing a URL or file name).
        input_options (InputOptions): input_options for reading a dataset.

    Returns:
        hxl.input.AbstractInput: a row-by-row input object (before checking for HXL hashtags)

    Raises:
        IOError: if there's an error loading the data.
        hxl.HXLException: if there's a structural error in the data.
        hxl.input.HXLAuthorizationException: if the source requires some kind of authorisation (possibly fixable by adding an Authorization: header to the ``http_headers`` arg.

    """

    def make_tempfile(input):
        tmpfile = tempfile.NamedTemporaryFile()
        shutil.copyfileobj(input, tmpfile)
        tmpfile.seek(0)
        input.close()
        return tmpfile # have to return the object, so it doesn't get garbage collected and delete the file

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

    if input_options is None:
        input_options = InputOptions(allow_local=False, verify_ssl=True) # safe default

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
        encoding = input_options.encoding
        url_or_filename = None
        fileno = None
        content_length = None

        if hasattr(raw_source, 'read'):
            # it's an input stream
            logger.debug('Making input from a stream')
            input = wrap_stream(raw_source)
        else:
            # assume a URL or filename
            logger.debug('Opening source %s as a URL or file', raw_source)

            # back to usual
            url_or_filename = raw_source
            (input, mime_type, file_ext, specified_encoding, content_length, fileno,) = open_url_or_file(raw_source, input_options)
            input = wrap_stream(input)

            # figure out the character encoding
            if encoding is None: # if no encoding was provided, use the inferred one
                if specified_encoding:
                    encoding = specified_encoding

        if not encoding: # if we still have no character encoding, default to UTF-8
            encoding = "utf-8"

        sig = input.peek(4)[:4]

        if (mime_type in HTML5_MIME_TYPES) or match_sigs(sig, HTML5_SIGS):
            raise HXLHTMLException(
                "Received HTML markup.\nCheck that the resource (e.g. a Google Sheet) is publicly readable.",
                url = url_or_filename
            )

        if match_sigs(sig, XLS_SIGS) or match_sigs(sig, XLSX_SIGS):

            tmpfile = None
            contents = None

            if fileno is not None:
                 # it's already a file; don't make a new one
                input.seek(0)
                contents = mmap.mmap(fileno, 0)
            elif content_length and content_length <= EXCEL_MEMORY_CUTOFF:
                # it's small-ish, so load into memory
                contents = input.read()
            else:
                # size unknown, so use a tempfile
                tmpfile = make_tempfile(input)
                contents = mmap.mmap(tmpfile.fileno(), 0)

            try:
                # Is it really an XLS(X) file?
                logger.debug('Trying input from an Excel file')
                return ExcelInput(contents, input_options, tmpfile=tmpfile, url_or_filename=url_or_filename)
            except xlrd.XLRDError:
                # If not, see if it contains a CSV file
                if match_sigs(sig, ZIP_SIGS): # more-restrictive
                    zf = zipfile.ZipFile(io.BytesIO(contents), "r")
                    for name in zf.namelist():
                        if os.path.splitext(name)[1].lower()==".csv":
                            return CSVInput(wrap_stream(io.BytesIO(zf.read(name))), input_options)

            raise HXLIOException("Cannot find CSV file or Excel content in zip archive")

        elif (mime_type in JSON_MIME_TYPES) or (file_ext in JSON_FILE_EXTS) or match_sigs(sig, JSON_SIGS):
            logger.debug('Trying to make input as JSON')
            return JSONInput(input, input_options)

        # fall back to CSV if all else fails
        logger.debug('Making input from CSV')
        return CSVInput(input, input_options)


def open_url_or_file(url_or_filename, input_options):
    """Try opening a local or remote resource.

    Allows only HTTP(S) and (S)FTP URLs.

    Args:
        url_or_filename (string): the string to try openining.
        input_options (InputOptions): options for reading a dataset.

    Returns:
        sequence of
          input (io.IOBase)
          mime_type (string or None)
          file_ext (string or None)
          encoding (string or None)
          content_length (long or None)
          fileno (int)

    Raises:
        IOError: if there's an error opening the data stream
    """
    mime_type = None
    file_ext = None
    encoding = None
    content_length = None
    fileno = None

    # Try for file extension
    result = re.search(r'\.([A-Za-z0-9]{1,5})$', url_or_filename)
    if result:
        file_ext = result.group(1).lower()

    result = re.match(r'^(?:https?|s?ftp)://([^/]+)', url_or_filename.lower())
    if result:

        # Check for possible exploits when allow_local is False
        if not input_options.allow_local:

            hostname = result.group(1).lower().strip()

            # forbid dotted quads
            if re.match(r'^[0-9.]+$', hostname):
                raise HXLIOException("Security settings forbid accessing host via IP address {}", hostname)

            # forbid localhost
            if hostname == "localhost":
                raise HXLIOException("Security settings forbid accessing localhost")

            # forbid localhost
            if hostname.endswith(".localdomain"):
                raise HXLIOException("Security settings forbid accessing hostnames ending in .localdomain: {}", hostname)

        # It looks like a URL
        file_ext = os.path.splitext(urllib.parse.urlparse(url_or_filename).path)[1]
        try:
            url = munge_url(url_or_filename, input_options)
            logup("Trying to open remote resource", {"url": url_or_filename})
            response = requests.get(
                url,
                stream=True,
                verify=input_options.verify_ssl,
                timeout=input_options.timeout,
                headers=input_options.http_headers
            )
            logup("Response status", {"url": url_or_filename, "status": response.status_code})
            if (response.status_code == 403): # CKAN sends "403 Forbidden" for a private file
                raise HXLAuthorizationException("Access not authorized", url=url)
            else:
                response.raise_for_status()
        except Exception as e:
            logger.error("Cannot open URL %s (%s)", url_or_filename, str(e))
            raise e

        content_type = response.headers.get('content-type')
        if content_type:
            result = re.match(r'^(\S+)\s*;\s*charset=(\S+)$', content_type)
            if result:
                mime_type = result.group(1).lower()
                encoding = result.group(2).lower()
            else:
                mime_type = content_type.lower()

        content_length = response.headers.get('content-length')
        if content_length is not None:
            try:
                content_length = int(content_length)
            except:
                content_length = None

        # return (RequestResponseIOWrapper(response), mime_type, file_ext, encoding, content_length, fileno,)
        return (io.BytesIO(response.content), mime_type, file_ext, encoding, content_length, fileno,)

    elif input_options.allow_local:
        # Default to a local file, if allowed
        try:
            info = os.stat(url_or_filename)
            content_length = info.st_size
            file = io.open(url_or_filename, 'rb+')
            fileno = file.fileno()
            return (file, mime_type, file_ext, encoding, content_length, fileno,)
        except Exception as e:
            logger.error("Cannot open local HXL file %s (%s)", url_or_filename, str(e))
            raise e

    else:
        # Forbidden to try local (allow_local is False), so give up.
        logger.error('Security settings forbid accessing local files or non http(s)/ftp(s) URL schemes: %s', url_or_filename)
        raise HXLIOException(
            "Only http(s) and (s)ftp URLs allowed: {}".format(url_or_filename),
            url=url_or_filename
        )



########################################################################
# Exported classes
########################################################################

class HXLIOException(hxl.HXLException, IOError):
    """ Base class for all HXL IO-related exceptions
    """
    def __init__(self, message, url=None):
        """
        Args:
            message (str): the error message
            url (str): the URL that caused the error (if relevant)

        """
        super().__init__(message)
        self.url = url


class HXLHTMLException(HXLIOException):
    """ Found HTML markup instead of data on the web
    """
    def __init__(self, message, url):
        """ Args:
          message (str): the error message
          url (str): the URL that caused the error
        """
        super().__init__(message, url)


class HXLAuthorizationException(HXLIOException):
    """ An authorisation error for a remote resource.

    This exception means that the library was not allowed to read the remote resource.
    Sometimes adding an ``Authorization:`` HTTP header with a token will help.

    """
    def __init__(self, message, url, is_ckan=False):
        """
        Args:
            message (str): the error message
            url (str): the URL that triggered the exception
            is_ckan (bool): if True, the error came from a CKAN instance

        """
        super().__init__(message, url)
        self.is_ckan = is_ckan


class HXLParseException(HXLIOException):
    """A parsing error in a HXL dataset.

    This exception means that something was wrong with the HXL tagging
    (or similar). The ``message`` will contain details, and if
    possible, there will be a column and row number to help the user
    locate the error.

    """
    def __init__(self, message, source_row_number=None, source_column_number=None, url=None):
        """
        Args:
            message (str): the error message
            source_row_number (int): the row number in the raw source data, if known.
            source_column_number (int): the column number in the raw source data, if known.
            url (str): the URL of the source data, if relevant.

        """
        super().__init__(message, url)
        self.source_row_number = source_row_number
        self.source_column_number = source_column_number


class HXLTimeoutException(HXLIOException):
    """ There is a timeout when trying to load data
    Right now, only Kobo uses this.
    """
    def __init__(self, message="Timeout downloading source data", url=None):
        """
        Args:
            message (str): the error message
            url (str): the URL that triggered the exception
        """
        super().init__(message, url)


class HXLTagsNotFoundException(HXLParseException):
    """ Specific parsing exception: no HXL tags.

    This exception means that the library could find no HXL hashtags in the source data.
    Using the ``tagger()`` function with tagging specs can resolve the problem.

    """
    def __init__(self, message='HXL tags not found in first 25 rows', url=None):
        """
        Args:
            message (str): the error message
            url (str): the URL of the source data, if available

        """
        super().__init__(message, url)


class InputOptions:
    """ Input options for datasets.

    Properties:
        allow_local (bool): if true, allow opening local filenames as well as remote URLs (default: False).
        sheet_index (int): if supplied, use the specified 1-based index to choose a sheet from an Excel workbook (default: None)
        timeout (int): if supplied, time out an HTTP(S) request after the specified number of seconds with no data received (default: None)
        verify_ssl (bool): if False, don't verify SSL certificates (e.g. for self-signed certs).
        http_headers (dict): optional dict of HTTP headers to add to a request.
        selector (str): selector property for a JSON file (will later also cover tabs, etc.)
        encoding (str): force a character encoding, regardless of HTTP info etc
        expand_merged (bool): expand merged areas by repeating the value (Excel only)
        scan_ckan_resources (bool): for a CKAN dataset URL, scan all resources for the first HXLated one (defaults to just using first resource)
    """

    def __init__ (
            self,
            allow_local=False,
            sheet_index=None,
            timeout=None,
            verify_ssl=True,
            http_headers=None,
            selector=None,
            encoding=None,
            expand_merged=False,
            scan_ckan_resources=False
            ):
        self.allow_local = allow_local
        self.sheet_index = sheet_index
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.http_headers = http_headers
        self.selector = selector
        self.encoding = encoding
        self.expand_merged = expand_merged
        self.scan_ckan_resources = scan_ckan_resources


# Deprecated - will remove in future release
class RequestResponseIOWrapper(io.RawIOBase):
    """Wrapper for a Response object from the requests library.  Streaming
    in requests is a bit broken: for example, if you're streaming, the
    stream from the raw property doesn't unzip the payload.
    """

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
    """Abstract base class for input classes.

    All of the derived classes allow returning one row of raw data at
    a time from child classes, via normal iteration, and support
    context management ("with" statements). Each row is represented as
    a list of values. No semantic HXL processing has taken place yet
    at this stage.

    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, input_options):
        super().__init__()
        self.input_options = input_options
        self.is_repeatable = False

    def info(self):
        """ Get information about the raw dataset.
        Uses low-level row-wise input, so the source doesn't have to be HXLated.

        The result will be a dict with info about the workbook:

        - format (e.g. "XLSX")
        - sheets (list)

        The following will appear for each sheet:

        - sheet_name (string)
        - is_hidden (boolean)
        - nrows (int)
        - ncols (int)
        - has_merged_cells (boolean)
        - is_hxlated (boolean)
        - header_hash (MD5 string)
        - hashtag hash (MD5 string, or null if not HXLated)

        (Currently supported only for Excel.)

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        pass


class CSVInput(AbstractInput):
    """Iterable: read raw CSV rows from an input stream.

    Supports context management.

    Example:
    ```
    with hxl.input.CSVInput(open("data.csv", "r")) as csv:
        for raw_row in csv:
            process_row(raw_row)
    ```

    """

    _DELIMITERS = [",", "\t", ";", ":", "|"]
    """ CSV delimiters allowed """

    def __init__(self, input, input_options):
        """
        Args:
            input (io.IOBase): a byte input stream
            input_options (InputOptions): options for reading a dataset.

        """
        super().__init__(input_options)

        # guess the delimiter
        delimiter = CSVInput._detect_delimiter(input, input_options.encoding or "utf-8")

        self._input = io.TextIOWrapper(input, encoding=input_options.encoding, errors="replace")
        self._reader = csv.reader(self._input, delimiter=delimiter)

    def __exit__(self, value, type, traceback):
        self._input.close()

    def __iter__(self):
        return self._reader

    @staticmethod
    def _detect_delimiter(input, encoding):
        """Detect the CSV delimiter in use
        Grab the first 16K bytes, split into lines, then try splitting
        each line with each delimiter. The first one that yields a well-formed
        HXL hashtag row wins. If there is no winner, then choose the delimiter
        that appeared most often in the sample.
        @param input: the input byte stream (with a peek() method)
        @param encoding: the character encoding to use
        @returns a csv.Dialect object or string.
        """

        raw = input.peek(16384)

        # Special case: there might be part of a multibyte Unicode character at the end
        for i in range(0, 7):
            try:
                sample = raw[:-1].decode(encoding, errors="replace")
            except Exception as e:
                continue
            break

        lines = re.split(r'\r?\n', sample)

        # first, try for a hashtag row
        logger.debug("No CSV delimiter specified; trying to autodetect by looking for a hashtag row")
        for row_index, line in enumerate(lines):
            if '#' in line:
                logger.debug("Row %d contains \"#\"", row_index)
                for delim in CSVInput._DELIMITERS:
                    logger.debug("Trying delimiter \"%s\"", delim)
                    fields = next(csv.reader([line], delimiter=delim))
                    if hxl.model.Column.parse_list(fields):
                        logger.debug("Succeeded in parsing hashtags in row %d. Using \"%s\" as CSV delimiter", row_index, delim)
                        return delim

        # if that fails, return the delimiter that appears most often
        most_common_delim = ','
        max_count = -1
        for delim in CSVInput._DELIMITERS:
            count = sample.count(delim)
            if count > max_count:
                max_count = count
                most_common_delim = delim

        logger.debug("Failed to parse a HXL hashtag row, so using most-common CSV delimiter \"%s\"", most_common_delim)
        return most_common_delim


class JSONInput(AbstractInput):
    """Iterable: Read raw JSON rows from an input stream.

    Can handle both row-style and object-style JSON, and will detect
    it from the data. Will also search through the data to find
    something that looks like rows of data, or can use an explicit
    selector (either a top-level property name or a JSONPath
    statement) to find the data rows.

    Example:
    ```
    with hxl.input.JSONInput(open("data.json", "r")) as json:
        for raw_row in json:
            process_row(raw_row)
    ```

    """

    def __init__(self, input, input_options):
        """
        Args:
            input (io.IOBase): an input byte stream
            input_options (InputOptions): options for reading a dataset.

        """
        super().__init__(input_options)

        # values to be set by _scan_data_element
        self.type = None
        self.headers = []
        self.show_headers = False

        # read the JSON data from the stream
        with io.TextIOWrapper(input, encoding=input_options.encoding) as _input:
            self.json_data = self._select(input_options.selector, json.load(_input, object_pairs_hook=collections.OrderedDict))
        if not self._scan_data_element(self.json_data):
            self.json_data = self._search_data(self.json_data)
        if self.json_data is None:
            raise HXLParseException("Could not usable JSON data (need array of objects or array of arrays)")

    def __iter__(self):
        """@returns: an iterator over raw HXL data (arrays of scalar values)"""
        return JSONInput._JSONIter(self)

    def _select(self, selector, data):
        """Find the JSON matching the selector"""
        if selector is None:
            # no selector
            return data
        elif hxl.datatypes.is_token(selector):
            # legacy selector (top-level JSON object property)
            if not isinstance(data, dict):
                raise HXLParseException("Expected a JSON object at the top level for simple selector {}".format(selector))
            if selector not in data:
                raise HXLParseException("Selector {} not found at top level of JSON data".format(selector))
            return data[selector]
        else:
            # full JSONpath
            path = jsonpath_ng.ext.parse(selector)
            matches = path.find(data)
            if len(matches) == 0:
                raise HXLParseException("No matches for JSONpath {}".format(selector))
            else:
                # Tricky: we have multiple matches
                # Do we want a list of all matches, or just the first match?
                # Try to guess from the first value matched
                values = [match.value for match in matches]
                if len(values) > 0 and self._scan_data_element(values[0]):
                    return values[0]
                else:
                    return values
            raise HXLParseException("JSONPath results for {} not usable as HXL data".format())

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
            elif isinstance(item, collections.abc.Sequence) and not isinstance(item, six.string_types):
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

    class _JSONIter:
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
            else:
                raise StopIteration()
            return row


class ExcelInput(AbstractInput):
    """Iterable: Read raw XLS or XLSX (Excel) rows from a temporary file object

    If there is no sheet number specified, will scan the Excel
    workbook for the first sheet containing HXL hashtags; if that
    fails, will use the first sheet in the workbook.

    Note that this requires a Python tempfile.TemporaryFile object,
    with the Excel contents copied into it.

    Example:
    ```
    tmpfile = tempfile.NamedTemporaryFile();
    with open("data.xls", "r") as input:
        shutil.copyfileobj(input, tmpfile)

    with hxl.input.ExcelInput(tmpfile) as xlsx:
        for raw_row in xlsx:
            process_row(raw_row)
    ```

    """

    def __init__(self, contents, input_options, tmpfile, url_or_filename=None):
        """

        One of tmpfile or contents must be specified.

        Args:
            contents (buffer or mmap): contents of the Excel file
            input_options (InputOptions): options for reading a dataset.
            tmpfile (tempfile.NamedTemporaryFile): temporary file object (keep to avoid garbage collection)
            url_or_filename (string): the original URL or filename or None
        """
        super().__init__(input_options)
        self.url_or_filename = url_or_filename
        self.is_repeatable = True
        self.contents = contents
        self.tmpfile = tmpfile # prevent garbage collection

        self._workbook = xlrd.open_workbook(file_contents=contents, on_demand=False, ragged_rows=True)

        sheet_index = self.input_options.sheet_index
        if sheet_index is None:
            sheet_index = self._find_hxl_sheet_index()

        self._sheet = self._get_sheet(sheet_index)
        self.merged_values = {}

    def info (self):
        """ See method doc for parent class """

        def hash_headers (raw_row):
            """ Create a hash just for the first row of values
            """
            md5 = hashlib.md5()
            for value in raw_row:
                md5.update(hxl.datatypes.normalise_space(value).encode('utf-8'))
            return md5.hexdigest()

        result = {
            "url_or_filename": self.url_or_filename,
            "format": "XLSX" if self._workbook.biff_version == 0 else "XLS",
            "sheets": [],
        }
        for sheet_index in range(0, self._workbook.nsheets):
            sheet = self._get_sheet(sheet_index)
            columns = self._get_columns(sheet)
            sheet_info = {
                "name": sheet.name,
                "is_hidden": (sheet.visibility > 0),
                "nrows": sheet.nrows,
                "ncols": sheet.ncols,
                "has_merged_cells": (len(sheet.merged_cells) > 0),
                "is_hxlated": (columns is not None),
                "header_hash": hash_headers(self._get_row(sheet, 0)) if sheet.nrows > 0 else None,
                "hashtag_hash": hxl.model.Column.hash_list(columns) if columns else None,
            }
            result["sheets"].append(sheet_info)
        return result

    def __iter__(self):
        return ExcelInput._ExcelIter(self)

    def _find_hxl_sheet_index(self):
        """Scan for a tab containing a HXL dataset."""
        logger.debug("No Excel sheet specified; scanning for HXL hashtags")
        for sheet_index in range(0, self._workbook.nsheets):
            logger.debug("Trying Excel sheet %d for HXL hashtags", sheet_index)
            sheet = self._get_sheet(sheet_index)
            if self._get_columns(sheet):
                logger.debug("Found HXL hashtags in Excel sheet %d", sheet_index)
                return sheet_index
        # if no sheet has tags, default to the first one for now
        logger.debug("No HXL hashtags found; defaulting to Excel sheet 0")
        return 0

    def _get_columns(self, sheet):
        """ Return a list of column objects if a sheet has HXL hashtags in the first 25 rows """
        previous_row = None
        for row_index in range(0, min(25, sheet.nrows)):
            raw_row = self._get_row(sheet, row_index)
            tags = hxl.model.Column.parse_list(raw_row, previous_row)
            if tags:
                return tags
            else:
                previous_row = raw_row
        return None

    def _get_sheet(self, index):
        """Try opening a sheet, and raise an exception if it's not possible"""
        if index >= self._workbook.nsheets:
            raise HXLIOException("Excel sheet index out of range 0-{}".format(self._workbook.nsheets))
        else:
            return self._workbook.sheet_by_index(index)

    def _get_row(self, sheet, index):
        row = sheet.row(index)
        return [self._fix_value(cell) for cell in row]

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
            try:
                data = xlrd.xldate_as_tuple(cell.value, 0)
                return '{0[0]:04d}-{0[1]:02d}-{0[2]:02d}'.format(data)
            except:
                return cell.value

        elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
            return int(cell.value)

        else: # XL_CELL_TEXT, or anything else
            return cell.value

    def _do_expand (self, row_num, col_num, value):
        """ Repeat a value in a merged section, if necessary.
        """

        if self.input_options.expand_merged:
            for merge in self._sheet.merged_cells:
                row_min, row_max, col_min, col_max = merge
                if row_num in range(row_min, row_max) and col_num in range(col_min, col_max):
                    if row_num == row_min and col_num == col_min:
                        # top left == the value merged through all the cells
                        self.merged_values[(merge)] = value
                        return value
                    else:
                        return self.merged_values[(merge)]

        # default: unchanged
        return value

    class _ExcelIter:
        """Internal iterator class for reading through an Excel sheet multiple times."""

        def __init__(self, outer):
            self.outer = outer
            self._row_index = 0
            self._col_max = 0

        def __next__(self):
            if self._row_index < self.outer._sheet.nrows:
                row = []

                # process the actual cells
                for col_index, cell in enumerate(self.outer._sheet.row(self._row_index)):

                    # keep track of maximum row length seen so far
                    if col_index >= self._col_max:
                        self._col_max = col_index + 1

                    # process and add the value
                    row.append(
                        self.outer._do_expand(
                            self._row_index,
                            col_index,
                            self.outer._fix_value(cell)
                        )
                    )

                # fill in the row with empty values, up the the maximum length previously observed
                # (this lets us expand merged areas at the end of the row, if needed)
                for col_index in range(len(self.outer._sheet.row(self._row_index)), self._col_max):
                    row.append(
                        self.outer._do_expand(
                            self._row_index,
                            col_index,
                            '',
                        )
                    )

                self._row_index += 1
                return row
            else:
                raise StopIteration()


class ArrayInput(AbstractInput):
    """Iterable: read raw input from an array.

    This is a simple placeholder class for dealing with a pre-parsed
    array of rows in the same class hierarchy as the other classes
    derived from hxl.input.AbstractInput. There is no value in using it
    alone.

    """

    def __init__(self, data):
        """
        Args:
            data (array): any iterable

        """
        super().__init__(input_options=None)
        self.data = data
        self.is_repeatable = True

    def __iter__(self):
        return iter(self.data)



########################################################################
# HXL semantic parsing
########################################################################

class HXLReader(hxl.model.Dataset):
    """Read HXL data from a raw input source

    This class is the parser that reads raw rows of data from a
    ``hxl.input.AbstractInput`` class and looks for HXL semantics such as
    hashtags and attributes. The object itself is a hxl.model.Dataset
    that's available for iteration and filter chaining.

    """

    def __init__(self, input):
        """
        Args:
            input (hxl.input.AbstractInput): an input source for raw data rows

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
        return HXLReader._HXLIter(self)

    @property
    def is_cached(self):
        """If the low-level input is repeatable, then the data is cached.

        "Repeatable" means that you can iterate over a dataset
        multiple times. By default, the data is streaming, so it's
        used up after one iteration.

        Returns:
            bool: True if the data is repeatable.

        """
        #return self._input.is_repeatable
        return False # FIXME until we know that HXLReader is repeatable

    @property
    def columns(self):
        """List of columns

        Overrides the method in the base class to allow lazy parsing of
        HXL data.

        Returns:
            list: a list of hxl.model.Column objects

        """
        if self._columns is None:
            self._columns = self._find_tags()
        return self._columns

    def _find_tags(self):
        """
        Go fishing for the HXL hashtag row in the first 25 rows.
        """

        logger.debug("Scanning first 25 rows for HXL hashtags")
        previous_row = []
        try:
            for n in range(0,25):
                logger.debug("Looking for hashtags in row %d", n)
                raw_row = self._get_row()
                columns = hxl.model.Column.parse_list(raw_row, previous_row)
                if columns is not None:
                    logger.debug("HXL hashtags found in row %d", n)
                    return columns
                previous_row = raw_row
        except StopIteration:
            pass
        raise HXLTagsNotFoundException()

    def _get_row(self):
        """Parse a row of raw CSV data.  Returns an array of strings."""
        self._source_row_number += 1
        return next(self._iter)

    class _HXLIter:
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


def from_spec(spec, allow_local_ok=False):
    """Build a full spec (including source) from a JSON-like data structure.

    The JSON spec can have the following top-level properties:

    - **input:** the source-data location (which is typically a URL or a nested JSON spec)
    - **allow_local:** if 1 (true), allow local filenames
    - **sheet_index:** the 0-based index of a sheet in an Excel workbook
    - **timeout:** the number of seconds to wait before timing out an HTTP connection
    - **verify_ssl:** if 0 (false), do not verify SSL certificates. This is useful for self-signed certificates.
    - **http_headers:** an object (dictionary) of HTTP headers and values, e.g. for authorization.
    - **encoding:** the character encoding to use (e.g. "utf-8")
    - **tagger:** optional information for adding HXL hashtags to a non-HXL data source
    - **recipe:** the filters to apply to the HXL data

    The _tagger_ spec is an object with the following properties:

    - **match_all:** if 1 (true), require complete matches for headers
    - **default_tag:** use this HXL hashtag and attributes for any unmatched column
    - **specs:** an object where each property is a header string, and each value is a HXL hashtag spec

    Example:
    ```
    "tagger": {
        "match_all": 0,
        "specs": {
            "Country": "#country+name",
            "ISO3": "#country+code",
            "Cluster:" "#sector+cluster",
            "Organisation": "#org"
        }
    }
    ```

    The _recipe_ spec is is a list of filter objects, with different
    properties for each filter type, and is documented at
    https://github.com/HXLStandard/hxl-proxy/wiki/JSON-recipes

    Example:
    ```
    "recipe:" [
        {
            "filter": "with_rows",
            "queries": ["org=unicef"]
        },
        {
            "filter": "count",
            "tags": "adm1+name"
        }
    ]
    ```

    """

    if isinstance(spec, six.string_types):
        # a JSON string (parse it first)
        spec = json.loads(spec)

    # source
    input_spec = spec.get('input')
    allow_local = spec.get('allow_local', False) and allow_local_ok
    sheet_index = spec.get('sheet_index', None)
    timeout = spec.get('timeout', None)
    verify_ssl = spec.get('verify_ssl', True)
    http_headers = spec.get('http_headers', None)
    encoding = spec.get('encoding', None)
    expand_merged = spec.get('expand_merged', False)
    scan_ckan_resources = spec.get('scan_ckan_resources', False)

    # recipe
    tagger_spec = spec.get('tagger', None)
    recipe_spec = spec.get('recipe', [])

    if not input_spec:
        raise hxl.HXLException("No input property specified.")

    # set up the input
    input = make_input(
        raw_source=input_spec,
        input_options = InputOptions(
            allow_local=allow_local,
            sheet_index=sheet_index,
            timeout=timeout,
            verify_ssl=verify_ssl,
            http_headers=http_headers,
            encoding=encoding,
            expand_merged=expand_merged,
            scan_ckan_resources=scan_ckan_resources,
        )
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



########################################################################
# URL-munging code
#
# This is where a lot of the magic happens, figuring out how to download
# machine readable data from difference specialised URLs.
########################################################################


def munge_url(url, input_options):
    """Munge a URL to get at underlying data for well-known types.

    For example, if it's an HDX dataset, figure out the download
    link for the first resource. If it's a Kobo survey, create an
    export and get the download link (given an appropriate
    authorization header).

    This function ignores InputOptions.scan_ckan_resources -- the
    scanning happens in hxl.input.data(). So it's not exactly
    equivalent to the URL that you would get via data().

    Args:
        url (str): the original URL to munge
        input_options (InputOptions): options for reading a dataset.

    Returns:
        str: the actual direct-download URL

    Raises:
        hxl.input.HXLAuthorizationException: if the source requires some kind of authorization

    """

    #
    # Stage 1: unpack indirect links (requires extra HTTP requests)
    #

    # Is it a CKAN resource? (Assumes the v.3 API for now)
    result = re.match(CKAN_URL, url)
    if result:
        logup("Using CKAN API to dereference", {"url": url})
        url = _get_ckan_urls(result.group(1), result.group(2), result.group(3), input_options)[0]

    # Is it a Google Drive "open" URL?
    result = re.match(GOOGLE_DRIVE_URL, url)
    if result:
        logup("HEAD request for Google Drive URL", {"url": url})
        response = requests.head(url)
        if response.is_redirect:
            new_url = response.headers['Location']
            logup("Google Drive redirect", {"url": url, "redirect": new_url})
            logger.info("Following Google Drive redirect to %s", new_url)
            url = new_url

    # Is it a Kobo survey?
    result = re.match(KOBO_URL, url)
    if result:
        logup("Using KOBO API to dereference", {"url": url})
        max_export_age_seconds = 4 * 60 * 60 # 4 hours; TODO: make configurable
        url = _get_kobo_url(result.group(1), url, input_options, max_export_age_seconds)

    #
    # Stage 2: rewrite URLs to get direct-download links
    #

    # Is it a Google *Sheet*?
    result = re.match(GOOGLE_SHEETS_URL, url)
    if result and not re.search(r'/pub', url):
        if result.group(2):
            new_url = 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv&gid={1}'.format(result.group(1), result.group(2))
            logup("Rewriting Google Sheets URL", {"url": url, "rewrite_url": new_url})
            url = new_url
        else:
            new_url = 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv'.format(result.group(1))
            logup("Rewriting Google Sheets URL", {"url": url, "rewrite_url": new_url})
            url = new_url
        return url

    # Is it a Google Drive *file*?
    result = re.match(GOOGLE_FILE_URL, url)
    if not result:
        result = re.match(GOOGLE_SHEETS_XLSX_URL, url)
    if result and not re.search(r'/pub', url):
        url = 'https://drive.google.com/uc?export=download&id={}'.format(result.group(1))
        logger.info("Google Drive direct file download URL: %s", url)
        return url

    # Is it a Dropbox URL?
    result = re.match(DROPBOX_URL, url)
    if result:
        url = 'https://www.dropbox.com/s/{0}/{1}?dl=1'.format(result.group(1), result.group(2))
        logger.info("Dropbox direct-download URL: %s", url)
        return url

    # Is it a HXL Proxy saved recipe?
    result = re.match(HXL_PROXY_SAVED_URL, url)
    if result:
        url = '{0}/data/{1}.csv{2}'.format(result.group(1), result.group(2), result.group(3))
        logger.info("HXL Proxy saved-recipe URL: %s", url)
        return url

    # Is it a HXL Proxy args-based recipe?
    result = re.match(HXL_PROXY_ARGS_URL, url)
    if result:
        url = '{0}/data.csv?{1}'.format(result.group(1), result.group(2))
        logger.info("HXL Proxy direct-download URL: %s", url)
        return url

    # No changes
    return url


def _get_ckan_urls(site_url, dataset_id, resource_id, input_options):
    """Look up a CKAN download URL starting from a dataset or resource page

    If the link is to a dataset page, try the first resource. If it's
    to a resource page, look up the resource's download link. Either
    dataset_id or resource_id is required (will prefer resource_id
    over dataset_id).

    Args:
        site_url (str): the CKAN site URL (e.g. https://data.humdata.org)
        dataset_id (str): the CKAN dataset ID, or None if unavailable
        resource_id (str): the CKAN resource ID, or None if unavailable
        input_options (InputOptions): options for reading a dataset.

    Returns:
        list of str: the direct-download URL for the CKAN dataset

    """

    result_urls = []

    if resource_id:
        # CKAN resource URL
        ckan_api_query = '{}/api/3/action/resource_show?id={}'.format(site_url, resource_id)
        logup("Trying CKAN API call", {"url": ckan_api_query})
        ckan_api_result = requests.get(ckan_api_query, verify=input_options.verify_ssl, headers=input_options.http_headers).json()
        if ckan_api_result['success']:
            url = ckan_api_result['result']['url']
            logup("Found candidate URL for CKAN dataset", {"url": url})
            result_urls.append(url)
        elif ckan_api_result['error']['__type'] == 'Authorization Error':
            raise HXLAuthorizationException(
                "Not authorised to read CKAN resource (is the dataset public?): {}".format(
                    ckan_api_result['error']['message']
                ),
                url=site_url,
                is_ckan=True
            )
        else:
            raise HXLIOException(
                "Unable to read HDX resource: {}".format(
                    ckan_api_result['error']['message']
                ),
                url=site_url
            )
    else:
        # CKAN dataset (package) URL
        ckan_api_query = '{}/api/3/action/package_show?id={}'.format(site_url, dataset_id)
        ckan_api_result = requests.get(ckan_api_query, verify=input_options.verify_ssl, headers=input_options.http_headers).json()
        if ckan_api_result['success']:
            for resource in ckan_api_result['result']['resources']:
                url = resource['url']
                logup("Found candidate URL for CKAN dataset", {"url": url})
                result_urls.append(url)
        elif ckan_api_result['error']['__type'] == 'Authorization Error':
            raise HXLAuthorizationException(
                "Not authorised to read CKAN dataset (is it public?): {}".format(
                    ckan_api_result['error']['message']
                ),
                url=site_url,
                is_ckan=True
            )
        else:
            raise HXLIOException(
                "Unable to read CKAN dataset: {}".format(
                    ckan_api_result['error']['message']
                ),
                url=site_url
            )

    return result_urls


def _get_kobo_url(asset_id, url, input_options, max_export_age_seconds=14400):
    """ Create an export for a Kobo survey, then return the download link.

    This will fail unless there's an Authorization: header including a Kobo
    API token.

    Args:
        asset_id (str): the Kobo asset ID for the survey (extracted from the URL)
        max_export_age_seconds (int): maximum age to reuse an existing export (defaults to 14,400 seconds, or 4 hours)
        input_options (InputOptions): options for reading a dataset.


    Returns:
        str: the direct-download URL for the Kobo survey data export

    Raises:
        hxl.input.HXLAuthorizationException: if http_headers does not include a valid Authorization: header

    """

    # 1. Check current exports
    params = {
        "q": "source:{}".format(asset_id)
    }
    logup("Trying Kobo dataset", {"url": asset_id})
    response = requests.get(
        "https://kobo.humanitarianresponse.info/exports/",
        verify=input_options.verify_ssl,
        headers=input_options.http_headers,
        params=params
    )
    logup("Result for Kobo dataset", {"asset_id": asset_id, "status": response.status_code})
    # check for errors
    if (response.status_code == 403): # CKAN sends "403 Forbidden" for a private file
        raise HXLAuthorizationException("Access not authorized", url=url)
    else:
        response.raise_for_status()

    exports = response.json()['results']
    if len(exports) > 0:
        export = exports[-1]
        created = dateutil.parser.isoparse(export['date_created'])
        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        age_in_seconds = (now - created).total_seconds()

        # if less than four hours, and has a URL, use it (and stop here)
        if export.get('result') and (age_in_seconds < max_export_age_seconds):
            logup("Reusing existing Kobo export", {"asset_id": asset_id, "export": export['result']})
            return export['result']

    logup("Generating new Kobo export", {"asset_id": asset_id})

    # 2. Create the export in Kobo
    params = {
        "source": "https://kobo.humanitarianresponse.info/assets/{}/".format(asset_id),
        "type": "csv",
        "lang": "en",
        "fields_from_all_versions": False,
        "hierarchy_in_labels": False,
        "group_sep": ",",
    }
    response = requests.post(
        "https://kobo.humanitarianresponse.info/exports/",
        verify=input_options.verify_ssl,
        headers=http_headers,
        data=params
    )
    logup("Generated Kobo export", {"asset_id": asset_id, "status": response.status_code})
    # check for errors
    if (response.status_code == 403): # CKAN sends "403 Forbidden" for a private file
        raise HXLAuthorizationException("Access not authorized", url=url)
    else:
        response.raise_for_status()

    info_url = response.json().get("url")

    # 3. Look up the data record for the export to get the download URL

    fail_counter = 0
    while True:
        with requests_cache.disabled():
            logup("Getting info for Kobo export", {"url": info_url})
            response = requests.get(
                info_url,
                verify=input_options.verify_ssl,
                headers=http_headers
            )
            logup("Response for Kobo info", {"url": info_url, "status": response.status_code})

        # check for errors
        if (response.status_code == 403): # CKAN sends "403 Forbidden" for a private file
            raise HXLAuthorizationException("Access not authorized", url=info_url)
        else:
            response.raise_for_status()

        url = response.json().get("result")

        if url:
            logger.info("Kobo export URL: %s", url)
            return url

        fail_counter += 1
        if fail_counter > 30:
            raise HXLTimeoutException("Time out generating Kobo export (try again)", url)
        else:
            logger.warning("Kobo export not ready; will try again")
            time.sleep(2)


# end
