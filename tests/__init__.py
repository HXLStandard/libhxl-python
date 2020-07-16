import io, logging, os, re, socket, sys, unittest.mock, warnings

# Default to turning off all but critical logging messages
logging.basicConfig(level=logging.CRITICAL)

# But turn on system-level warnings
warnings.simplefilter("default")
os.environ["PYTHONWARNINGS"] = "default"


def mock_open_url(url, allow_local=False, timeout=None, verify_ssl=True, http_headers=None):
    """Open local files instead of URLs.
    If it's a local file path, leave it alone; otherwise,
    open as a file under ./files/

    This is meant as a side effect for unittest.mock.Mock
    """
    if re.match(r'https?:', url):
        # Looks like a URL
        filename = re.sub(r'^.*/([^/]+)$', '\\1', url)
        path = resolve_path('files/mock/' + filename)
    else:
        # Assume it's a file
        path = url
    with open(path, 'rb') as input:
        data = input.read()
    return (io.BytesIO(data), None, None, None)

def resolve_path(filename):
    """Resolve a pathname for a test input file."""
    return os.path.join(os.path.dirname(__file__), filename)

def have_connectivity(host="8.8.8.8", port=53, timeout=3):
    """ Attempt to make a DNS connection to see if we're on the Internet.
    From https://stackoverflow.com/questions/3764291/checking-network-connection
    @param host: the host IP to connect to (default 8.8.8.8, google-public-dns-a.google.com)
    @param port: the port to connect to (default 53, TCP)
    @param timeout: seconds before timeout (default 3)
    @returns: True if connected; False otherwise.
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception as ex:
        return False

# Target function to replace for mocking URL access.
URL_MOCK_TARGET = 'hxl.io.open_url_or_file'

# Mock object to replace hxl.io.make_stream
URL_MOCK_OBJECT = unittest.mock.Mock()
URL_MOCK_OBJECT.side_effect = mock_open_url
