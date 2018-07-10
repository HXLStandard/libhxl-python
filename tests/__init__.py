import logging
import os
import re
import unittest.mock

# Default to turning off all but critical logging messages
logging.basicConfig(level=logging.CRITICAL)

def mock_open_url(url, allow_local=False, timeout=None, verify_ssl=True):
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
    return (open(path, 'rb'), None, None, None)

def resolve_path(filename):
    """Resolve a pathname for a test input file."""
    return os.path.join(os.path.dirname(__file__), filename)

# Target function to replace for mocking URL access.
URL_MOCK_TARGET = 'hxl.io.open_url_or_file'

# Mock object to replace hxl.io.make_stream
URL_MOCK_OBJECT = unittest.mock.Mock()
URL_MOCK_OBJECT.side_effect = mock_open_url
