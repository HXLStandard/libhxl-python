"""
Top-level package/module for libhxl.
David Megginson
Started February 2015

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import re
from hxl.common import TOKEN, HXLException
from hxl.model import DataProvider
from hxl.io import HXLReader, ArrayInput, StreamInput

def hxl(data):
    """
    Convenience method for reading a HXL dataset.
    If passed an existing DataProvider, simply returns it.
    @param data a HXL data provider, file object, array, or string (representing a URL or file name).
    """

    if isinstance(data, DataProvider):
        # it's already HXL data
        return data

    elif hasattr(data, 'read'):
        # it's a file stream
        return HXLReader(StreamInput(data))

    elif hasattr(data, '__len__') and (not isinstance(data, str)):
        # it's an array
        return HXLReader(ArrayInput(data))

    elif re.match(r'\.xlsx?', data):
        return HXLReader(ExcelInput(data))
    
    else:
        return HXLReader(CSVInput(data))

# end


        
