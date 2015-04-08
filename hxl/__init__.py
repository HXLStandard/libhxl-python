"""
Top-level package/module for libhxl.
David Megginson
Started February 2015

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import re
from hxl.common import TOKEN, HXLException
from hxl.model import Dataset
from hxl.io import HXLReader, ArrayInput, StreamInput

def hxl(data):
    """
    Convenience method for reading a HXL dataset.
    If passed an existing Dataset, simply returns it.
    @param data a HXL data provider, file object, array, or string (representing a URL or file name).
    """

    if isinstance(data, Dataset):
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


        
