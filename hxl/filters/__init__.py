"""
Filter submodule for libhxl.
David Megginson
Started February 2015

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
from hxl import HXLException
from hxl.model import HXLDataProvider, HXLColumn

class HXLFilterException(HXLException):
    pass

def run_script(func):
    try:
        func(sys.argv[1:], sys.stdin, sys.stdout)
    except HXLException as e:
        print >>sys.stderr, "Fatal error (" + e.__class__.__name__ + "): " + str(e.message)
        print >>sys.stderr, "Exiting ..."
        sys.exit(2)

def fix_tag(t):
    """trim whitespace and add # if needed"""
    t = t.strip()
    if not t.startswith('#'):
        t = '#' + t
    return t

def parse_tags(s):
    """Parse tags out from a comma-separated list"""
    if s:
        return list(map(fix_tag, s.split(',')))
    else:
        return None

def find_column_index(tag, columns):
    """Find the first column in a list with tag"""
    index = 0
    for column in columns:
        if column.tag == tag:
            return index
        index += 1
    return None

def find_column(tag, columns):
    """Find the first column in a list with tag"""
    index = find_column_index(tag, columns)
    if index is not None:
        return columns[index]
    else:
        return None
