"""
Filter submodule for libhxl.
David Megginson
Started February 2015

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
from hxl import HXLException
from hxl.model import HXLDataProvider, HXLColumn

class HXLFilterException(HXLException):
    pass

def run_script(func):
    func(sys.argv[1:], sys.stdin, sys.stdout)

def fix_tag(t):
    """trim whitespace and add # if needed"""
    t = t.strip()
    if not t.startswith('#'):
        t = '#' + t
    return t

def parse_tags(s):
    """Parse tags out from a comma-separated list"""
    return list(map(fix_tag, s.split(',')))

def find_column(tag, columns):
    """Find the first column in a list with tag"""
    for column in columns:
        if column.hxlTag == tag:
            return column
    return None
