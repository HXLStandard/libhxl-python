"""
Parsing library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import csv

class HXLReader:
    """Read HXL data from a file"""

    def __init__(self, source):
        self.source = source

    def read(self):
        print 'hello'
