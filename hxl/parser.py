"""
Parsing library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import csv

class hxlreader:
    """Read HXL data from a file"""

    def __init__(self, source):
        self.csvreader = csv.reader(source)

    def next(self):
        return self.csvreader.next()

    def __iter__(self):
        return self;
