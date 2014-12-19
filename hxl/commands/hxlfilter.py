"""
Command function to filter rows columns from a HXL dataset.
David Megginson
October 2014

Supply a list of tag=value pairs, and return the rows in the HXL
dataset that contain matches for any of them.

Usage:

  import sys
  from hxl.scripts.hxlfilter import hxlfilter

  hxlfilter(sys.stdin, sys.stdout, [('#country', 'Colombia), ('#sector', 'WASH'_)]

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
import argparse
from hxl.parser import HXLReader

def hxlfilter(input, output, filter=[], invert=False):
    """
    Filter rows from a HXL dataset
    Uses a logical OR (use multiple instances in a pipeline for logical AND).
    @param input The input stream
    @param output The output stream
    @param filter A list of filter expressions
    @param invert True if the command should output lines that don't match.
    """

    def row_matches_p(row):
        """Check if a key-value pair appears in a HXL row"""
        for f in filter:
            values = row.getAll(f[0])
            if not invert:
                if values and (f[1] in values):
                    return True
            else:
                if values and (f[1] in values):
                    return False
        if invert:
            return True
        else:
            return False

    parser = HXLReader(input)
    writer = csv.writer(output)

    if parser.hasHeaders:
        writer.writerow(parser.headers)
    writer.writerow(parser.tags)

    for row in parser:
        if row_matches_p(row):
            writer.writerow(row.values)

# end
