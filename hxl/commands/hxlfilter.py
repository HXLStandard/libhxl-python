"""
Script to filter rows columns from a HXL dataset.
David Megginson
October 2014

Supply a list of tag=value pairs, and return only the rows in the HXL
dataset that contain matches for all of them.

Command-line usage:

  python -m hxl.scripts.hxlfilter -f country=Colombia -f sector=WASH < DATA_IN.csv > DATA_OUT.csv

(Use -h option to get full usage.)

Program usage:

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
    """

    def row_matches_p(row):
        """Check if a key-value pair appears in a HXL row"""
        for f in filter:
            values = row.getAll(f[0])
            if not invert:
                if not values or (f[1] not in values):
                    return False
            else:
                if values and (f[1] in values):
                    return False
        return True

    parser = HXLReader(input)
    writer = csv.writer(output)

    if parser.hasHeaders:
        writer.writerow(parser.headers)
    writer.writerow(parser.tags)

    for row in parser:
        if row_matches_p(row):
            writer.writerow(row.values)

# end
