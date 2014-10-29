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

# If run as script
if __name__ == '__main__':

    def parse_value(s):
        t = s.split('=', 1)
        if not t[0].startswith('#'):
            t[0] = '#' + t[0]
        return t

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Cut columns from a HXL dataset.')
    parser.add_argument('infile', help='HXL file to read (if omitted, use standard input).', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', help='HXL file to write (if omitted, use standard output).', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-f', '--filter', help='hashtag=value pair for filtering', action='append', metavar='tag=value', default=[], type=parse_value)
    parser.add_argument('-v', '--invert', help='Show only lines *not* matching criteria', action='store_const', const=True, default=False)
    args = parser.parse_args()

    # Call the command function
    hxlfilter(args.infile, args.outfile, filter=args.filter, invert=args.invert)

# end
