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


def run(args, stdin=sys.stdin, stdout=sys.stdout):

    def parse_value(s):
        t = s.split('=', 1)
        if not t[0].startswith('#'):
            t[0] = '#' + t[0]
            return t

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Filter rows in a HXL dataset.')
    parser.add_argument(
        'infile',
        help='HXL file to read (if omitted, use standard input).',
        nargs='?',
        type=argparse.FileType('r'),
        default=stdin
        )
    parser.add_argument(
        'outfile',
        help='HXL file to write (if omitted, use standard output).',
        nargs='?',
        type=argparse.FileType('w'),
        default=stdout
        )
    parser.add_argument(
        '-f',
        '--filter',
        help='hashtag=value pair for filtering (use multiple times for logical OR)',
        action='append',
        metavar='tag=value',
        default=[],
        type=parse_value
        )
    parser.add_argument(
        '-v',
        '--invert',
        help='Show only lines *not* matching criteria',
        action='store_const',
        const=True,
        default=False
        )
    args = parser.parse_args(args)

    # Call the command function
    hxlfilter(args.infile, args.outfile, filter=args.filter, invert=args.invert)

# end
