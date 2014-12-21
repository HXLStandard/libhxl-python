"""
Command function to filter rows columns from a HXL dataset.
David Megginson
October 2014

Supply a list of simple <hashtag><operator><value> pairs, and return
the rows in the HXL dataset that contain matches for any of them.

Usage:

  import sys
  import operator
  from hxl.scripts.hxlfilter import hxlfilter

  hxlfilter(sys.stdin, sys.stdout, [('#country', operator.eq, 'Colombia'), ('#sector', operator.eq, 'WASH'_)]

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import re
import operator
import csv
import argparse
from hxl.parser import HXLReader

def hxlfilter(input, output, filters=[], invert=False):
    """
    Filter rows from a HXL dataset
    Uses a logical OR (use multiple instances in a pipeline for logical AND).
    @param input The input stream
    @param output The output stream
    @param filter A list of filter expressions
    @param invert True if the command should output lines that don't match.
    """

    def try_op(op, v1, v2):
        """Try an operator as numeric first, then string"""
        # TODO add dates
        # TODO use knowledge about HXL tags
        try:
            return op(float(v1), float(v2))
        except ValueError:
            return op(v1, v2)

    def row_matches_p(row):
        """Check if a key-value pair appears in a HXL row"""
        for filter in filters:
            values = row.getAll(filter[0])
            if values:
                for value in values:
                    op = filter[1]
                    if value and try_op(op, value, filter[2]):
                        return not invert
        return invert

    parser = HXLReader(input)
    writer = csv.writer(output)

    if parser.hasHeaders:
        writer.writerow(parser.headers)
    writer.writerow(parser.tags)

    for row in parser:
        if row_matches_p(row):
            writer.writerow(row.values)

# Map of comparison operators
operator_map = {
    '=': operator.eq,
    '!=': operator.ne,
    '<': operator.lt,
    '<=': operator.le,
    '>': operator.gt,
    '>=': operator.ge
}


def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlfilter with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    def parse_value(s):
        """
        Parse a filter expression
        """
        result = re.match('^#?([a-zA-Z][a-zA-Z0-9_]*)([<>]=?|!?=)(.*)$', s)
        if result:
           filter = list(result.group(1, 2, 3))
           # (re)add hash to start of tag
           filter[0] = '#' + filter[0]
           op = operator_map[filter[1]]
           if op:
               filter[1] = op
               return filter
        print >>stderr, "Bad filter expression: " + s
        exit(2)

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
        help='expression for filtering (use multiple times for logical OR): <hashtag><op><value>, where <op> is =, !=, <, <=, >, or >=',
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
    return hxlfilter(args.infile, args.outfile, filters=args.filter, invert=args.invert)

# end
