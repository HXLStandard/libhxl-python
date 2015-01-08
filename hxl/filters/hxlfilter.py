"""
Filter rows from a HXL dataset.
David Megginson
October 2014

Supply a list of simple <hashtag><operator><value> pairs, and return
the rows in the HXL dataset that contain matches for any of them.

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import re
import operator
import argparse
from hxl.model import HXLSource
from hxl.parser import HXLReader, writeHXL

class HXLFilterFilter(HXLSource):
    """
    Composable filter class to filter rows from a HXL dataset.

    This is the class supporting the hxlfilter command-line utility.

    Because this class is a {@link hxl.model.HXLSource}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = HXLFilterFilter(source, filters=[('#org', operator.eq, 'OXFAM')])
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, filters=[], invert=False):
        """
        Constructor
        @param source the HXL data source
        @param filters a series for parsed filters
        @param invert True to invert the sense of the filter
        """
        self.source = source
        self.filters = filters
        self.invert = invert

    @property
    def columns(self):
        return self.source.columns

    def next(self):
        """
        Return the next row that matches the filter.
        """
        row = self.source.next()
        while not self._row_matches_p(row):
            row = self.source.next()
        return row

    def _try_op(self, op, v1, v2):
        """Try an operator as numeric first, then string"""
        # TODO add dates
        # TODO use knowledge about HXL tags
        try:
            return op(float(v1), float(v2))
        except ValueError:
            return op(v1, v2)

    def _row_matches_p(self, row):
        """Check if a key-value pair appears in a HXL row"""
        for filter in self.filters:
            values = row.getAll(filter[0])
            if values:
                for value in values:
                    op = filter[1]
                    if value and self._try_op(op, value, filter[2]):
                        return not self.invert
        return self.invert

def operator_re(s, pattern):
    """Regular-expression comparison operator."""
    return re.match(pattern, s)

def operator_nre(s, pattern):
    """Regular-expression negative comparison operator."""
    return not re.match(pattern, s)

# Map of comparison operators
operator_map = {
    '=': operator.eq,
    '!=': operator.ne,
    '<': operator.lt,
    '<=': operator.le,
    '>': operator.gt,
    '>=': operator.ge,
    '~': operator_re,
    '!~': operator_nre
}

#
# Command-line support
#

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
        result = re.match('^#?([a-zA-Z][a-zA-Z0-9_]*)([<>]=?|!?=|!?~)(.*)$', s)
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
        help='expression for filtering (use multiple times for logical OR): <hashtag><op><value>, where <op> is =, !=, <, <=, >, >=, ~, or !~',
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
    source = HXLReader(args.infile)
    filter = HXLFilterFilter(source, args.filter, args.invert)
    writeHXL(args.outfile, filter)

# end
