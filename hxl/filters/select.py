"""
Select rows from a HXL dataset.
David Megginson
October 2014

Supply a list of simple <hashtag><operator><value> pairs, and return
the rows in the HXL dataset that contain matches for any of them.

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import re
import operator
import argparse
from hxl.model import HXLDataProvider
from hxl.parser import HXLReader, writeHXL

class HXLSelectFilter(HXLDataProvider):
    """
    Composable filter class to select rows from a HXL dataset.

    This is the class supporting the hxlselect command-line utility.

    Because this class is a {@link hxl.model.HXLDataProvider}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = HXLSelectFilter(source, queries=[('#org', operator.eq, 'OXFAM')])
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, queries=[], reverse=False):
        """
        Constructor
        @param source the HXL data source
        @param queries a series for parsed queries
        @param reverse True to reverse the sense of the select
        """
        self.source = source
        self.queries = queries
        self.reverse = reverse

    @property
    def columns(self):
        return self.source.columns

    def __next__(self):
        """
        Return the next row that matches the select.
        """
        row = next(self.source)
        while not self._row_matches_p(row):
            row = next(self.source)
        return row

    next = __next__

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
        for query in self.queries:
            values = row.getAll(query[0])
            if values:
                for value in values:
                    op = query[1]
                    if value and self._try_op(op, value, query[2]):
                        return not self.reverse
        return self.reverse

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

def parse_query(s):
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

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlselect with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

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
        '-q',
        '--query',
        help='query expression for selecting rows (use multiple times for logical OR): <hashtag><op><value>, where <op> is =, !=, <, <=, >, >=, ~, or !~',
        action='append',
        metavar='tag=value, etc.',
        default=[],
        type=parse_query
        )
    parser.add_argument(
        '-r',
        '--reverse',
        help='Show only lines *not* matching criteria',
        action='store_const',
        const=True,
        default=False
        )
    args = parser.parse_args(args)

    with args.infile, args.outfile:
        source = HXLReader(args.infile)
        filter = HXLSelectFilter(source, queries=args.query, reverse=args.reverse)
        writeHXL(args.outfile, filter)

# end
