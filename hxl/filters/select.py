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
from hxl.filters import TagPattern
from hxl.model import HXLDataProvider
from hxl.io import StreamInput, HXLReader, writeHXL


def operator_re(s, pattern):
    """Regular-expression comparison operator."""
    return re.match(pattern, s)

def operator_nre(s, pattern):
    """Regular-expression negative comparison operator."""
    return not re.match(pattern, s)


class Query(object):
    """Query to execute against a row of HXL data."""

    def __init__(self, pattern, op, value):
        self.pattern = pattern
        self.op = op
        self.value = value

    def match_row(self, row):
        """Check if a key-value pair appears in a HXL row"""
        for i in range(len(row.columns)):
            if self.pattern.match(row.columns[i]):
                if row.values[i] and self.match_value(row.values[i]):
                    return True
        return False

    def match_value(self, value):
        """Try an operator as numeric first, then string"""
        # TODO add dates
        # TODO use knowledge about HXL tags
        try:
            return self.op(float(value), float(self.value))
        except ValueError:
            return self.op(value, self.value)

    @staticmethod
    def parse(s):
        """Parse a filter expression"""
        parts = re.split(r'([<>]=?|!?=|!?~)', s, maxsplit=1)
        pattern = TagPattern.parse(parts[0])
        op = Query.operator_map[parts[1]]
        value = parts[2]
        return Query(pattern, op, value)

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
        while not self.match_row(row):
            row = next(self.source)
        return row

    next = __next__

    def match_row(self, row):
        """Check if any of the queries matches the row (implied OR)."""
        for query in self.queries:
            if query.match_row(row):
                return not self.reverse
        return self.reverse


#
# Command-line support
#

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
        type=Query.parse
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
        source = HXLReader(StreamInput(args.infile))
        filter = HXLSelectFilter(source, queries=args.query, reverse=args.reverse)
        writeHXL(args.outfile, filter)

# end
