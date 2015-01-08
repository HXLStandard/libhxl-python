"""
Filter class to cut columns from a HXL dataset.
David Megginson
October 2014

Can use a whitelist of HXL tags, a blacklist, or both.

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
from hxl.model import HXLSource, HXLRow
from hxl.parser import HXLReader, writeHXL
from hxl.filters import parse_tags

class HXLCutFilter(HXLSource):

    def __init__(self, source, include_tags=[], exclude_tags=[]):
        self.source = source
        self.include_tags = include_tags
        self.exclude_tags = exclude_tags
        self.columns_out = None

    @property
    def columns(self):
        if self.columns_out is None:
            self.columns_out = []
            for column in self.source.columns:
                if self._test_column(column):
                    self.columns_out.append(column)
        return self.columns_out

    def next(self):
        row = self.source.next()
        values_out = []
        for pos, value in enumerate(row):
            if self._test_column(row.columns[pos]):
                values_out.append(value)
        row_out = HXLRow(self.columns)
        row_out.values = values_out
        return row_out

    def _test_column(self, column):
        return ((not self.include_tags) or (column.hxlTag in self.include_tags)) and ((not self.exclude_tags) or (column.hxlTag not in self.exclude_tags))

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcut with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Cut columns from a HXL dataset.')
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
        '-i',
        '--include',
        help='Comma-separated list of column tags to include',
        metavar='tag,tag...',
        type=parse_tags
        )
    parser.add_argument(
        '-x',
        '--exclude',
        help='Comma-separated list of column tags to exclude',
        metavar='tag,tag...',
        type=parse_tags
        )
    args = parser.parse_args(args)

    # Call the command function
    source = HXLReader(args.infile)
    filter = HXLCutFilter(source, args.include, args.exclude)
    writeHXL(args.outfile, filter)

# end
