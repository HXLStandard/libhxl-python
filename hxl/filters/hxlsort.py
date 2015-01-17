"""
Sort a HXL dataset.
David Megginson
January 2015

Warning: this filter reads the entire source dataset into memory
before sorting it.

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
from hxl.model import HXLSource
from hxl.parser import HXLReader, writeHXL
from hxl.filters import parse_tags

class HXLSortFilter(HXLSource):
    """
    Composable filter class to sort a HXL dataset.

    This is the class supporting the hxlsort command-line utility.

    Because this class is a {@link hxl.model.HXLSource}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = HXLSortFilter(source, tags=['#sector', '#org', '#adm1'])
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, tags=[], reverse=False):
        """
        @param source a HXL data source
        @param include_tags a whitelist list of hashtags to include
        @param exclude_tags a blacklist of hashtags to exclude
        """
        self.source = source
        self.sort_tags = tags
        self.reverse = reverse
        self._iter = None

    @property
    def columns(self):
        """
        Return the same columns as the source.
        """
        return self.source.columns

    def next(self):
        """
        Sort the dataset first, then return it row by row.
        """
        def make_key(row):
            """
            Use the requested tags as keys (if provided).
            """
            if self.sort_tags:
                return map(lambda tag: row.get(tag), self.sort_tags)
            else:
                return row.values
        if self._iter is None:
            self._iter = iter(sorted(self.source, key=make_key, reverse=self.reverse))
        return self._iter.next()
            
#
# Command-line support
#

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcut with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Sort a HXL dataset.')
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
        '-t',
        '--tags',
        help='Comma-separated list of tags to for columns to use as sort keys.',
        metavar='tag,tag...',
        type=parse_tags
        )
    parser.add_argument(
        '-r',
        '--reverse',
        help='Flag to reverse sort order.',
        action='store_const',
        const=True,
        default=False
        )
    args = parser.parse_args(args)

    # Call the command function
    source = HXLReader(args.infile)
    filter = HXLSortFilter(source, args.tags, args.reverse)
    writeHXL(args.outfile, filter)

# end
