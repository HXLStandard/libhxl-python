"""
Sort a HXL dataset.
David Megginson
January 2015

The SortFilter class will use numeric sorting for hashtags ending
in _num or _deg, and date-normalised sorting for hashtags ending in
_date.

Warning: this filter reads the entire source dataset into memory
before sorting it.

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import argparse
import dateutil.parser
from hxl.model import DataProvider, TagPattern
from hxl.io import StreamInput, HXLReader, write_hxl
from hxl.filters import make_input, make_output

class SortFilter(DataProvider):
    """
    Composable filter class to sort a HXL dataset.

    This is the class supporting the hxlsort command-line utility.

    Because this class is a {@link hxl.model.DataProvider}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = SortFilter(source, tags=[TagPattern.parse('#sector'), TagPattern.parse('#org'), TagPattern.parse('#adm1']))
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, tags=[], reverse=False):
        """
        @param source a HXL data source
        @param tags list of TagPattern objects for sorting
        @param reverse True to reverse the sort order
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

    def __next__(self):
        """
        Sort the dataset first, then return it row by row.
        """

        # Closures
        def make_key(row):
            """Closure: use the requested tags as keys (if provided). """

            def get_value(pattern):
                """Closure: extract a sort key"""
                raw_value = pattern.get_value(row)
                if pattern.tag.endswith('_num') or pattern.tag.endswith('_deg'):
                    # left-pad numbers with zeros
                    try:
                        raw_value = str(float(raw_value)).zfill(15)
                    except ValueError:
                        pass
                elif pattern.tag.endswith('_date'):
                    # normalise dates for sorting
                    raw_value = dateutil.parser.parse(raw_value).strftime('%Y-%m-%d')
                return raw_value.upper() if raw_value else ''

            if self.sort_tags:
                return list(map(get_value, self.sort_tags))
            else:
                return row.values

        # Main method
        if self._iter is None:
            self._iter = iter(sorted(self.source, key=make_key, reverse=self.reverse))
        return next(self._iter)

    next = __next__

            
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
        nargs='?'
        )
    parser.add_argument(
        'outfile',
        help='HXL file to write (if omitted, use standard output).',
        nargs='?'
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of tags to for columns to use as sort keys.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
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

    with make_input(args.infile, stdin) as input, make_output(args.outfile, stdout) as output:
        source = HXLReader(input)
        filter = SortFilter(source, args.tags, args.reverse)
        write_hxl(output.output, filter)

# end
