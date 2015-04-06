"""
Command function to normalise a HXL dataset.
David Megginson
October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import re
import dateutil.parser
import argparse
import copy
from hxl.model import DataProvider, TagPattern
from hxl.io import HXLReader, write_hxl, StreamInput
from hxl.filters import make_input, make_output

class CleanFilter(DataProvider):
    """
    Filter for cleaning values in HXL data.
    Can normalise whitespace, convert to upper/lowercase, and fix dates and numbers.
    TODO: clean up lat/lon coordinates
    """

    def __init__(self, source, whitespace=False, upper=[], lower=[], date=[], number=[]):
        """
        Construct a new data-cleaning filter.
        @param source the HXLDataSource
        @param whitespace list of TagPatterns for normalising whitespace, or True to normalise all.
        @param upper list of TagPatterns for converting to uppercase, or True to convert all.
        @param lower list of TagPatterns for converting to lowercase, or True to convert all.
        @param lower list of TagPatterns for normalising dates, or True to normalise all ending in "_date"
        @param lower list of TagPatterns for normalising numbers, or True to normalise all ending in "_num"
        """
        self.source = source
        self.whitespace = whitespace
        self.upper = upper
        self.lower = lower
        self.date = date
        self.number = number

    @property
    def columns(self):
        """Pass on the source columns unmodified."""
        return self.source.columns

    def __iter__(self):
        return CleanFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __next__(self):
            """Return the next row, with values cleaned as needed."""
            # TODO implement a lazy copy
            row = copy.copy(next(self.iterator))
            for i in range(min(len(row.values), len(row.columns))):
                row.values[i] = self._clean_value(row.values[i], row.columns[i])
            return row

        next = __next__

        def _clean_value(self, value, column):
            """Clean a single HXL value."""

            # TODO prescan columns at start for matches

            # Whitespace (-w or -W)
            if self._match_patterns(self.outer.whitespace, column):
                value = re.sub('^\s+', '', value)
                value = re.sub('\s+$', '', value)
                value = re.sub('\s+', ' ', value)

            # Uppercase (-u)
            if self._match_patterns(self.outer.upper, column):
                if sys.version_info[0] > 2:
                    value = value.upper()
                else:
                    value = value.decode('utf8').upper().encode('utf8')

            # Lowercase (-l)
            if self._match_patterns(self.outer.lower, column):
                if sys.version_info[0] > 2:
                    value = value.lower()
                else:
                    value = value.decode('utf8').lower().encode('utf8')

            # Date (-d or -D)
            if self._match_patterns(self.outer.date, column, '_date'):
                if value:
                    value = dateutil.parser.parse(value).strftime('%Y-%m-%d')

            # Number (-n or -N)
            if self._match_patterns(self.outer.number, column, '_num') and re.match('\d', value):
                if value:
                    value = re.sub('[^\d.]', '', value)
                    value = re.sub('^0+', '', value)
                    value = re.sub('(\..*)0+$', '\g<1>', value)
                    value = re.sub('\.$', '', value)

            return value

        def _match_patterns(self, patterns, column, extension=None):
            """Test if a column matches a list of patterns."""
            if not patterns:
                return False
            elif patterns is True:
                # if there's an extension specific like "_date", must match it
                return (column.tag and (not extension or column.tag.endswith(extension)))
            else:
                for pattern in patterns:
                    if pattern.match(column):
                        return True
                return False


def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlclean with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Clean data in a HXL file.')
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
        '-W',
        '--whitespace-all',
        help='Normalise whitespace in all columns',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-w',
        '--whitespace',
        help='Comma-separated list of tags for normalised whitespace.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-u',
        '--upper',
        help='Comma-separated list of tags to convert to uppercase.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-l',
        '--lower',
        help='Comma-separated list of tags to convert to lowercase.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-D',
        '--date-all',
        help='Normalise all dates.',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-d',
        '--date',
        help='Comma-separated list of tags for date normalisation.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-N',
        '--number-all',
        help='Normalise all numbers.',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-n',
        '--number',
        help='Comma-separated list of tags for number normalisation.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-r',
        '--remove-headers',
        help='Remove text header row above HXL hashtags',
        action='store_const',
        const=False,
        default=True
        )
    args = parser.parse_args(args)
    
    with make_input(args.infile, stdin) as input, make_output(args.outfile, stdout) as output:

        if args.whitespace_all:
            whitespace_arg = True
        else:
            whitespace_arg = args.whitespace

        if args.date_all:
            date_arg = True
        else:
            date_arg = args.date

        if args.number_all:
            number_arg = True
        else:
            number_arg = args.number

        source = HXLReader(input)
        filter = CleanFilter(source, whitespace=whitespace_arg, upper=args.upper, lower=args.lower, date=date_arg, number=number_arg)
        write_hxl(output.output, filter, args.remove_headers)

# end
