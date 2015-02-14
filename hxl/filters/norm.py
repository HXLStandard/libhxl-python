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
from copy import copy
from hxl.model import HXLDataProvider
from hxl.parser import HXLReader, writeHXL
from hxl.filters import parse_tags

class HXLNormFilter(HXLDataProvider):

    def __init__(self, source, whitespace=False, upper=[], lower=[], date=[], number=[]):
        self.source = source
        self.whitespace = whitespace
        self.upper = upper
        self.lower = lower
        self.date = date
        self.number = number

    @property
    def columns(self):
        return self.source.columns

    def __next__(self):

        def normalise(value, column):
            """
            Normalise a single HXL value.
            """

            # Whitespace (-w or -W)
            if self.whitespace:
                if (self.whitespace is True) or (column.hxlTag in self.whitespace):
                    value = re.sub('^\s+', '', value)
                    value = re.sub('\s+$', '', value)
                    value = re.sub('\s+', ' ', value)

            # Uppercase (-u)
            if self.upper and column.hxlTag in self.upper:
                if sys.version_info[0] > 2:
                    value = value.upper()
                else:
                    value = value.decode('utf8').upper().encode('utf8')

            # Lowercase (-l)
            if self.lower and column.hxlTag in self.lower:
                if sys.version_info[0] > 2:
                    value = value.lower()
                else:
                    value = value.decode('utf8').lower().encode('utf8')

            # Date (-d or -D)
            if self.date and value:
                if (self.date is True and column.hxlTag.endswith('_date')) or (self.date is not True and column.hxlTag in self.date):
                    value = dateutil.parser.parse(value).strftime('%Y-%m-%d')

            # Number (-n or -N)
            if self.number and re.match('\d', value):
                if (self.number is True and column.hxlTag.endswith('_num')) or (self.number is not True and column.hxlTag in self.number):
                    value = re.sub('[^\d.]', '', value)
                    value = re.sub('^0+', '', value)
                    value = re.sub('(\..*)0+$', '\g<1>', value)
                    value = re.sub('\.$', '', value)

            return value

        row = copy(next(self.source))
        row.values = row.map(normalise)
        return row

    next = __next__


def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlnorm with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Normalize a HXL file.')
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
        type=parse_tags
        )
    parser.add_argument(
        '-u',
        '--upper',
        help='Comma-separated list of tags to convert to uppercase.',
        metavar='tag,tag...',
        type=parse_tags
        )
    parser.add_argument(
        '-l',
        '--lower',
        help='Comma-separated list of tags to convert to lowercase.',
        metavar='tag,tag...',
        type=parse_tags
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
        type=parse_tags
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
        type=parse_tags
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
    
    with args.infile, args.outfile:

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

        source = HXLReader(args.infile)
        filter = HXLNormFilter(source, whitespace=whitespace_arg, upper=args.upper, lower=args.lower, date=date_arg, number=number_arg)
        writeHXL(args.outfile, filter, args.remove_headers)

# end
