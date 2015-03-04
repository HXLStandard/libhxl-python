"""
Add constant values to a HXL dataset.
David Megginson
January 2015

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import argparse
import re
from copy import copy

from . import HXLFilterException
from hxl.model import HXLDataProvider, HXLColumn
from hxl.io import StreamInput, HXLReader, writeHXL
from hxl.filters import TagPattern, make_input, make_output

class HXLAddFilter(HXLDataProvider):
    """
    Composable filter class to add constant values to every row of a HXL dataset.

    This is the class supporting the hxladd command-line utility.

    Because this class is a {@link hxl.model.HXLDataProvider}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    date_column = HXLColumn(tag='#report_date', header='Date reported')
    country_column = HXLColumn(tag='#country', header='Country name')
    filter = HXLAddFilter(source, values=[(date_column, '2015-03-03'), (country_column, 'Kenya')])
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, values, before=False):
        """
        @param source a HXL data source
        @param values a sequence of pairs of HXLColumn objects and constant values
        @param before True to add new columns before existing ones
        """
        self.source = source
        self.before = before
        self.values = values
        self._columns_out = None

    @property
    def columns(self):
        """
        Add the constant columns to the end.
        """
        if self._columns_out is None:
            new_columns = [value[0] for value in self.values]
            if self.before:
                self._columns_out = new_columns + self.source.columns
            else:
                self._columns_out = self.source.columns + new_columns
            # constant values to add
            self._const_values = [value[1] for value in self.values]
        return self._columns_out

    def __next__(self):
        """
        Return the next row, with constant values added.
        """
        row = copy(next(self.source))
        row.columns = self.columns
        if self.before:
            row.values = self._const_values + row.values
        else:
            row.values = row.values + self._const_values
        return row

    next = __next__


#
# Command-line support
#

def parse_value(s):
    result = re.match(r'^(?:([^#]*)#)?([^=]+)=(.*)$', s)
    if result:
        header = result.group(1)
        tag = '#' + result.group(2)
        value = result.group(3)
        return (HXLColumn(tag=tag, header=header), value)
    else:
        raise HXLFilterException("Badly formatted --value: " + s)

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxladd with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Add new columns with constant values to a HXL dataset.')
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
        '-v',
        '--value',
        help='Constant value to add to each row',
        metavar='[[Text header]#]<tag>=<value>',
        action='append',
        required=True,
        type=parse_value
        )
    parser.add_argument(
        '-b',
        '--before',
        help='Add new columns before existing ones rather than after them.',
        action='store_const',
        const=True,
        default=False
    )
        
    args = parser.parse_args(args)

    with make_input(args.infile, stdin) as input, make_output(args.outfile, stdout) as output:
        source = HXLReader(input)
        filter = HXLAddFilter(source, values=args.value, before=args.before)
        writeHXL(output.output, filter)

# end
