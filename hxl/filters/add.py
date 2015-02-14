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
from hxl.parser import HXLReader, writeHXL
from hxl.filters import parse_tags

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
    filter = HXLAddFilter(source, values={'#report_date': '2015-03-01', '#country': 'Kenya'})
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, values, before=False):
        """
        @param source a HXL data source
        @param values a dictionary of tags and constant values
        @param before True to add new columns before existing ones
        """
        self.source = source
        self.values = values
        self.before = before
        self._columns_out = None
        self._const_values = [self.values[tag][0] for tag in self.values]

    @property
    def columns(self):
        """
        Add the constant columns to the end.
        """
        if self._columns_out is None:
            new_columns = []
            for tag in self.values:
                new_columns.append(HXLColumn(hxlTag=tag, headerText=self.values[tag][1]))
            if self.before:
                self._columns_out = new_columns + self.source.columns
            else:
                self._columns_out = self.source.columns + new_columns
        return self._columns_out

    def __next__(self):
        """
        Return the next row, with constant values added.
        """
        row = copy(next(self.source))
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
    """Parse a tag=value statement."""
    result = re.match('^(?:(.*)#)?([a-zA-Z][a-zA-Z0-9_]*)=(.*)$', s)
    if result:
        items = [result.group(2), result.group(3, 1)]
        # make sure the tag starts with '#'
        if not items[0].startswith('#'):
            items[0] = '#' + items[0]
        return items
    else:
        raise HXLFilterException("Bad value expression: " + s)

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

    with args.infile, args.outfile:
        source = HXLReader(args.infile)
        filter = HXLAddFilter(source, values=dict(args.value), before=args.before)
        writeHXL(args.outfile, filter)

# end
