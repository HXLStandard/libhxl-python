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
import six
from copy import copy

import hxl
from . import HXLFilterException
from hxl.model import Dataset, TagPattern, Column
from hxl.io import StreamInput, HXLReader, write_hxl
from hxl.filters import make_input, make_output

class AddFilter(Dataset):
    """
    Composable filter class to add constant values to every row of a HXL dataset.

    This is the class supporting the hxladd command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.
    """

    def __init__(self, source, values, before=False):
        """
        @param source a HXL data source
        @param values a sequence of pairs of Column objects and constant values
        @param before True to add new columns before existing ones
        """
        self.source = source
        if isinstance(values, six.string_types):
            values = [values]
        self.values = [parse_value(value) for value in values]
        self.before = before
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

    def __iter__(self):
        return AddFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __next__(self):
            """
            Return the next row, with constant values added.
            """
            row = copy(next(self.iterator))
            row.columns = self.outer.columns
            if self.outer.before:
                row.values = self.outer._const_values + row.values
            else:
                row.values = row.values + self.outer._const_values
            return row

        next = __next__


#
# Command-line support
#

VALUE_PATTERN = r'^\s*(?:([^#]*)#)?({token})=(.*)\s*$'.format(token=hxl.common.TOKEN)

def parse_value(s):
    if not isinstance(s, six.string_types):
        return s
    result = re.match(VALUE_PATTERN, s)
    if result:
        header = result.group(1)
        tag = '#' + result.group(2)
        value = result.group(3)
        return (Column(tag=tag, header=header), value)
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
        filter = AddFilter(source, values=args.value, before=args.before)
        write_hxl(output.output, filter)

# end
