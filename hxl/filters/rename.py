"""
Command function to rename columns in a HXL dataset.
David Megginson
October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import argparse
import copy
import re
from hxl.model import HXLDataProvider, HXLColumn
from hxl.filters import TagPattern, make_input, make_output
from hxl.io import StreamInput, HXLReader, writeHXL

class HXLRenameFilter(HXLDataProvider):
    """
    Composable filter class to rename columns in a HXL dataset.

    This is the class supporting the hxlrename command-line utility.

    Because this class is a {@link hxl.model.HXLDataProvider}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = HXLRenameFilter(source, rename=[[TagPattern.parse('#foo'), HXLColumn.parse('#bar')]])
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, rename=[]):
        """
        Constructor
        @param source the HXLDataProvider for the data.
        @param rename_map map of tags to rename
        """
        self.source = source
        self.rename = rename
        self._saved_columns = None

    @property
    def columns(self):
        """
        Return the renamed columns.
        """

        if self._saved_columns is None:
            def rename_column(column):
                for spec in self.rename:
                    if spec[0].match(column):
                        new_column = copy.copy(spec[1])
                        if new_column.header is None:
                            new_column.header = column.header
                        return new_column
                return column
            self._saved_columns = [rename_column(column) for column in self.source.columns]
        return self._saved_columns

    def __next__(self):
        return next(self.source)

    next = __next__


#
# Command-line support.
#

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcut with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Rename and retag columns in a HXL dataset')
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
        '-r',
        '--rename',
        help='Rename an old tag to a new one (with an optional new text header).',
        action='append',
        metavar='#?<original_tag>:<Text header>?#?<new_tag>',
        default=[],
        type=parse_rename
        )
    args = parser.parse_args(args)

    with make_input(args.infile, stdin) as input, make_output(args.outfile, stdout) as output:
        source = HXLReader(input)
        filter = HXLRenameFilter(source, args.rename)
        writeHXL(output.output, filter)

def parse_rename(s):
    result = re.match(r'^\s*#?([^:]+):(?:([^#]*)#)?([^#]+)\s*$', s)
    if result:
        pattern = TagPattern.parse(result.group(1))
        column = HXLColumn.parse('#' + result.group(3), header=result.group(2), use_exception=True)
        return (pattern, column)
    else:
        raise HXLFilterException("Bad rename expression: " + s)

# end
