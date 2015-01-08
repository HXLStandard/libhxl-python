"""
Command function to rename columns in a HXL dataset.
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
import copy
from hxl.model import HXLSource
from hxl.filters import fix_tag
from hxl.parser import HXLReader, writeHXL

class HXLRenameFilter(HXLSource):
    """
    Composable filter class to rename columns in a HXL dataset.

    This is the class supporting the hxlrename command-line utility.

    Because this class is a {@link hxl.model.HXLSource}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = HXLRenameFilter(source, rename_map={'#x_district': '#adm1'})
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, rename_map={}):
        """
        Constructor
        @param source the HXLSource for the data.
        @param rename_map map of tags to rename
        """
        self.source = source
        self.rename_map = rename_map

    @property
    def columns(self):
        """
        Return the renamed columns.
        """
        def rename_tags(column):
            if column.hxlTag in self.rename_map:
                column = copy.copy(column)
                column.hxlTag = self.rename_map[column.hxlTag]
            return column
        return map(rename_tags, self.source.columns)

    def next(self):
        return self.source.next()

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
        '-r',
        '--rename',
        help='Rename an old tag to a new one',
        action='append',
        metavar='original_tag:new_tag',
        default=[],
        type=parse_rename
        )
    args = parser.parse_args(args)

    # Call the command function
    source = HXLReader(args.infile)
    filter = HXLRenameFilter(source, dict(args.rename))
    writeHXL(args.outfile, filter)

def parse_rename(s):
    return map(fix_tag, s.split(':'))

# end
