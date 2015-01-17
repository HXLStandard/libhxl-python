"""
Add constant values to a HXL dataset.
David Megginson
January 2015

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
import re
from copy import copy
from hxl.model import HXLSource, HXLColumn
from hxl.parser import HXLReader, writeHXL
from hxl.filters import parse_tags

class HXLAddFilter(HXLSource):
    """
    Composable filter class to add constant values to every row of a HXL dataset.

    This is the class supporting the hxladd command-line utility.

    Because this class is a {@link hxl.model.HXLSource}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = HXLAddFilter(source, values={'#report_date': '2015-03-01', '#country': 'Kenya'})
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, values):
        """
        @param source a HXL data source
        @param include_tags a whitelist list of hashtags to include
        @param exclude_tags a blacklist of hashtags to exclude
        """
        self.source = source
        self.values = values
        self.columns_out = None

    @property
    def columns(self):
        """
        Add the constant columns to the end.
        """
        if self.columns_out is None:
            self.columns_out = copy(self.source.columns)
            for tag in self.values:
                self.columns_out.append(HXLColumn(hxlTag=tag))
        return self.columns_out

    def next(self):
        """
        Return the next row, with constant values added.
        """
        row = copy(self.source.next())
        for tag in self.values:
            row.values.append(self.values[tag])
        return row

#
# Command-line support
#

def parse_value(s):
    """Parse a tag=value statement."""
    result = re.match('^#?([a-zA-Z][a-zA-Z0-9_]*)=(.*)$', s)
    if result:
        items = list(result.group(1,2))
        # make sure the tag starts with '#'
        if not items[0].startswith('#'):
            items[0] = '#' + items[0]
        return items
    else:
        print >>stderr, "Bad value expression: " + s
        exit(2)

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxladd with command-line arguments.
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
        '-v',
        '--value',
        help='Constant value to add to each row',
        metavar='tag1=value,tag2=value,...',
        action='append',
        required=True,
        type=parse_value
        )
    args = parser.parse_args(args)

    # Call the command function
    source = HXLReader(args.infile)
    filter = HXLAddFilter(source, values=dict(args.value))
    writeHXL(args.outfile, filter)

# end
