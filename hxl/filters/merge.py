"""
Command function to merge multiple HXL datasets.
David Megginson
November 2014

Can use a whitelist of HXL tags, a blacklist, or both.

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
from copy import copy
from hxl.model import HXLDataProvider, HXLColumn
from hxl.parser import HXLReader, writeHXL
from hxl.filters import parse_tags, find_column

class HXLMergeFilter(HXLDataProvider):
    """
    Composable filter class to merge values from two HXL datasets.

    This is the class supporting the hxlmerge command-line utility.

    Warning: this filter may store a large amount of data in memory, depending on the merge.

    Because this class is a {@link hxl.model.HXLDataProvider}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    merge_source = HXLReader(open('file-to-merge.csv', 'r'))
    filter = HXLMergeFilter(source, merge_source=merge_source, keys=['adm1_id'], tags=['adm1'])
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, merge_source, keys, tags, before=False):
        """
        Constructor.
        @param source the HXL data source.
        @param merge_source a second HXL data source to merge into the first.
        @param keys the shared key hashtags to use for the merge
        @param tags the tags to include from the second dataset
        @param before if True, add new columns before existing ones
        """
        self.source = source
        self.merge_source = merge_source
        self.keys = keys
        self.merge_tags = tags
        self.before = before
        self.saved_columns = None
        self.merge_map = None
        self.empty_result = [''] * len(tags)

    @property
    def columns(self):
        """
        @return column definitions for the merged dataset
        """
        if self.saved_columns is None:
            new_columns = []
            for tag in self.merge_tags:
                column = find_column(tag, self.merge_source.columns)
                if column:
                    headerText = column.headerText
                else:
                    headerText = None
                new_columns.append(HXLColumn(hxlTag=tag, headerText=headerText))
            if self.before:
                self.saved_columns =  new_columns + self.source.columns
            else:
                self.saved_columns = self.source.columns + new_columns
        return self.saved_columns

    def __next__(self):
        """
        @return the next merged row of data
        """
        if self.merge_map is None:
            self.merge_map = self._read_merge()
        row = copy(next(self.source))
        merge_values = self.merge_map.get(self._make_key(row))
        if not merge_values:
            merge_values = self.empty_result
        if self.before:
            row.values = merge_values + row.values
        else:
            row.values = row.values + merge_values
        return row

    next = __next__

    def _make_key(self, row):
        """
        Make a tuple key for a row.
        """
        values = []
        for key in self.keys:
            values.append(row.get(key))
        return tuple(values)

    def _read_merge(self):
        """
        Read the second (merging) dataset into memory.
        Stores only the values necessary for the merge.
        @return a map of merge values
        """
        merge_map = {}
        for row in self.merge_source:
            values = []
            for tag in self.merge_tags:
                values.append(row.get(tag))
            merge_map[self._make_key(row)] = values
        return merge_map

#
# Command-line support
#

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlmerge with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Merge part of one HXL dataset into another.')
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
        '-m',
        '--merge',
        help='HXL file to write (if omitted, use standard output).',
        metavar='filename',
        required=True,
        type=argparse.FileType('r')
        )
    parser.add_argument(
        '-k',
        '--keys',
        help='HXL tag(s) to use as a shared key.',
        metavar='tag,tag...',
        required=True,
        type=parse_tags
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to include from the merge dataset.',
        metavar='tag,tag...',
        required=True,
        type=parse_tags
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

    source = HXLReader(args.infile)
    filter = HXLMergeFilter(source, merge_source=HXLReader(args.merge), keys=args.keys, tags=args.tags, before=args.before)
    writeHXL(args.outfile, filter)

# end
