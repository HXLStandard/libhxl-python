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
from hxl.model import HXLSource, HXLColumn
from hxl.parser import HXLReader, writeHXL
from hxl.filters import parse_tags

class HXLMergeFilter(HXLSource):
    """
    Composable filter class to merge values from two HXL datasets.

    This is the class supporting the hxlmerge command-line utility.

    Warning: this filter may store a large amount of data in memory, depending on the merge.

    Because this class is a {@link hxl.model.HXLSource}, you can use
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

    def __init__(self, source, merge_source, keys, tags):
        """
        Constructor.
        @param source the HXL data source.
        @param merge_source a second HXL data source to merge into the first.
        @param keys the shared key hashtags to use for the merge
        @param tags the tags to include from the second dataset
        """
        self.source = source
        self.merge_source = merge_source
        self.keys = keys
        self.merge_tags = tags
        self.saved_columns = None
        self.merge_map = None
        self.empty_result = [''] * len(tags)

    @property
    def columns(self):
        """
        @return column definitions for the merged dataset
        """
        if self.saved_columns is None:
            self.saved_columns = self.source.columns + map(lambda tag: HXLColumn(hxlTag=tag), self.merge_tags)
        return self.saved_columns

    def next(self):
        """
        @return the next merged row of data
        """
        if self.merge_map is None:
            self.merge_map = self._read_merge()
        row = copy(self.source.next())
        merge_values = self.merge_map.get(self._make_key(row))
        if not merge_values:
            merge_values = self.empty_result
        row.values = row.values + merge_values
        return row

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
    args = parser.parse_args(args)

    source = HXLReader(args.infile)
    filter = HXLMergeFilter(source, HXLReader(args.merge), args.keys, args.tags)
    writeHXL(args.outfile, filter)

# end
