"""
Command function to merge multiple HXL datasets.
David Megginson
November 2014

Can use a whitelist of HXL tags, a blacklist, or both.

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import argparse
from copy import copy
from hxl.model import HXLDataProvider, HXLColumn
from hxl.io import HXLReader, writeHXL
from hxl.filters import parse_tags, find_column, find_column_index

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

    def __init__(self, source, merge_source, keys, tags, replace=False, overwrite=False):
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
        self.replace = replace
        self.overwrite = overwrite

        self.saved_columns = None
        self.merge_map = None

    @property
    def columns(self):
        """
        @return column definitions for the merged dataset
        """
        if self.saved_columns is None:
            new_columns = []
            for tag in self.merge_tags:
                if self.replace and find_column(tag, self.source.columns):
                    # will use existing column
                    continue
                else:
                    column = find_column(tag, self.merge_source.columns)
                    if column:
                        headerText = column.headerText
                    else:
                        headerText = None
                    new_columns.append(HXLColumn(hxlTag=tag, headerText=headerText))
            self.saved_columns = self.source.columns + new_columns
        return self.saved_columns

    def __next__(self):
        """
        @return the next merged row of data
        """

        # First, check if we already have the merge map, and read it if not
        if self.merge_map is None:
            self.merge_map = self._read_merge()

        # Make a copy of the next row from the source
        row = copy(next(self.source))

        # Look up the merge values, based on the --keys
        merge_values = self.merge_map.get(self._make_key(row), {})

        # Go through the --tags
        for tag in self.merge_tags:
            # Try to substitute in place?
            if self.replace:
                # the column must actually exist in the source
                index = find_column_index(tag, self.source.columns)
                if index is not None:
                    # --overwrite means replace an existing value
                    if self.overwrite or not row.values[index]:
                        row.values[index] = merge_values.get(tag)
                    # go to next tag if we made it here
                    continue

            # otherwise, fall through
            row.append(merge_values.get(tag, ''))
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
            values = {}
            for tag in self.merge_tags:
                values[tag] = row.get(tag)
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
        '-r',
        '--replace',
        help='Replace empty values in existing columns (when available) instead of adding new ones.',
        action='store_const',
        const=True,
        default=False
    )
    parser.add_argument(
        '-O',
        '--overwrite',
        help='Used with --replace, overwrite existing values.',
        action='store_const',
        const=True,
        default=False
    )
    args = parser.parse_args(args)

    # FIXME - will this be OK with stdin/stdout?
    with args.infile, args.outfile, args.merge:
        source = HXLReader(args.infile)
        filter = HXLMergeFilter(source, merge_source=HXLReader(args.merge),
                                keys=args.keys, tags=args.tags, replace=args.replace, overwrite=args.overwrite)
        writeHXL(args.outfile, filter)

# end
