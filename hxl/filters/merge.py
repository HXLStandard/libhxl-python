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
import copy
from hxl.model import DataProvider, TagPattern, Column
from hxl.io import StreamInput, HXLReader, write_hxl
from hxl.filters import make_input, make_output

class MergeFilter(DataProvider):
    """
    Composable filter class to merge values from two HXL datasets.

    This is the class supporting the hxlmerge command-line utility.

    Warning: this filter may store a large amount of data in memory, depending on the merge.

    Because this class is a {@link hxl.model.DataProvider}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    merge_source = HXLReader(open('file-to-merge.csv', 'r'))
    filter = MergeFilter(source, merge_source=merge_source, keys=['adm1_id'], tags=['adm1'])
    write_hxl(sys.stdout, filter)
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

    @property
    def columns(self):
        """
        @return column definitions for the merged dataset
        """
        if self.saved_columns is None:
            new_columns = []
            for pattern in self.merge_tags:
                if self.replace and pattern.find_column(self.source.columns):
                    # will use existing column
                    continue
                else:
                    column = pattern.find_column(self.merge_source.columns)
                    if column:
                        header = column.header
                    else:
                        header = None
                    new_columns.append(Column(tag=pattern.tag, attributes=pattern.include_attributes, header=header))
            self.saved_columns = self.source.columns + new_columns
        return self.saved_columns

    def __iter__(self):
        return MergeFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)
            self.merge_iterator = iter(outer.merge_source)
            self.merge_map = None

        def __next__(self):
            """
            @return the next merged row of data
            """

            # First, check if we already have the merge map, and read it if not
            if self.merge_map is None:
                self.merge_map = self._read_merge()

            # Make a copy of the next row from the source
            row = copy.copy(next(self.iterator))

            # Look up the merge values, based on the --keys
            merge_values = self.merge_map.get(self._make_key(row), {})

            # Go through the --tags
            for pattern in self.outer.merge_tags:
                # Try to substitute in place?
                if self.outer.replace:
                    index = pattern.find_column_index(self.outer.source.columns)
                    if index is not None:
                        if self.outer.overwrite or not row.values[index]:
                            row.values[index] = merge_values.get(pattern)
                        continue

                # otherwise, fall through
                row.append(merge_values.get(pattern, ''))
            return row

        next = __next__

        def _make_key(self, row):
            """
            Make a tuple key for a row.
            """
            values = []
            for pattern in self.outer.keys:
                values.append(pattern.get_value(row))
            return tuple(values)

        def _read_merge(self):
            """
            Read the second (merging) dataset into memory.
            Stores only the values necessary for the merge.
            @return a map of merge values
            """
            merge_map = {}
            for row in self.merge_iterator:
                values = {}
                for pattern in self.outer.merge_tags:
                    values[pattern] = pattern.get_value(row)
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
        nargs='?'
        )
    parser.add_argument(
        'outfile',
        help='HXL file to write (if omitted, use standard output).',
        nargs='?'
        )
    parser.add_argument(
        '-m',
        '--merge',
        help='HXL file to write (if omitted, use standard output).',
        metavar='filename',
        required=True
        )
    parser.add_argument(
        '-k',
        '--keys',
        help='HXL tag(s) to use as a shared key.',
        metavar='tag,tag...',
        required=True,
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to include from the merge dataset.',
        metavar='tag,tag...',
        required=True,
        type=TagPattern.parse_list
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
    with make_input(args.infile, stdin) as input, make_output(args.outfile, stdout) as output, make_input(args.merge, None) as merge:
        source = HXLReader(input)
        filter = MergeFilter(source, merge_source=HXLReader(merge),
                                keys=args.keys, tags=args.tags, replace=args.replace, overwrite=args.overwrite)
        write_hxl(output.output, filter)

# end
