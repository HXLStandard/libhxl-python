"""
Command function to merge multiple HXL datasets.
David Megginson
November 2014

Can use a whitelist of HXL tags, a blacklist, or both.

Usage:

  import sys
  from hxl.scripts.hxlmerge import hxlmerge

  hxlmerge(sys.stdin, sys.stdout, merge=open('mergefile.csv', 'r'), keys=['#sector_id'], tags=['#sector'])

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
import argparse
from hxl.filters import parse_tags
from hxl.parser import HXLReader

def hxlmerge(input, output, merge, keys, tags):
    """
    Merge multiple HXL datasets
    @input Input stream for the primary HXL file.
    @output Output stream for writing HXL.
    @merge Input stream for the HXL merge file.
    @keys Shared keys for merging HXL files.
    @tags Tags to include from the merge file.
    """

    def make_key(row):
        """
        Make a tuple key for a row.
        """
        values = []
        for key in keys:
            values.append(row.get(key))
        return tuple(values)

    def get_values(row):
        """
        Get the values to merge from a row.
        """
        values = []
        for tag in tags:
            values.append(row.get(tag))
        return values

    # Load the merge file first.
    merge_map = {}
    parser = HXLReader(merge)
    for row in parser:
        merge_map[make_key(row)] = get_values(row)

    # Now process the main file
    parser = HXLReader(input)
    writer = csv.writer(output)
    writer.writerow(parser.tags + tags)
    empty_result = [''] * len(tags)
    for row in parser:
        merge_values = merge_map.get(make_key(row))
        if not merge_values:
            merge_values = empty_result
        writer.writerow(row.values + merge_values)

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

    # Call the command function
    hxlmerge(args.infile, args.outfile, merge=args.merge, keys=args.keys, tags=args.tags)

# end
