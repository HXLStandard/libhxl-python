"""
Command function to cut columns from a HXL dataset.
David Megginson
October 2014

Can use a whitelist of HXL tags, a blacklist, or both.

Usage:

  import sys
  from hxl.scripts.hxlcut import hxlcut

  hxlcut(sys.stdin, sys.stdout, include_tags=['#org', '#country', '#sector'])

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
import csv
from hxl.parser import HXLReader
from hxl.filters import parse_tags

def hxlcut(input, output, include_tags = [], exclude_tags = []):
    """
    Cut columns from a HXL dataset
    """

    parser = HXLReader(input)
    writer = csv.writer(output)

    tags = parser.tags

    def restrict_tags(list_in):
        '''Apply include_tags and exclude_tags to the columns'''
        list_out = []
        for i, e in enumerate(list_in):
            if ((not include_tags) or (tags[i] in include_tags)) and ((not exclude_tags) or (tags[i] not in exclude_tags)):
                list_out.append(e)
        return list_out

    if parser.hasHeaders:
        writer.writerow(restrict_tags(parser.headers))
    writer.writerow(restrict_tags(parser.tags))

    for row in parser:
        writer.writerow(restrict_tags(row.values))

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
        '-i',
        '--include',
        help='Comma-separated list of column tags to include',
        metavar='tag,tag...',
        type=parse_tags
        )
    parser.add_argument(
        '-x',
        '--exclude',
        help='Comma-separated list of column tags to exclude',
        metavar='tag,tag...',
        type=parse_tags
        )
    args = parser.parse_args(args)

    # Call the command function
    hxlcut(args.infile, args.outfile, include_tags=args.include, exclude_tags=args.exclude)

# end
