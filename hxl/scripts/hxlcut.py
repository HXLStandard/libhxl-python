"""
Script to cut columns from a HXL dataset.
David Megginson
October 2014

Can use a whitelist of HXL tags, a blacklist, or both.

Command-line usage:

  python -m hxl.scripts.hxlcut -c org,country,sector < DATA_IN.csv > DATA_OUT.csv

(Use -h option to get all options.)

Program usage:

  import sys
  from hxl.scripts.hxlcut import hxlcut

  hxlcut(sys.stdin, sys.stdout, include_tags=['#org', '#country', '#sector'])

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
import argparse
from hxl.parser import HXLReader
from . import parse_tags

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

# If run as script
if __name__ == '__main__':

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Cut columns from a HXL dataset.')
    parser.add_argument('infile', help='HXL file to read (if omitted, use standard input).', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', help='HXL file to write (if omitted, use standard output).', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-c', '--include-tags', help='Comma-separated list of column tags to include', metavar='tag,tag...', type=parse_tags)
    parser.add_argument('-C', '--exclude-tags', help='Comma-separated list of column tags to exclude', metavar='tag,tag...', type=parse_tags)
    args = parser.parse_args()

    # Call the command function
    hxlcut(args.infile, args.outfile, include_tags=args.include_tags, exclude_tags=args.exclude_tags)

# end
