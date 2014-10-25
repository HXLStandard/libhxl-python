"""
Script to normalise a HXL dataset.
David Megginson
October 2014

Expand all compact-disaggregated columns.
Strip columns without hashtags.
Strip leading and trailing whitespace from values.
Strip all but one pre-tag header row.

Command-line usage:

  python -m hxl.scripts.normalize < DATA_IN.csv > DATA_OUT.csv

(Use -h option to get all options.)

Program usage:

  import sys
  from hxl.scripts.normalize import normalize

  normalize(sys.stdin, sys.stdout, show_headers = true)

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
import argparse
from hxl.parser import HXLReader

def normalize(input, output, show_headers = False, include_tags = [], exclude_tags = []):
    """
    Normalize a HXL dataset
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

    if (show_headers):
        writer.writerow(restrict_tags(parser.headers))
    writer.writerow(restrict_tags(parser.tags))

    for row in parser:
        writer.writerow(restrict_tags(row.values))

# If run as script
if __name__ == '__main__':

    def parse_tags(s):
        '''Parse tags out from a comma-separated list'''
        def fix_tag(t):
            '''trim whitespace and add # if needed'''
            t = t.strip()
            if not t.startswith('#'):
                t = '#' + t
            return t
        return map(fix_tag, s.split(','))

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Normalize a HXL file.')

    parser.add_argument('infile', help='HXL file to read (if omitted, use standard input).', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', help='HXL file to write (if omitted, use standard output).', nargs='?', type=argparse.FileType('w'), default=sys.stdout)

    parser.add_argument('-H', '--headers', help='Preserve text header row above HXL hashtags', action='store_const', const=True, default=False);
    parser.add_argument('-i', '--include-tags', help='Comma-separated list of column tags to include', metavar='tag,tag...', type=parse_tags)
    parser.add_argument('-e', '--exclude-tags', help='Comma-separated list of column tags to exclude', metavar='tag,tag...', type=parse_tags)

    args = parser.parse_args()

    normalize(args.infile, args.outfile, show_headers=args.headers, include_tags=args.include_tags, exclude_tags=args.exclude_tags)

# end
