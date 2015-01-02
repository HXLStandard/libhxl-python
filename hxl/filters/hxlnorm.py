"""
Command function to normalise a HXL dataset.
David Megginson
October 2014

Expand all compact-disaggregated columns.
Strip columns without hashtags.
Strip leading and trailing whitespace from values.
Strip all but one pre-tag header row.

Usage:

  import sys
  from hxl.scripts.hxlnorm import hxlnorm

  hxlnorm(sys.stdin, sys.stdout, show_headers = true)

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
import re
import argparse
from hxl.parser import HXLReader

def hxlnorm(input, output, show_headers = False, include_tags = [], exclude_tags = [], whitespace = True):
    """
    Normalize a HXL dataset
    """

    parser = HXLReader(input)
    writer = csv.writer(output)

    tags = parser.tags

    if (show_headers):
        writer.writerow(parser.headers)
    writer.writerow(parser.tags)

    for row in parser:
        values = row.values
        if whitespace is True:
            values = map(norm_whitespace, values)
        writer.writerow(values)

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlfilter with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Normalize a HXL file.')
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
        '-H',
        '--headers',
        help='Preserve text header row above HXL hashtags',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-W',
        '--whitespace-all',
        help='Normalise whitespace in all columns',
        action='store_const',
        const=True,
        default=False
        )
    args = parser.parse_args(args)

    # Call the command function
    hxlnorm(args.infile, args.outfile, show_headers=args.headers, whitespace=args.whitespace_all)


def norm_whitespace(s):
    """
    Normalise whitespace in a string.

    Remove all leading and trailing space, and reduce internal whitespace.
    """
    s = re.sub('^\s+', '', s)
    s = re.sub('\s+$', '', s)
    s = re.sub('\s+', ' ', s)
    return s

# end
