"""
Command function to rename columns in a HXL dataset.
David Megginson
October 2014

Usage:

  import sys
  from hxl.scripts.hxlrename import hxlrename

  hxlrename(sys.stdin, sys.stdout, rename={'#oldtag1': '#newtag1', '#oldtag2', '#newtag2'})

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
import csv
from hxl.filters import fix_tag
from hxl.parser import HXLReader

def hxlrename(input, output, rename_map={}):
    """
    Cut columns from a HXL dataset
    """

    parser = HXLReader(input)
    writer = csv.writer(output)

    def rename_tags(tag):
        if tag in rename_map:
            return rename_map[tag]
        else:
            return tag

    tags = map(rename_tags, parser.tags)

    if parser.hasHeaders:
        writer.writerow(parser.headers)
    writer.writerow(tags)

    for row in parser:
        writer.writerow(row.values)

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
        '-r',
        '--rename',
        help='Rename an old tag to a new one',
        metavar='original_tag:new_tag',
        type=parse_rename,
        action='append',
        default=[]
        )
    args = parser.parse_args(args)

    # Call the command function
    hxlrename(args.infile, args.outfile, rename_map=dict(args.rename))

def parse_rename(s):
    return map(fix_tag, s.split(':'))

# end
