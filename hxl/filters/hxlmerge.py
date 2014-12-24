"""
Command function to merge multiple HXL datasets.
David Megginson
November 2014

Can use a whitelist of HXL tags, a blacklist, or both.

Usage:

  import sys
  from hxl.scripts.hxlmerge import hxlmerge

  hxlmerge(inputs=[sys.stdin], sys.stdout, tags=['#org', '#country', '#sector'])

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
import argparse
from hxl.filters import parse_tags
from hxl.parser import HXLReader

def hxlmerge(inputs, output, tags = []):
    """
    Merge multiple HXL datasets
    
    FIXME naive implementation just appends
    FIXME doesn't handle repeated tags
    FIXME doesn't handle multiple languages
    """

    if tags:
        need_tags = False
        tagset = frozenset(tags)
    else:
        need_tags = True
        tagset = set()

    parsers = []
    for input in inputs:
        parser = HXLReader(input)
        parsers.append(parser)
        if need_tags:
            tagset.update(parser.tags)

    writer = csv.writer(output)

    writer.writerow(list(tagset))

    for parser in parsers:
        for row in parser:
            values = []
            for tag in tagset:
                values.append(row.get(tag))
            writer.writerow(values)

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlmerge with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Merge multiple HXL datasets')
    parser.add_argument(
        'infile',
        help='HXL files to read.',
        nargs='+',
        type=argparse.FileType('r')
        )
    parser.add_argument(
        '-o',
        '--outfile',
        help='HXL file to write (if omitted, use standard output).',
        metavar='filename',
        type=argparse.FileType('w'),
        default=stdout
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to include',
        metavar='tag,tag...',
        type=parse_tags
        )
    args = parser.parse_args(args)

    # Call the command function
    hxlmerge(args.infile, args.outfile, tags=args.tags)

# end
