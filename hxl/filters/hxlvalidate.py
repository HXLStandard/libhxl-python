"""
Command function to schema-validate a HXL dataset.
David Megginson
November 2014

Can use a whitelist of HXL tags, a blacklist, or both.

Usage:

  import sys
  from hxl.scripts.hxlvalidate import hxlvalidate

  hxlvalidate(sys.stdin, sys.stdout, open('MySchema.csv', 'r'))

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
from hxl.parser import HXLReader
from hxl.schema import loadHXLSchema

def hxlvalidate(input=sys.stdin, output=sys.stderr, schema_input=None):

    def callback(error):
        print >>output, error

    parser = HXLReader(input)
    schema = loadHXLSchema(schema_input)
    schema.callback = callback
    return schema.validate(parser)

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlvalidate with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Validate a HXL dataset.')
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
        '-s',
        '--schema',
        help='Schema file for validating the HXL dataset (if omitted, use the default core schema).',
        metavar='schema',
        type=argparse.FileType('r'),
        default=None
        )
    args = parser.parse_args(args)

    # Call the command function
    hxlvalidate(input=args.infile, output=args.outfile, schema_input=args.schema)

# end
