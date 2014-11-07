"""
Script to schema-validate a HXL dataset.
David Megginson
November 2014

Can use a whitelist of HXL tags, a blacklist, or both.

Command-line usage:

  python -m hxl.scripts.hxlvalidate --schema MYSCHEMA.csv > error-list.txt

(Use -h option to get all options.)

Program usage:

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

def hxlvalidate(input, output=sys.stdout, schema_input=None):
    parser = HXLReader(input)
    schema = loadHXLSchema(schema_input)
    print parser
    print schema
    pass

# If run as script
if __name__ == '__main__':

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Validate a HXL dataset.')
    parser.add_argument('infile', help='HXL file to read (if omitted, use standard input).', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', help='HXL file to write (if omitted, use standard output).', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-s', '--schema', help='Schema file for validating the HXL dataset.', metavar='schema', type=argparse.FileType('r'), default=None)
    args = parser.parse_args()

    # Call the command function
    hxlvalidate(input=args.infile, output=args.outfile, schema_input=args.schema)

# end
