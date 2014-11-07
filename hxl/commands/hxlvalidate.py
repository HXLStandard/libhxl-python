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

# end
