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
from hxl.parser import HXLReader
from hxl.schema import loadHXLSchema

def hxlvalidate(input=sys.stdin, output=sys.stderr, schema_input=None):
    parser = HXLReader(input)
    schema = loadHXLSchema(schema_input)
    return schema.validate(parser)

# end
