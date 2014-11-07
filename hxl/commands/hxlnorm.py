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
import argparse
from hxl.parser import HXLReader

def hxlnorm(input, output, show_headers = False, include_tags = [], exclude_tags = []):
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
        writer.writerow(row.values)

# end
