"""
Script to normalise a HXL dataset.
David Megginson
October 2014

Expand all compact-disaggregated columns.
Strip columns without hashtags.
Strip leading and trailing whitespace from values.
Strip all but one pre-tag header row.

Usage:

  python -m hxl.scripts.normalize < DATA_IN.csv > DATA_OUT.csv

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
from hxl.parser import HXLReader
from hxl.writer import HXLWriter

parser = HXLReader(sys.stdin)
writer = HXLWriter(sys.stdout)

is_first = True
for row in parser:
    if is_first:
        writer.writeHeaders(row)
        writer.writeTags(row)
        is_first = False
    writer.writeData(row)

# end
