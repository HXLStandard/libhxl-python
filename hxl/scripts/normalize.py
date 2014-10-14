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

Program usage:

  import sys
  from hxl.scripts.normalize import normalize

  normalize(sys.stdin, sys.stdout)

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
from hxl.parser import HXLReader
from hxl.writer import HXLWriter

def normalize(input, output):
    """
    Normalize a HXL dataset
    """

    parser = HXLReader(input)
    writer = HXLWriter(output)
    is_first = True

    for row in parser:
        if is_first:
            writer.writeHeaders(row)
            writer.writeTags(row)
            is_first = False
        writer.writeData(row)

# If run as script
if __name__ == '__main__':
    normalize(sys.stdin, sys.stdout)

# end
