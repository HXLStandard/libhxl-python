"""
Command function to cut columns from a HXL dataset.
David Megginson
October 2014

Can use a whitelist of HXL tags, a blacklist, or both.

Usage:

  import sys
  from hxl.scripts.hxlcut import hxlcut

  hxlcut(sys.stdin, sys.stdout, include_tags=['#org', '#country', '#sector'])

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
from hxl.parser import HXLReader

def hxlcut(input, output, include_tags = [], exclude_tags = []):
    """
    Cut columns from a HXL dataset
    """

    parser = HXLReader(input)
    writer = csv.writer(output)

    tags = parser.tags

    def restrict_tags(list_in):
        '''Apply include_tags and exclude_tags to the columns'''
        list_out = []
        for i, e in enumerate(list_in):
            if ((not include_tags) or (tags[i] in include_tags)) and ((not exclude_tags) or (tags[i] not in exclude_tags)):
                list_out.append(e)
        return list_out

    if parser.hasHeaders:
        writer.writerow(restrict_tags(parser.headers))
    writer.writerow(restrict_tags(parser.tags))

    for row in parser:
        writer.writerow(restrict_tags(row.values))

# end
