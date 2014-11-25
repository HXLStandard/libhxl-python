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

import csv
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

# end
