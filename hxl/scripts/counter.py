"""
Script to count distinct values in a HXL dataset.
David Megginson
October 2014

Counts all combinations of the tags specified on the command line. In
the command-line version, you may omit the initial '#' from tag names
to avoid the need to quote them.

Only the *first* column with each hashtag is currently used.

Command-line usage:

  python -m hxl.scripts.count <tag> <tag...> < DATA_IN.csv > DATA_OUT.csv

Program usage:

  import sys
  from hxl.scripts.counter import counter

  counter(sys.stdin, sys.stdout, tags)

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
import json
from hxl.parser import HXLReader

def counter(input, output, tags):
    """
    Count occurances of value combinations for a set of tags.
    """

    parser = HXLReader(input)
    writer = csv.writer(output)

    stats = {}

    # Add up the value combinations in the rows
    for row in parser:
        values = []
        for tag in tags:
            value = row.get(tag)
            if value is not False:
                values.append(value)

        if values:
            # need to use a tuple as a key
            key = tuple(values)
            if stats.get(key):
                stats[key] += 1
            else:
                stats[key] = 1

    # Write the HXL hashtag row
    tags.append('#x_total_num')
    writer.writerow(tags)

    # Write the stats, sorted in value order
    for aggregate in sorted(stats.items()):
        data = list(aggregate[0])
        data.append(aggregate[1])
        writer.writerow(data)

# If run as script
if __name__ == '__main__':
    tags = []
    for tag in sys.argv[1:]:
        if not tag.startswith('#'):
            tag = '#' + tag
        tags.append(tag)
    if tags:
        counter(sys.stdin, sys.stdout, tags)
    else:
        sys.exit('Usage: python -m hxl.scripts.counter <hxlTag> [hxlTag...] < DATA_IN.csv > DATA_OUT.csv')

# end
