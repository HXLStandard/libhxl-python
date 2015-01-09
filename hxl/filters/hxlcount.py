"""
Script to count distinct values in a HXL dataset.
David Megginson
October 2014

Counts all combinations of the tags specified on the command line. In
the command-line version, you may omit the initial '#' from tag names
to avoid the need to quote them.  Also optionally calculates sum,
average (mean), min, and max for a numeric tag.

Only the *first* column with each hashtag is currently used.

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
from hxl.model import HXLSource, HXLColumn, HXLRow
from hxl.filters import parse_tags, fix_tag
from hxl.parser import HXLReader, writeHXL

class HXLCountFilter(HXLSource):
    """
    Composable filter class to aggregate rows in a HXL dataset.

    This is the class supporting the hxlcount command-line utility.

    Because this class is a {@link hxl.model.HXLSource}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    WARNING: this filter reads the entire source dataset before
    producing output, and may need to hold a large amount of data in
    memory, depending on the number of unique combinations counted.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = HXLCountFilter(source, tags=['#org', '#sector', '#adm1'])
    writeHXL(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, tags, aggregate_tag=None):
        """
        Constructor
        @param source the HXL data source
        @param tags a list of HXL tags that form a unique key together (what combinations are you counting?)
        @param aggregate_tag an optional numeric tag for calculating aggregate values.
        """
        self.source = source
        self.count_tags = tags
        self.aggregate_tag = aggregate_tag
        self.saved_columns = None
        self.aggregate_iter = None

    @property
    def columns(self):
        """
        @return the column definitions used in the aggregation report
        """
        if self.saved_columns is None:
            cols = []
            for tag in self.count_tags:
                cols.append(HXLColumn(hxlTag=tag))
            cols.append(HXLColumn(hxlTag='#x_count_num'))
            if self.aggregate_tag is not None:
                cols.append(HXLColumn(hxlTag='#x_sum_num'))
                cols.append(HXLColumn(hxlTag='#x_average_num'))
                cols.append(HXLColumn(hxlTag='#x_min_num'))
                cols.append(HXLColumn(hxlTag='#x_max_num'))
            self.saved_columns = cols
        return self.saved_columns

    def next(self):
        """
        @return the next row of aggregated data.
        """
        if self.aggregate_iter is None:
            self._aggregate()
        # Write the stats, sorted in value order
        aggregate = self.aggregate_iter.next()
        values = list(aggregate[0])
        values.append(aggregate[1].count)
        if self.aggregate_tag:
            if aggregate[1].seen_numbers:
                values.append(aggregate[1].sum)
                values.append(aggregate[1].average)
                values.append(aggregate[1].min)
                values.append(aggregate[1].max)
            else:
                values = values + ([''] * 4)

        row = HXLRow(self.columns)
        row.values = values
        return row

    def _aggregate(self):
        """
        Read the entire source dataset and produce saved aggregate data.
        """
        aggregators = {}
        for row in self.source:
            values = []
            for tag in self.count_tags:
                value = row.get(tag)
                if value is not False:
                    values.append(value)
            if values:
                key = tuple(values)
                if not key in aggregators:
                    aggregators[key] = Aggregator(self.aggregate_tag)
                aggregators[key].add(row)
        self.aggregate_iter = iter(sorted(aggregators.items()))

class Aggregator:
    """
    Class to collect aggregates for a single combination.

    Currently calculates count, sum, average, min, and max
    """

    def __init__(self, tag):
        """
        Constructor
        @param tag the HXL tag being counted in the row.
        """
        self.tag = tag
        self.count = 0
        self.sum = 0.0
        self.average = 0.0
        self.min = None
        self.max = None
        self.seen_numbers = False

    def add(self, row):
        """
        Add a new row of data to the aggregator.
        """
        self.count += 1
        if self.tag:
            value = row.get(self.tag)
            try:
                n = float(value)
                self.sum += n
                self.average = self.sum / self.count
                if self.min is None or n < self.min:
                    self.min = n
                if self.max is None or n > self.max:
                    self.max = n
                self.seen_numbers = True
            except ValueError:
                pass

#
# Command-line support
#

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcount with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Generate aggregate counts for a HXL dataset')
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
        '-t',
        '--tags',
        help='Comma-separated list of column tags to count.',
        metavar='tag,tag...',
        type=parse_tags,
        default='loc,org,sector,adm1,adm2,adm3'
        )
    parser.add_argument(
        '-a',
        '--aggregate',
        help='Hashtag to aggregate.',
        metavar='tag',
        type=fix_tag
        )
    args = parser.parse_args(args)
    source = HXLReader(args.infile)
    filter = HXLCountFilter(source, args.tags, args.aggregate)
    writeHXL(args.outfile, filter)

# end
