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
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import argparse
from hxl.model import DataProvider, TagPattern, Column, Row
from hxl.filters import make_input, make_output
from hxl.io import StreamInput, HXLReader, write_hxl

class CountFilter(DataProvider):
    """
    Composable filter class to aggregate rows in a HXL dataset.

    This is the class supporting the hxlcount command-line utility.

    Because this class is a {@link hxl.model.DataProvider}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    WARNING: this filter reads the entire source dataset before
    producing output, and may need to hold a large amount of data in
    memory, depending on the number of unique combinations counted.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = CountFilter(source, tags=[TagPattern.parse('#org'), TagPattern.parse('#sector'), TagPattern.parse('#adm1')])
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, tags, aggregate_tag=None):
        """
        Constructor
        @param source the HXL data source
        @param tags a list of TagPattern objects that form a unique key together (what combinations are you counting?)
        @param aggregate_tag an optional numeric tag for calculating aggregate values.
        """
        self.source = source
        self.count_tags = tags
        self.aggregate_tag = aggregate_tag
        self.saved_columns = None

    @property
    def columns(self):
        """
        @return the column definitions used in the aggregation report
        """
        if self.saved_columns is None:
            cols = []
            for pattern in self.count_tags:
                column = pattern.find_column(self.source.columns)
                if column is not None:
                    header = column.header
                else:
                    header = None
                cols.append(Column(tag=pattern.tag, attributes=pattern.include_attributes, header=header))
            cols.append(Column(tag='#x_count_num', header='Count'))
            if self.aggregate_tag is not None:
                cols.append(Column(tag='#x_sum_num', header='Sum'))
                cols.append(Column(tag='#x_average_num', header='Average (mean)'))
                cols.append(Column(tag='#x_min_num', header='Minimum value'))
                cols.append(Column(tag='#x_max_num', header='Maximum value'))
            self.saved_columns = cols
        return self.saved_columns

    def __iter__(self):
        return CountFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)
            self.aggregate_iter = None

        def __next__(self):
            """
            @return the next row of aggregated data.
            """
            if self.aggregate_iter is None:
                self._aggregate()
            # Write the stats, sorted in value order
            aggregate = next(self.aggregate_iter)
            values = list(aggregate[0])
            values.append(aggregate[1].count)
            if self.outer.aggregate_tag:
                if aggregate[1].seen_numbers:
                    values.append(aggregate[1].sum)
                    values.append(aggregate[1].average)
                    values.append(aggregate[1].min)
                    values.append(aggregate[1].max)
                else:
                    values = values + ([''] * 4)

            row = Row(self.outer.columns)
            row.values = values
            return row

        next = __next__

        def _aggregate(self):
            """
            Read the entire source dataset and produce saved aggregate data.
            """
            aggregators = {}
            for row in self.iterator:
                values = [pattern.get_value(row) for pattern in self.outer.count_tags]
                if values:
                    key = tuple(values)
                    if not key in aggregators:
                        aggregators[key] = Aggregator(self.outer.aggregate_tag)
                    aggregators[key].add(row)
            self.aggregate_iter = iter(sorted(aggregators.items()))

class Aggregator:
    """
    Class to collect aggregates for a single combination.

    Currently calculates count, sum, average, min, and max
    """

    def __init__(self, pattern):
        """
        Constructor
        @param pattern the HXL tag being counted in the row.
        """
        self.pattern = pattern
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
        if self.pattern:
            value = self.pattern.get_value(row)
            if value:
                try:
                    n = float(value)
                    self.sum += n
                    self.average = self.sum / self.count
                    if self.min is None or n < self.min:
                        self.min = n
                    if self.max is None or n > self.max:
                        self.max = n
                    self.seen_numbers = True
                except:
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
        nargs='?'
        )
    parser.add_argument(
        'outfile',
        help='HXL file to write (if omitted, use standard output).',
        nargs='?'
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to count.',
        metavar='tag,tag...',
        type=TagPattern.parse_list,
        default='loc,org,sector,adm1,adm2,adm3'
        )
    parser.add_argument(
        '-a',
        '--aggregate',
        help='Hashtag to aggregate.',
        metavar='tag',
        type=TagPattern.parse
        )

    args = parser.parse_args(args)
    with make_input(args.infile, stdin) as input, make_output(args.outfile, stdout) as output:
        source = HXLReader(input)
        filter = CountFilter(source, args.tags, args.aggregate)
        write_hxl(output.output, filter)

# end
