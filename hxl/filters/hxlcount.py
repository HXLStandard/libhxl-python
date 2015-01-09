"""
Script to count distinct values in a HXL dataset.
David Megginson
October 2014

Counts all combinations of the tags specified on the command line. In
the command-line version, you may omit the initial '#' from tag names
to avoid the need to quote them.

Only the *first* column with each hashtag is currently used.

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import csv
import json
import argparse
from hxl.model import HXLSource, HXLColumn, HXLRow
from hxl.filters import parse_tags, fix_tag
from hxl.parser import HXLReader, writeHXL

class Aggregator:

    def __init__(self, tag):
        self.tag = tag
        self.count = 0
        self.sum = 0.0
        self.average = 0.0
        self.min = None
        self.max = None
        self.seen_numbers = False

    def add(self, row):
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

class HXLCountFilter(HXLSource):

    def __init__(self, source, tags, aggregate_tag=None):
        self.source = source
        self.count_tags = tags
        self.aggregate_tag = aggregate_tag
        self.saved_columns = None
        self.aggregator_iter = None

    @property
    def columns(self):
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
        if self.aggregator_iter is None:
            self._aggregate()
        # Write the stats, sorted in value order
        aggregate = self.aggregator_iter.next()
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
        self.aggregator_iter = iter(sorted(aggregators.items()))

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
