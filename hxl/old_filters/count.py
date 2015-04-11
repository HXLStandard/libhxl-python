"""
Script to count distinct values in a HXL dataset.
David Megginson
October 2014
"""

from hxl.common import pattern_list
from hxl.model import Dataset, TagPattern, Column, Row

class CountFilter(Dataset):
    """
    Composable filter class to aggregate rows in a HXL dataset.

    This is the class supporting the hxlcount command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    WARNING: this filter reads the entire source dataset before
    producing output, and may need to hold a large amount of data in
    memory, depending on the number of unique combinations counted.

    Usage:

    <pre>
    filter = CountFilter(source, patterns=['#org', '#sector'])
    </pre>

    or

    <pre>
    filter = source.count(['#org', '#sector'])
    </pre>
    """

    def __init__(self, source, patterns, aggregate_pattern=None):
        """
        Constructor
        @param source the HXL data source
        @param patterns a list of strings or TagPattern objects that form a unique key together
        @param aggregate_pattern an optional tag pattern calculating numeric aggregate values.
        """
        self.source = source
        self.patterns = pattern_list(patterns)
        self.aggregate_pattern = TagPattern.parse(aggregate_pattern) if aggregate_pattern else None
        self._saved_columns = None

    @property
    def columns(self):
        """
        @return the column definitions used in the aggregation report
        """
        if self._saved_columns is None:
            cols = []
            for pattern in self.patterns:
                column = pattern.find_column(self.source.columns)
                if column is not None:
                    header = column.header
                else:
                    header = None
                cols.append(Column(tag=pattern.tag, attributes=pattern.include_attributes, header=header))
            cols.append(Column(tag='#x_count_num', header='Count'))
            if self.aggregate_pattern is not None:
                cols.append(Column(tag='#x_sum_num', header='Sum'))
                cols.append(Column(tag='#x_average_num', header='Average (mean)'))
                cols.append(Column(tag='#x_min_num', header='Minimum value'))
                cols.append(Column(tag='#x_max_num', header='Maximum value'))
            self._saved_columns = cols
        return self._saved_columns

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
            if self.outer.aggregate_pattern:
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
                values = [pattern.get_value(row) for pattern in self.outer.patterns]
                if values:
                    key = tuple(values)
                    if not key in aggregators:
                        aggregators[key] = Aggregator(self.outer.aggregate_pattern)
                    aggregators[key].add(row)
            self.aggregate_iter = iter(sorted(aggregators.items()))

class Aggregator(object):
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

# end
