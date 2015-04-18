"""
Filters for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys, re, six, operator
from copy import copy
import dateutil.parser

import hxl
from hxl.common import pattern_list, normalise_string
from hxl.model import TagPattern, Dataset, Column, Row


#
# Filter-specific exception
#
class HXLFilterException(hxl.HXLException):
    pass

#
# Filter classes
#

class AddColumnsFilter(Dataset):
    """
    Composable filter class to add constant values to every row of a HXL dataset.

    This is the class supporting the hxladd command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.
    """

    def __init__(self, source, specs, before=False):
        """
        @param source a HXL data source
        @param specs a sequence of pairs of Column objects and constant values
        @param before True to add new columns before existing ones
        """
        self.source = source
        if isinstance(specs, six.string_types):
            specs = [specs]
        self.specs = [AddColumnsFilter.parse_spec(spec) for spec in specs]
        self.before = before
        self._columns_out = None

    @property
    def columns(self):
        """
        Add the constant columns to the end.
        """
        if self._columns_out is None:
            new_columns = [spec[0] for spec in self.specs]
            if self.before:
                self._columns_out = new_columns + self.source.columns
            else:
                self._columns_out = self.source.columns + new_columns
            # constant values to add
            self._const_values = [spec[1] for spec in self.specs]
        return self._columns_out

    def __iter__(self):
        return AddColumnsFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __iter__(self):
            return self

        def __next__(self):
            """
            Return the next row, with constant values added.
            """
            row = copy(next(self.iterator))
            row.columns = self.outer.columns
            if self.outer.before:
                row.values = self.outer._const_values + row.values
            else:
                row.values = row.values + self.outer._const_values
            return row

        next = __next__

    VALUE_PATTERN = r'^\s*(?:([^#]*)#)?({token})=(.*)\s*$'.format(token=hxl.common.TOKEN)

    @staticmethod
    def parse_spec(spec):
        if not isinstance(spec, six.string_types):
            return spec
        result = re.match(AddColumnsFilter.VALUE_PATTERN, spec)
        if result:
            header = result.group(1)
            tag = '#' + result.group(2)
            value = result.group(3)
            return (Column(tag=tag, header=header), value)
        else:
            raise HXLFilterException("Badly formatted new-column spec: " + s)


class CacheFilter(Dataset):
    """Composable filter class to cache HXL data in memory."""

    def __init__(self, source, max_rows=None):
        """
        Constructor
        @param max_rows If >0, maximum number of rows to cache.
        """
        self.source = source
        self.max_rows = max_rows
        self.cached_columns = copy(source.columns)
        self.cached_rows = [copy(row) for row in source]
        self.overflow = False

    @property
    def columns(self):
        return self.cached_columns

    def __iter__(self):
        return iter(self.cached_rows)

    def _load(self):
        if self.cached_rows is None:
            self.cached_rows = []
            self.cached_columns = copy(self.source.columns)
            row_count = 0
            for row in self.source:
                row_count += 1
                if self.max_rows > 1 and row_count >= self.max_rows:
                    self.overflow = True
                    break
                else:
                    self.cached_rows.append(copy(row))


class CleanDataFilter(Dataset):
    """
    Filter for cleaning values in HXL data.
    Can normalise whitespace, convert to upper/lowercase, and fix dates and numbers.
    TODO: clean up lat/lon coordinates
    """

    def __init__(self, source, whitespace=False, upper=[], lower=[], date=[], number=[]):
        """
        Construct a new data-cleaning filter.
        @param source the HXLDataSource
        @param whitespace list of TagPatterns for normalising whitespace, or True to normalise all.
        @param upper list of TagPatterns for converting to uppercase, or True to convert all.
        @param lower list of TagPatterns for converting to lowercase, or True to convert all.
        @param lower list of TagPatterns for normalising dates, or True to normalise all ending in "_date"
        @param lower list of TagPatterns for normalising numbers, or True to normalise all ending in "_num"
        """
        self.source = source
        self.whitespace = whitespace
        self.upper = upper
        self.lower = lower
        self.date = date
        self.number = number

    @property
    def columns(self):
        """Pass on the source columns unmodified."""
        return self.source.columns

    def __iter__(self):
        return CleanDataFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __iter__(self):
            return self

        def __next__(self):
            """Return the next row, with values cleaned as needed."""
            # TODO implement a lazy copy
            row = copy(next(self.iterator))
            for i in range(min(len(row.values), len(row.columns))):
                row.values[i] = self._clean_value(row.values[i], row.columns[i])
            return row

        next = __next__

        def _clean_value(self, value, column):
            """Clean a single HXL value."""

            # TODO prescan columns at start for matches

            # Whitespace (-w or -W)
            if self._match_patterns(self.outer.whitespace, column):
                value = re.sub('^\s+', '', value)
                value = re.sub('\s+$', '', value)
                value = re.sub('\s+', ' ', value)

            # Uppercase (-u)
            if self._match_patterns(self.outer.upper, column):
                if sys.version_info[0] > 2:
                    value = value.upper()
                else:
                    value = value.decode('utf8').upper().encode('utf8')

            # Lowercase (-l)
            if self._match_patterns(self.outer.lower, column):
                if sys.version_info[0] > 2:
                    value = value.lower()
                else:
                    value = value.decode('utf8').lower().encode('utf8')

            # Date (-d or -D)
            if self._match_patterns(self.outer.date, column, '_date'):
                if value:
                    value = dateutil.parser.parse(value).strftime('%Y-%m-%d')

            # Number (-n or -N)
            if self._match_patterns(self.outer.number, column, '_num') and re.match('\d', value):
                if value:
                    value = re.sub('[^\d.]', '', value)
                    value = re.sub('^0+', '', value)
                    value = re.sub('(\..*)0+$', '\g<1>', value)
                    value = re.sub('\.$', '', value)

            return value

        def _match_patterns(self, patterns, column, extension=None):
            """Test if a column matches a list of patterns."""
            if not patterns:
                return False
            elif patterns is True:
                # if there's an extension specific like "_date", must match it
                return (column.tag and (not extension or column.tag.endswith(extension)))
            else:
                for pattern in patterns:
                    if pattern.match(column):
                        return True
                return False


class ColumnFilter(Dataset):
    """
    Composable filter class to cut columns from a HXL dataset.

    This is the class supporting the hxlcut command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    filter = ColumnFilter(source, include_tags=['#org', '#sector', '#adm1'])
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, include_tags=[], exclude_tags=[]):
        """
        @param source a HXL data source
        @param include_tags a whitelist of TagPattern objects to include
        @param exclude_tags a blacklist of TagPattern objects to exclude
        """
        self.source = source
        self.include_tags = pattern_list(include_tags)
        self.exclude_tags = pattern_list(exclude_tags)
        self.indices = [] # saved indices for columns to include
        self.columns_out = None

    @property
    def columns(self):
        """
        Filter out the columns that should be removed.
        """
        if self.columns_out is None:
            self.columns_out = []
            columns = self.source.columns
            for i in range(len(columns)):
                column = columns[i]
                if self._test_column(column):
                    self.columns_out.append(column)
                    self.indices.append(i) # save index to avoid retesting for data
        return self.columns_out

    def __iter__(self):
        return ColumnFilter.Iterator(self)

    def _test_column(self, column):
        """
        Test whether a column should be included in the output.
        If there is a whitelist, it must be in the whitelist; if there is a blacklist, it must not be in the blacklist.
        """
        if self.exclude_tags:
            # blacklist
            for pattern in self.exclude_tags:
                if pattern.match(column):
                    # fail as soon as we match an excluded pattern
                    return False

        if self.include_tags:
            # whitelist
            for pattern in self.include_tags:
                if pattern.match(column):
                    # succeed as soon as we match an included pattern
                    return True
            # fail if there was a whitelist and we didn't match
            return False
        else:
            # no whitelist
            return True

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __iter__(self):
            return self

        def __next__(self):
            """
            Return the next row, with appropriate columns filtered out.
            """
            row_in = next(self.iterator)
            row_out = Row(columns=self.outer.columns)
            values_out = []
            for i in self.outer.indices:
                values_out.append(row_in.values[i])
            row_out.values = values_out
            return row_out

        next = __next__


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
            cols.append(Column(tag='#meta+count', header='Count'))
            if self.aggregate_pattern is not None:
                cols.append(Column(tag='#meta+sum', header='Sum'))
                cols.append(Column(tag='#meta+average', header='Average (mean)'))
                cols.append(Column(tag='#meta+min', header='Minimum value'))
                cols.append(Column(tag='#meta+max', header='Maximum value'))
            self._saved_columns = cols
        return self._saved_columns

    def __iter__(self):
        return CountFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)
            self.aggregate_iter = None

        def __iter__(self):
            return self

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
                        aggregators[key] = CountFilter.Aggregator(self.outer.aggregate_pattern)
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

class MergeDataFilter(Dataset):
    """
    Composable filter class to merge values from two HXL datasets.

    This is the class supporting the hxlmerge command-line utility.

    Warning: this filter may store a large amount of data in memory, depending on the merge.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    merge_source = HXLReader(open('file-to-merge.csv', 'r'))
    filter = MergeDataFilter(source, merge_source=merge_source, keys=['adm1_id'], tags=['adm1'])
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, merge_source, keys, tags, replace=False, overwrite=False):
        """
        Constructor.
        @param source the HXL data source.
        @param merge_source a second HXL data source to merge into the first.
        @param keys the shared key hashtags to use for the merge
        @param tags the tags to include from the second dataset
        """
        self.source = source
        self.merge_source = merge_source
        self.keys = keys
        self.merge_tags = tags
        self.replace = replace
        self.overwrite = overwrite

        self.saved_columns = None

    @property
    def columns(self):
        """
        @return column definitions for the merged dataset
        """
        if self.saved_columns is None:
            new_columns = []
            for pattern in self.merge_tags:
                if self.replace and pattern.find_column(self.source.columns):
                    # will use existing column
                    continue
                else:
                    column = pattern.find_column(self.merge_source.columns)
                    if column:
                        header = column.header
                    else:
                        header = None
                    new_columns.append(Column(tag=pattern.tag, attributes=pattern.include_attributes, header=header))
            self.saved_columns = self.source.columns + new_columns
        return self.saved_columns

    def __iter__(self):
        return MergeDataFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)
            self.merge_iterator = iter(outer.merge_source)
            self.merge_map = None

        def __iter__(self):
            return self

        def __next__(self):
            """
            @return the next merged row of data
            """

            # First, check if we already have the merge map, and read it if not
            if self.merge_map is None:
                self.merge_map = self._read_merge()

            # Make a copy of the next row from the source
            row = copy(next(self.iterator))

            # Look up the merge values, based on the --keys
            merge_values = self.merge_map.get(self._make_key(row), {})

            # Go through the --tags
            for pattern in self.outer.merge_tags:
                # Try to substitute in place?
                if self.outer.replace:
                    index = pattern.find_column_index(self.outer.source.columns)
                    if index is not None:
                        if self.outer.overwrite or not row.values[index]:
                            row.values[index] = merge_values.get(pattern)
                        continue

                # otherwise, fall through
                row.append(merge_values.get(pattern, ''))
            return row

        next = __next__

        def _make_key(self, row):
            """
            Make a tuple key for a row.
            """
            values = []
            for pattern in self.outer.keys:
                values.append(pattern.get_value(row))
            return tuple(values)

        def _read_merge(self):
            """
            Read the second (merging) dataset into memory.
            Stores only the values necessary for the merge.
            @return a map of merge values
            """
            merge_map = {}
            for row in self.merge_iterator:
                values = {}
                for pattern in self.outer.merge_tags:
                    values[pattern] = pattern.get_value(row)
                merge_map[self._make_key(row)] = values
            return merge_map

class RenameFilter(Dataset):
    """
    Composable filter class to rename columns in a HXL dataset.

    This is the class supporting the hxlrename command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = RenameFilter(source, rename=[[TagPattern.parse('#foo'), Column.parse('#bar')]])
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, rename=[]):
        """
        Constructor
        @param source the Dataset for the data.
        @param rename_map map of tags to rename
        """
        self.source = source
        if isinstance(rename, six.string_types):
            rename = [rename]
        self.rename = [self.parse_rename(spec) for spec in rename]
        self._saved_columns = None

    @property
    def columns(self):
        """
        Return the renamed columns.
        """

        if self._saved_columns is None:
            def rename_column(column):
                for spec in self.rename:
                    if spec[0].match(column):
                        new_column = copy(spec[1])
                        if new_column.header is None:
                            new_column.header = column.header
                        return new_column
                return column
            self._saved_columns = [rename_column(column) for column in self.source.columns]
        return self._saved_columns

    def __iter__(self):
        return iter(self.source)

    RENAME_PATTERN = r'^\s*#?({token}(?:\s*[+-]{token})*):(?:([^#]*)#)?({token}(?:\s*[+]{token})*)\s*$'.format(token=hxl.common.TOKEN)

    @staticmethod
    def parse_rename(s):
        if isinstance(s, six.string_types):
            result = re.match(RenameFilter.RENAME_PATTERN, s)
            if result:
                pattern = TagPattern.parse(result.group(1))
                column = Column.parse('#' + result.group(3), header=result.group(2), use_exception=True)
                return (pattern, column)
            else:
                raise HXLFilterException("Bad rename expression: " + s)
        else:
            return s


class RowFilter(Dataset):
    """
    Composable filter class to select rows from a HXL dataset.

    This is the class supporting the hxlselect command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = RowFilter(source, queries=[(TagPattern.parse('#org'), operator.eq, 'OXFAM')])
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, queries=[], reverse=False):
        """
        Constructor
        @param source the HXL data source
        @param queries a series for parsed queries
        @param reverse True to reverse the sense of the select
        """
        self.source = source
        if not hasattr(queries, '__len__') or isinstance(queries, str):
            # make a list if needed
            queries = [queries]
        self.queries = [RowFilter.Query.parse(query) for query in queries]
        self.reverse = reverse

    @property
    def columns(self):
        """Pass on the source columns unmodified."""
        return self.source.columns

    def __iter__(self):
        return RowFilter.Iterator(self)

    class Iterator(object):

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __iter__(self):
            return self

        def __next__(self):
            """
            Return the next row that matches the select.
            """
            row = next(self.iterator)
            while not self.match_row(row):
                row = next(self.iterator)
            return row

        next = __next__

        def match_row(self, row):
            """Check if any of the queries matches the row (implied OR)."""
            for query in self.outer.queries:
                if query.match_row(row):
                    return not self.outer.reverse
            return self.outer.reverse

    class Query(object):
        """Query to execute against a row of HXL data."""

        def __init__(self, pattern, op, value):
            self.pattern = pattern
            self.op = op
            self.value = value
            self._saved_indices = None
            try:
                float(value)
                self._is_numeric = True
            except:
                self._is_numeric = False

        def match_row(self, row):
            """Check if a key-value pair appears in a HXL row"""
            indices = self._get_saved_indices(row.columns)
            length = len(row.values)
            for i in indices:
                if i < length and row.values[i] and self.match_value(row.values[i]):
                        return True
            return False

        def match_value(self, value):
            """Try an operator as numeric first, then string"""
            # TODO add dates
            # TODO use knowledge about HXL tags
            if self._is_numeric:
                try:
                    return self.op(float(value), float(self.value))
                except ValueError:
                    pass
            return self.op(normalise_string(value), normalise_string(self.value))

        def _get_saved_indices(self, columns):
            """Cache the column tests, so that we run them only once."""
            # FIXME - assuming that the columns never change
            if self._saved_indices is None:
                self._saved_indices = []
                for i in range(len(columns)):
                    if self.pattern.match(columns[i]):
                        self._saved_indices.append(i)
            return self._saved_indices

        @staticmethod
        def parse(s):
            """Parse a filter expression"""
            if isinstance(s, RowFilter.Query):
                # already parsed
                return s
            parts = re.split(r'([<>]=?|!?=|!?~)', s, maxsplit=1)
            pattern = TagPattern.parse(parts[0])
            op = RowFilter.Query.OPERATOR_MAP[parts[1]]
            value = parts[2]
            return RowFilter.Query(pattern, op, value)

        @staticmethod
        def operator_re(s, pattern):
            """Regular-expression comparison operator."""
            return re.match(pattern, s)

        @staticmethod
        def operator_nre(s, pattern):
            """Regular-expression negative comparison operator."""
            return not re.match(pattern, s)

        # Constant map of comparison operators
        OPERATOR_MAP = {
            '=': operator.eq,
            '!=': operator.ne,
            '<': operator.lt,
            '<=': operator.le,
            '>': operator.gt,
            '>=': operator.ge
        }


# Extra static initialisation
RowFilter.Query.OPERATOR_MAP['~'] = RowFilter.Query.operator_re
RowFilter.Query.OPERATOR_MAP['!~'] = RowFilter.Query.operator_nre


class SortFilter(Dataset):
    """
    Composable filter class to sort a HXL dataset.

    This is the class supporting the hxlsort command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = SortFilter(source, tags=[TagPattern.parse('#sector'), TagPattern.parse('#org'), TagPattern.parse('#adm1']))
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, tags=[], reverse=False):
        """
        @param source a HXL data source
        @param tags list of TagPattern objects for sorting
        @param reverse True to reverse the sort order
        """
        self.source = source
        self.sort_tags = pattern_list(tags)
        self.reverse = reverse
        self._iter = None

    @property
    def columns(self):
        """
        Return the same columns as the source.
        """
        return self.source.columns

    def __iter__(self):
        """
        Sort the dataset first, then return it row by row.
        """

        # Closures
        def make_key(row):
            """Closure: use the requested tags as keys (if provided). """

            def get_value(pattern):
                """Closure: extract a sort key"""
                raw_value = pattern.get_value(row)
                if pattern.tag.endswith('_num') or pattern.tag.endswith('_deg'):
                    # left-pad numbers with zeros
                    try:
                        raw_value = str(float(raw_value)).zfill(15)
                    except ValueError:
                        pass
                elif pattern.tag.endswith('_date'):
                    # normalise dates for sorting
                    raw_value = dateutil.parser.parse(raw_value).strftime('%Y-%m-%d')
                return raw_value.upper() if raw_value else ''

            if self.sort_tags:
                return [get_value(pattern) for pattern in self.sort_tags]
            else:
                return row.values

        # Main method
        return iter(sorted(self.source, key=make_key, reverse=self.reverse))


class ValidateFilter(Dataset):
    """Composable filter class to validate a HXL dataset against a schema.

    This is the class supporting the hxlvalidate command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, singled-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    schema = hxl_schema('my-schema.csv')
    filter = ValidateFilter(source, schema)
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, schema, show_all=False):
        """
        @param source a HXL data source
        @param schema a Schema object
        @param show_all boolean flag to report all lines (including those without errors).
        """
        self.source = source
        self.schema = schema
        self.show_all = show_all
        self._saved_columns = None

    @property
    def columns(self):
        """
        Add columns for the error reporting.
        """
        if self._saved_columns is None:
            # append error columns
            err_col = Column(tag='#x_errors', header='Error messages')
            tag_col = Column(tag='#x_tags', header='Error tag')
            row_col = Column(tag='#x_rows', header='Error row number (source)')
            col_col = Column(tag='#x_cols', header='Error column number (source)')
            self._saved_columns = self.source.columns + [err_col, tag_col, row_col, col_col]
        return self._saved_columns

    def __iter__(self):
        return ValidateFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __iter__(self):
            return self

        def __next__(self):
            """
            Report rows with error information.
            """
            validation_errors = []
            def callback(error):
                """
                Collect validation errors
                """
                validation_errors.append(error)
            self.outer.schema.callback = callback

            """
            Read rows until we find an error (unless we're printing all rows)
            """
            row = next(self.iterator)
            while row:
                if not self.outer.schema.validate_row(row) or self.outer.show_all:
                    # append error data to row
                    error_row = copy(row)
                    messages = "\n".join(map(lambda e: e.message, validation_errors))
                    tags = "\n".join(map(lambda e: str(e.rule.tag_pattern) if e.rule else '', validation_errors))
                    rows = "\n".join(map(lambda e: str(e.row.source_row_number) if e.row else '', validation_errors))
                    error_row.columns = self.outer.columns
                    error_row.values = error_row.values + [messages, tags, rows]
                    return error_row
                else:
                    row = next(self.iterator)

        next = __next__


