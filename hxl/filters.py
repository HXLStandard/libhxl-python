"""Data filter classes for the Humanitarian Exchange Language (HXL) v1.0

Filters are virtual L{datasets<hxl.model.Dataset>} that read from
I{other} datasets and modify them on the fly. Most of the filter
classes here have corresponding convenience methods in the
L{hxl.model.Dataset} class, such as L{sort()<hxl.model.Dataset.sort>}
for L{SortFilter}::

  # Filter as a class
  source = SortFilter(hxl.data('http://example.org/data.csv'))

  # Filter as a method
  source = hxl.data('http://example.org/data.csv).sort()

Most filters do not keep a copy of the data internally, so it is
efficient to chain many filters together::

  source = hxl.data(url).with_rows('org=UNICEF').with_rows('sector=Education')

Some filters, however, do have to keep a cached version of the data
internally, such as L{SortFilter}, L{CountFilter}, and
L{CacheFilter}. If you are working with very large datasets, you
should be aware of this limitation (for example, it is better to sort
I{after} filtering out a lot of rows, so that there will be less held
in memory).

If you are creating your own filters, you should normally subclass
them from L{AbstractStreamingFilter} (if they don't have to keep data
internally) or L{AbstractCachingFilter}, but you can also subclass the
lower-level L{AbstractBaseFilter} directly for especially-complex cases.

@author: David Megginson
@organization: UNOCHA
@license: Public Domain
@date: Started October 2014
@see: U{hxlstandard.org}

"""

import hxl
import abc, copy, dateutil.parser, json, jsonpath_rw, logging, re, six, sys


logger = logging.getLogger(__name__)


class HXLFilterException(hxl.HXLException):
    """Base class for HXL filter exceptions.

    This subclass of L{hxl.HXLException} exists only to make it
    easier to distinguish filter-based exceptions in C{except:} clauses.
    """


class AbstractBaseFilter(hxl.model.Dataset):
    """Abstract base class for composable filters.

    This is the base class for all filters. A B{filter} is like a
    L{hxl.model.Dataset}, except that it uses another dataset as its source, and
    performs some kind of transformation on it before producing its
    output.

    This class stores the upstream source, and provides a
    L{filter_columns} method that child classes can implement. The
    L{columns} method will call filter_columns() precisely once for
    each instantiation, giving the child a chance to provide a
    different set of columns than those in the source.

    If you're writing your own filter classes, you should normally
    subclass L{AbstractStreamingFilter} or L{AbstractCachingFilter},
    both of which are child classes of this one; however, there may be
    some special applications where you need to subclass
    I{AbstractBaseFilter} directly (see L{AppendFilter} for an
    example).

    Subclassing works like this::
    
      class MyFilter(hxl.filters.AbstractBaseFilter):

          def __init__(self, source):
              super(AbstractBaseFilter, self).__init__(source)

          def filter_columns(self):
              return [Column.parse('#org'), Column.parse('#adm1')]

    The output will be identical to the source, except that the
    columns will now be '#org' and '#adm1'.

    @see: L{AbstractStreamingFilter}
    @see: L{AbstractCachingFilter}

    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, source):
        """Construct a new abstract filter.
        @param source: the source dataset
        """

        super().__init__()

        self.source = source
        """HXL data source for the filter"""

        self._filtered_column_cache = None
        """Cache of columns as filtered by the child class."""

    @property
    def is_cached(self):
        """Test if the input is cached (can be replayed).
        Subclasses that cache should override this.
        @returns: C{True} only if the source is cached.
        """
        return self.source.is_cached

    @property
    def columns(self):
        """Return the filter's (possibly-modified) columns.

        By default, return the columns defined by the source HXL
        data. Child classes override the L{filter_columns} method to
        return something different.

        @returns: a list of L{hxl.model.Column} objects
        """
        if self._filtered_column_cache is None:
            self._filtered_column_cache = self.filter_columns()
        return self._filtered_column_cache

    def filter_columns(self):
        """Return a new list of columns for the filtered dataset.

        By default, return the source HXL data's columns. Child
        classes override this method to return a different set of
        columns

        @returns: a list of L{hxl.model.Column} objects
        @see: L{AbstractStreamingFilter.filter_row}
        @see: L{AbstractCachingFilter.filter_rows}
        """
        return self.source.columns

    def _setup_queries(self, query_specs):
        """Parse a list of query specs, and calculate aggregates when needed.
        Side-effect: may replace self.source with a caching filter.
        @param query_specs: a list of row-query string specs
        @returns: a list of hxl.model.RowQuery objects, ready for use
        """
        queries = hxl.model.RowQuery.parse_list(query_specs)

        # Some queries need to run through the dataset first to calculate an aggregate value
        for query in queries:
            if query.needs_aggregate:
                if not self.source.is_cached:
                    self.source = self.source.cache()
                query.calc_aggregate(self.source)

        return queries

    def _get_indices(self, patterns):
        """Get indices of columns to fill.
        If there's no column pattern, then fill all columns.
        @param patterns: list of tag patterns for matching columns (if empty, all columns match)
        @returns: a set of indices for filling.
        """
        indices = set()
        for i, column in enumerate(self.source.columns):
            if len(patterns) == 0 or hxl.model.TagPattern.match_list(column, patterns):
                indices.add(i)
        return indices

    @staticmethod
    def _load (source, spec):
        """Create an instance of the filter from a dict.
        Child classes must override this method.
        @param spec: the JSON-like spec to read
        @returns: a new filter object
        """
        raise NotImplementedError("No static _load method implemented.")

    
class AbstractStreamingFilter(AbstractBaseFilter):
    """Abstract base class for streaming filters.

    A streaming filter processes one row at a time.  It can skip rows,
    but it never reorders them.  As a result, a streaming filter is
    I{much} more efficient than a L{caching
    filter<AbstractCachingFilter>}, since it needs to hold only one
    row in memory at a time.

    It is not possible to replay the data from a streaming filter
    (e.g. to run a second processing pass), unless it has a caching
    filter higher up the filter chain; if you are reading directly
    from disk or a URL, the data is gone once it has passed through
    the filter once.

    If you are implementing your own filter class, you should subclass
    I{AbstractStreamingFilter} whenever possible. Child classes may
    implement the L{AbstractBaseFilter.filter_columns} method to
    change the columns and tags, and/or this class's L{filter_row}
    method to change, add, or suppress individual rows, like this:

      class MyFilter(hxl.filters.AbstractStreamingFilter):

          def __init__(self, source):
              super(AbstractStreamingFilter, self).__init__(source)

          def filter_row(self, row):
              if row.get('org+name') == "Unknown":
                  return None # remove from output
              else:
                  return row

    This simple filter will produce a copy of the source data, but
    omitting rows where the org name is "Unknown".

    @see: L{AbstractCachingFilter}

    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, source):
        """Construct a new streaming filter.
        @param source: the source dataset
        """
        super().__init__(source)

    @abc.abstractmethod
    def filter_row(self, row):
        """Filter a single row of data.

        By default, this method returns the row's values,
        unchanged. Subclasses can use it to replace, add or suppress
        rows on the fly, without having to keep a copy of the entire
        dataset in memory.

        @param row: the original L{hxl.model.Row} object.  
        @returns: A list of string values (I{not} a Row object) or
        C{None} to skip the row.
        @see: L{AbstractBaseFilter.filter_columns}

        """
        return row.values

    def __iter__(self):
        return AbstractStreamingFilter._Iterator(self)

    class _Iterator:
        """Internal iterator class to return the filtered rows.
        Note that the filtering happens here, not in the main class.
        """

        def __init__(self, outer):
            """Create an iterator for a streaming filter
            @param outer: a reference to the parent object (an L{AbstractStreamingFilter}).
            """
            self.outer = outer # ref to outer object
            self.source_iter = iter(self.outer.source) # iterator for the source data
            self.row_number = -1

        def __iter__(self):
            return self

        def __next__(self):
            """Return the next filtered row of data.  

            Uses the L{AbstractStreamingFilter.filter_row} method. The
            returned row is always a new object, so that if the client
            changes it, it won't change the version visible upstream
            in the filter chain.

            @returns: a L{hxl.model.Row} object

            """
            # call this here, in case it caches any useful information
            columns = self.outer.columns
            while True:
                # a StopIterationException will terminate the loop
                row = next(self.source_iter)
                # get a new list of filtered values
                values = self.outer.filter_row(row)
                if values is not None:
                    # keep looping if filter_row(row) returned None
                    self.row_number += 1
                    # create a new Row object
                    return hxl.model.Row(columns, values, self.row_number)


class AbstractCachingFilter(AbstractBaseFilter):
    """Abstract base class for caching filters.

    A caching filter processes the entire dataset together in
    memory. This approach is less efficient than a L{streaming
    filter<AbstractStreamingFilter>}, but it's necessary when a filter
    needs to reorder rows, or to change some rows that depend on
    others. Examples of caching filters include L{SortFilter},
    L{CountFilter}, and L{CacheFilter}.

    Another important property of a caching filter is that it's
    possible to replay it. As a result, caching filters are especially
    useful for data that a client application needs to process more
    than once, and the L{CacheFilter} class exists for precisely that
    purpose. The filter will transform the data only once, then save a
    copy of it for future use.

    Child classes may implement the
    L{AbstractBaseFilter.filter_columns} method to change the columns
    and tags, and/or this class's L{filter_rows} method to change the
    data all at once, like this::

      class MyFilter(hxl.filters.AbstractCachingFilter):

          def __init__(self, source):
              super(AbstractStreamingFilter, self).__init__(source)

          def filter_rows(self, row):
              raw_data = self.source.values
              # return a list of lists, one for each row
              return raw_data.sort()

    This simple filter will produce a copy of the source data sorted
    using Python's default sorting method.

    @see: L{AbstractStreamingFilter}

    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, source):
        """Construct a new caching filter.
        @param source: the source dataset
        """
        super().__init__(source)

        self._saved_rows = None
        """Cache of the output rows, for replaying."""

    @property
    def is_cached(self):
        """Test if the input is cached.
        @returns: always C{True} for caching filters
        """
        return True

    def filter_rows(self):
        """Filter all of the data together.
        
        This method returns raw lists and strings, not objects from
        the L{hxl.model} module; for example, it could return the
        following for a three-row dataset::

          [['UNICEF', '20'], ['UNHCR', '15'], ['OXFAM', '25']]

        The I{AbstractCachingFilter} class will construct the
        appropriate objects around the data.
        
        @returns: a list of lists of strings, one list for each row.
        @see: L{AbstractBaseFilter.filter_columns}
        """
        return self.source.values

    def __iter__(self):
        return AbstractCachingFilter._Iterator(self)

    class _Iterator:
        """Internal iterator class to return the filtered rows."""

        def __init__(self, outer):
            """Create an iterator for a caching filter
            @param outer: a reference to the parent object (an L{AbstractStreamingFilter}).
            """
            self.outer = outer
            self.values_iter = None
            self.row_number = -1

        def __iter__(self):
            return self

        def __next__(self):
            """Return the next filtered row.

            If we haven't called L{AbstractCachingFilter.filter_rows}
            yet, call it now, and save the result in the parent's I{_saved_rows} property.

            @returns: a L{hxl.model.Row} object
            """

            if self.values_iter is None:
                if self.outer._saved_rows is None:
                    # filter rows only once, when requested
                    self.outer._saved_rows = self.outer.filter_rows()
                self.values_iter = iter(self.outer._saved_rows)
            self.row_number += 1
            return hxl.model.Row(self.outer.columns, next(self.values_iter), self.row_number)
        

#
# Utility classes
#
class Aggregator(object):
    """Class for aggregating a single value vertically through a dataset
.
    This is the class that accumulates a line count, sum, min, max, or average value
    across all rows of a dataset. Add any new aggregator types here.
    """

    def __init__(self, type='count', pattern=None, column=None):
        """Constructor
        See the L{parse} and L{parse_list} static methods for creating an aggregator from a string spec.
        @param type: the aggregator type to create, as a string
        @param pattern: the tag pattern for disaggregation (may be C{None} for just counting lines)
        @param column: the hashtag and attributes for the output column with aggregated values
        @exception HXLFilterException: if C{pattern} is C{None} and C{type} isn't C{"count"}
        """
        super().__init__()
        self.type = type.lower()
        if pattern:
            self.pattern = hxl.model.TagPattern.parse(pattern)
        elif type == 'count':
            self.pattern = None
        else:
            raise HXLFilterException('Pattern missing for {} aggregator'.format(type))
        if not column:
            column = '{type}#meta+{type}'.format(type=self.type)
        self.column = hxl.model.Column.parse_spec(column)

        self.total = 0
        """Total number of rows used."""

        self.value = None
        """Resulting aggregation value."""

        self.normalised = None
        """Normalised value to use for internal comparison"""

    def evaluate_row(self, row):
        """Evaluate a single row of HXL data against this aggregator.
        @param row: the input row to read
        @exception HXLFilterException: for an unrecognised aggregator type
        """

        #
        # Shortcut: just counting rows
        #
        if self.type == 'count':
            if self.value is None:
                self.value = 0
            self.value += 1
            return

        #
        # Aggregating values
        #
        value = row.get(self.pattern)

        # Skip empty values
        if value is '' or value is None:
            return

        # Numbers only for sum and average
        datatype = hxl.datatypes.typeof(value, self.pattern)
        if self.type in ['sum', 'average'] and datatype != 'number':
            logger.error("Cannot use %s as a numeric value for aggregation; skipping.", value)
            return

        normalised = hxl.datatypes.normalise(value, self.pattern)
        self.total += 1

        # aggregate as appropriate
        # note that we track a separate normalised value for strings and dates
        if self.type == 'sum':
            if self.value is None:
                self.value = 0
            self.value += normalised
        elif self.type == 'average':
            if self.value is None:
                self.value = 0
            self.value = ((self.value * (self.total - 1)) + normalised) / self.total
        elif self.type == 'min':
            if self.normalised is None or self.normalised > normalised:
                self.value = value
                self.normalised = normalised
        elif self.type == 'max':
            if self.normalised is None or self.normalised < normalised:
                self.value = value
                self.normalised = normalised
        else:
            raise HXLFilterException("Bad aggregator type for count filter: {}".format(type))

    TAG_PATTERN = '#?{token}(?:\s*[+-]{token})*'.format(token=hxl.datatypes.TOKEN_PATTERN)
    """Regular expression for a tag pattern"""
    
    COL_PATTERN = '#{token}(?:\s*\+{token})*'.format(token=hxl.datatypes.TOKEN_PATTERN)
    """Regular expression for an output column pattern"""

    AGGREGATOR_PATTERN = r'^\s*({token})\(({tag})?\)(?:\s*as\s+([^#]*)({col}))?$'.format(
        token = hxl.datatypes.TOKEN_PATTERN,
        tag = TAG_PATTERN,
        col = COL_PATTERN
    )
    """ Regular expression for an aggregation pattern
    Matches 1=aggregator, 2=tag pattern, 3=column header, 4=column tag
    """

    @staticmethod
    def parse(spec):
        """Parse a string specification and create an aggregator.
        Example: C{sum(#affected) as #affected+total}
        @param spec: the string specification
        @returns: an aggregator
        @exception HXLFilterException: if unable to parse, or an unrecognised aggregator type
        """
        if isinstance(spec, Aggregator):
            # in case it's already parsed
            return spec
        match = re.match(Aggregator.AGGREGATOR_PATTERN, spec)
        if not match:
            raise HXLFilterException("Malformed aggregator: {}".format(spec))
        return Aggregator(
            type=match.group(1),
            pattern=hxl.model.TagPattern.parse(match.group(2)) if match.group(2) else None,
            column=hxl.model.Column.parse(match.group(4), header=match.group(3), use_exception=True) if match.group(4) else None,
        )

    @staticmethod
    def parse_list(specs):
        """Parse a list of string specifications for aggregators
        Applies L{parse} to each item in the list
        @param specs: a list of string specifications for aggregators
        @returns: a list of L{Aggregator} objects
        @exception HXLFilterException: if unable to parse, or an unrecognised aggregator type
        """
        result = []
        if isinstance(specs, six.string_types):
            specs = [specs]
        for spec in specs:
            result.append(Aggregator.parse(spec))
        return result


#
# Filter classes
#

class AddColumnsFilter(AbstractStreamingFilter):
    """Composable filter class to add constant values to every row of a HXL dataset.

    This is a L{streaming filter<AbstractStreamingFilter>} to add
    constant values, such as a country code, to every row of data.  It
    supports the L{hxl.model.Dataset.add_columns} method and the
    L{hxladd<hxl.scripts.hxladd>} command-line utility.

    This example will add a column with the label "Country name", the
    HXL hashtag "#country", and the value "Malaysia" to every row in
    the dataset::

        filter = AddColumnsFilter(source, "Country name#country=Malaysia")

    This filter also supports substitution patterns in the fixed
    values, surrounded by double braces. For example, the pattern

        X-{{#country+code}}

    will include the value "X-" followed by the value of the column
    #country+code in each new cell in the new column.

    @see: L{ColumnFilter}, L{RenameFilter}

    """

    def __init__(self, source, specs, before=False):
        """Construct a new AddColumnsFilter.

        The I{source} parameter may be either a string or a list of
        strings (for multiple columns). Each string must have the
        following format (I{<header text>} is optional):

        I{<header text>}B{#}I{<tag and attributes>}B{=}I{<constant value>}

        Example::

            Country name#country=Malasia

        By default, this filter will add new columns to the end of
        existing ones, but you can use optional I{before} parameter to
        add them to the front.

        @param source: a HXL data source

        @param specs: a string or list of strings containing new
        column specifications, as described above (or a list of tuples
        as described in L{parse_spec})
        @param before: true to add new columns before existing ones (default C{False})

        """
        super().__init__(source)
        if isinstance(specs, six.string_types):
            # make a one-item list if the param is a string
            specs = [specs]
        self.specs = [AddColumnsFilter.parse_spec(spec) for spec in specs]
        self.before = before
        """If True, add new columns before existing ones"""
        self.const_values = None
        """Constant values to add to the new columns"""

    def filter_columns(self):
        """Internal: return the new columns list
        @returns: a list of L{hxl.model.Column} objects
        """
        new_columns = [spec[0] for spec in self.specs]
        if self.before:
            new_columns = new_columns + self.source.columns
        else:
            new_columns = self.source.columns + new_columns
        self.const_values = [spec[1] for spec in self.specs]
        return new_columns

    def filter_row(self, row):
        """Internal: return each row with the new fixed value(s) attached.
        Will execute pattern substitutions inside double braces for the fixed values.
        @returns: a list of values, including the fixed values for new columns
        """
        values = copy.copy(row.values)
        if self.before:
            return self._subst(row, self.const_values) + values
        else:
            return values + self._subst(row, self.const_values)

    _SUBST_PATTERN = '{{(#' + hxl.datatypes.TOKEN_PATTERN + '(?:[+-]' + hxl.datatypes.TOKEN_PATTERN + ')*)}}';
    """Regular expression to parse a substitution pattern in the fixed contents for the new cell"""

    def _subst(self, row, const_values):
        """Execute pattern substitutions for fixed values for the new columns.
        Substitutions are tag patterns inside double braces. Example: "X-{{#country+code}}"
        @param row: the row to search for matches to any patterns.
        @param const_values: the constant values to scan for patterns
        @returns: the constant values (only) with substitutions executed
        """
        def do_sub(match_object):
            return row.get(match_object.group(1))
        values = []
        for value in const_values:
            values.append(re.sub(AddColumnsFilter._SUBST_PATTERN, do_sub, value))
        return values

    SPEC_PATTERN = r'^\s*(?:([^#]*)#)?({token}(?:\s*\+{token})*)=(.*)\s*$'.format(token=hxl.datatypes.TOKEN_PATTERN)
    """Pattern for a string specification for a new column"""

    @staticmethod
    def parse_spec(spec):
        """Parse a new-column specification.

        The format is I{<header text>}B{#}I{<tag and
        attributes>}B{=}I{<fixed value>}. Example::

          Country name#country=Malaysia

        @param spec: the string spec to parse, in the format above.
        @returns: a tuple containing a L{hxl.model.Column} object and the fixed value.
        """

        if not isinstance(spec, six.string_types):
            return spec
        result = re.match(AddColumnsFilter.SPEC_PATTERN, spec)
        if result:
            header = result.group(1)
            tag = '#' + result.group(2)
            value = result.group(3)
            return (hxl.model.Column.parse(tag, header=header), value)
        else:
            raise HXLFilterException("Badly formatted new-column spec: " + spec)

    @staticmethod
    def _load(source, spec):
        """New instance from a JSON-style dictionary.
        @param source: the upstream data source
        @param spec: the JSON-style specification
        """
        return AddColumnsFilter(
            source=source,
            specs=req_arg(spec, 'specs'),
            before=opt_arg(spec, 'before', False)
        )


class AppendFilter(AbstractBaseFilter):

    """Composable filter class to concatenate two datasets.

    Usage::
    
        filter = AppendFilter(hxl.data(url), hxl.data(url2))

    This filter concatenates a second dataset to the end of the first
    one. It supports the L{hxl.model.Dataset.append} convenience
    method, and the L{hxl.scripts.hxlappend} command-line script.

    The filter preserves the order of the columns in the first
    dataset. If there are any columns in the second dataset that do
    not appear in the first, then the behaviour depends on the value
    of the I{add_columns} property:

      - if C{True} (default), add extra columns from the second dataset
        (and leave their values blank for the first).
      - if C{False}, ignore extra columns from the second dataset.

    Note that HXL tags must match exactly, including any tag
    attributes, for two columns to be considered as matches.

    A common use case is be to start with an empty dataset as a
    template so that the columns and headers are as desired in the
    final output, then append other datasets to that, with
    I{add_columns} set to C{False} to ignore any extra data in the
    dataset.

    To append multiple datasets, chain the filters::

        filter = HXLAppend(HXLAppend(hxl.data(url), hxl.data(url2), False), hxl.data(url3), False)

    Or, more intuitively (using the convenience methods)::

        hxl.data(url).append(url2, False).append(url3, False)

    If you have an unknown number of URLs to append, try something
    like this::

        source = hxl.data(template_url)
        for url in my_list_of_urls:
            source = AppendFilter(source, url, True)

    It's also possible to be selective about what you append, using the I{queries} parameter::

        filter = AppendFilter(source, source2, queries='org=UNICEF')

    In the second dataset, this filter will include I{only} rows where
    the value "UNICEF" appears under the C{#org} tag.

    This class class is a special case, neither a L{streaming
    filter<AbstractStreamingFilter>} nor a L{caching
    filter<AbstractCachingFilter>}; instead, it streams two separate
    datasets, one starting after the other, so it extends
    L{AbstractBaseFilter} directly, and implements its own custom
    iterator.

    @see: L{MergeDataFilter}, which combines two datasets horizontally
    rather than vertically.

    """

    def __init__(self, source, append_sources, add_columns=True, queries=[]):
        """Construct a new I{AppendFilter}
        @param source: a L{hxl.model.Dataset} object for the principal data
        @param append_sources: one or more L{hxl.model.Dataset} objects for the dataset to append (or strings containing URLs)
        @param add_columns: flag for adding extra columns in append_sources but not source (default True)
        @param queries: optional list of L{hxl.model.RowQuery} objects
        (or a single strig) to select which rows to include from the
        second dataset
        """
        super(AppendFilter, self).__init__(source)

        # parameters
        if is_sourcey(append_sources):
            append_sources = [append_sources]
        self.append_sources = [hxl.data(src) for src in append_sources] # so that we can take a plain URL
        """The sources to append to this source"""
        self.add_columns = add_columns
        """If true, always add new columns instead of replacing existing ones"""
        self.queries = self._setup_queries(queries)
        """The row queries to limit where we choose append candidates"""

        # internal properties
        self._column_positions = []
        """Cache of column positions, to avoid rescanning on every pass"""
        self._template_row = []
        """Empty template for appending to each row (will copy and fill in as needed"""

    def filter_columns(self):
        """Internal: generate the columns for the combined dataset
        We expect this method to run only once.
        @returns: the output list of L{hxl.model.Column} objects
        """

        columns_out = copy.deepcopy(self.source.columns)

        for i, append_source in enumerate(self.append_sources):

            columns_in = list(columns_out)
            self._column_positions.append({})

            # see if there's a corresponding column in the source
            for j, column in enumerate(append_source.columns):
                for k, original_column in enumerate(columns_in):
                    if column == original_column:
                        # yes, there is one; clear it, so it's not reused
                        self._column_positions[i][j] = k
                        columns_in[k] = None
                        break
                if self._column_positions[i].get(j) is None:
                    # no -- we need to add a new column
                    if self.add_columns:
                        self._column_positions[i][j] = len(columns_out)
                        columns_out.append(copy.deepcopy(column))
                    else:
                        self._column_positions[i][j] = None

        # make an empty template for each row
        self._template_row = [''] * len(columns_out)

        # return the (usually cached) columns
        return columns_out


    def __iter__(self):
        self.columns # make sure this is triggered first
        return AppendFilter._Iterator(self)

    class _Iterator:
        """Custom iterator to return the contents of all sources, in sequence."""

        def __init__(self, outer):
            """@param outer: reference to outer object"""
            self.outer = outer
            
            self._iterator = iter(outer.source)
            self._column_map = {i: i for i in range(len(self.outer.source.columns))}
            self._is_source = True

            self._sources = list(self.outer.append_sources)
            self._column_positions = list(self.outer._column_positions)

        def __iter__(self):
            return self

        def __next__(self):

            def make_row():
                row_in = next(self._iterator)
                while ((not self._is_source) and (not hxl.model.RowQuery.match_list(row_in, self.outer.queries))):
                    row_in = next(self._iterator)

                row_out = hxl.model.Row(
                    columns=self.outer.columns,
                    values=copy.deepcopy(self.outer._template_row)
                )

                for i, value in enumerate(row_in.values):
                    pos = self._column_map[i]
                    if pos is not None:
                        row_out.values[pos] = value

                return row_out

            while self._iterator is not None:
                try:
                    return make_row()
                except StopIteration:
                    if self._sources:
                        self._iterator = iter(self._sources[0])
                        self._column_map = self._column_positions[0]
                        self._sources = self._sources[1:]
                        self._column_positions = self._column_positions[1:]
                        self._is_source = False
                    else:
                        self._iterator = None

            raise StopIteration()

    @staticmethod
    def parse_external_source_list(source):
        append_sources = []
        for row in hxl.data(source):
            append_source = row.get('#x_source')
            if append_source:
                append_sources.append(append_source)
        return append_sources
        
    @staticmethod
    def _load(source, spec):
        """Create an AppendFilter from a dict spec.
        @param spec: the string specification
        """

        if spec.get('filter') == 'append_external_list':
            append_sources = AppendFilter.parse_external_source_list(
                hxl.data(req_arg(spec, 'source_list_url'))
            )
        else:
            append_sources = req_arg(spec, 'append_sources')
            
        return AppendFilter(
            source=source,
            append_sources=append_sources,
            add_columns=opt_arg(spec, 'add_columns', True),
            queries=opt_arg(spec, 'queries', [])
        )


class CacheFilter(AbstractBaseFilter):
    """Composable filter to cache HXL data in memory.

    This filter saves a copy of the HXL data in memory. It supports
    the L{hxl.model.Dataset.cache} method, and has no corresponding
    command-line script.

    Does not extend AbstractCachingFilter, because it needs to
    preserve original row numbers (when they exist).

    While L{streaming filters<AbstractStreamingFilter>} are more
    efficient, sometimes you need to keep a copy of your HXL data in
    memory, so that you can iterate over it more than once. Some
    filters, like L{SortFilter} and L{CountFilter}, do that as a side
    effect, but you can also choose to add a I{CacheFilter} explicitly
    to your chain.

    This filter does nothing but save a copy of your data for repeated
    use, as in the following example::

      filter = Cache(hxl.data(url))

    or::

      filter = hxl.data(url).cache()

    It is also possible to cache just I{part} of the data, as a
    preview or to avoid crashing on excessively-large datasets::

      # cache just the first 10 rows
      preview = Cache(hxl.data(url), 10)

    If there were more rows of data available, then the filter will
    set the L{overflow} property to C{True}.

    You can also use the cache filter strategically in a filter chain
    to save the results of an expensive operation (like replacing
    data) to avoid repeating it.  For example, this sequence will
    never run the replacements more than once::

      filter = hxl.data(url).replace_data_map(map_url).cache().with_rows('org=UNICEF')

    """

    def __init__(self, source, max_rows=None):
        """Constructor
        @param source: the upstream data source
        @param max_rows: if >0, maximum number of rows to cache
        """
        super().__init__(source)

        self.max_rows = max_rows
        """Maximum number of rows to keep in the cache (-1 means no limit)"""

        self.overflow = False
        """Flag for whether there were more rows than L{max_rows} available."""

        self.cached_rows = None

    @property
    def is_cached(self):
        return True

    def filter_columns(self):
        """@returns: a deep copy of the source columns"""
        return copy.deepcopy(self.source.columns)

    def __iter__(self):

        # if we haven't read the source yet, cache some rows
        if self.cached_rows is None:
            self.cached_rows = []
            for row_number, row in enumerate(self.source):
                # is there a limit?
                if self.max_rows is not None and row_number >= self.max_rows:
                    self.overflow = True
                    break
                else:
                    self.cached_rows.append(row)

        # return the iterator over the cached rows (repeatable)
        return iter(self.cached_rows)

    @staticmethod
    def _load(source, spec):
        """Create a new CacheFilter from a dict spec."""
        return CacheFilter(
            source=source,
            max_rows=opt_arg(spec, 'max_rows', None)
        )


class CleanDataFilter(AbstractStreamingFilter):
    """Data-cleaning filter.

    This filter can perform a set of automated cleaning tasks,
    including character-case conversion, whitespace normalisation, and
    date and number normalisation. It supports the
    L{hxl.model.Dataset.clean_data} method and the
    L{hxl.scripts.hxlclean} command-line script.

    The clean filter is especially useful when you are working with
    data from different sources who might use different date and
    number conventions, and also for datasets with inconsistent
    whitespace and capitalisation. The following functions are
    available (and can be filtered by row and column):

      - B{whitespace}: strip leading/trailing spaces, and normalise
        all internal whitespace (including lineends) to a single
        space.
      - B{upper}: convert text to all uppercase.
      - B{lower}: convert text to all lowercase.
      - B{date}: attempt to parse and normalise dates to ISO 8601
        format (YYYY-MM-DD). This will not always succeed -- for
        example, it is impossible to guess whether "9/6/15" refers to
        6 September 2016 or 9 June 2016 (hence the need for ISO dates)
        -- but it will do its best.
      - B{number}: attempt to normalise numbers to standard computer
        format (remove commas or spaces between thousands, and use "."
        as the decimal separator).
      - B{latlon}: attempt to normalise latitude and longitude values.
      
    For each type of cleaning, you specify one or more L{tag
    patterns<hxl.model.TagPattern>} to which the cleaning applies (you
    may use string representations instead of creating the objects),
    or use C{True} to apply the cleaning to the whole row. You may
    also include L{hxl.model.RowQuery} objects to apply the cleaning
    tasks only to specific rows. You can also specify a format for
    normalised dates or numbers, and choose to purge any dates,
    numbers, or latlon that can't be parsed (to guarantee clean data,
    at the cost of possible information loss).

    This example normalises all start dates from Oxfam::

      filter = CleanFilter(hxl.data('data.csv'), dates='date+start', queries='org=Oxfam')

      # or

      filter = hxl.data('data.csv').clean_data(dates='date+start', queries='org=Oxfam')

    @see: L{ReplaceDataFilter}, which allows for more-specific
    replacements using string and regular-expression patterns.
    """

    def __init__(
            self, source, whitespace=False, upper=[], lower=[], date=[], date_format=None,
            number=[], number_format=None, latlon=[], purge=False, queries=[]):
        """Construct a new data-cleaning filter.

        The I{upper}, I{lower}, I{date}, I{number}, and I{latlon}
        arguments all accept either lists of tag
        patterns<hxl.model.TagPattern or individual patterns, which
        can be strings (like C{#org+impl-code}) or full
        L{hxl.model.TagPattern} objects. The I{queries} argument
        accepts either lists of queries or individual queries, which
        can be strings (like C{org=Oxfam}) or full
        L{hxl.model.RowQuery} objects.

        @param source: a L{hxl.model.Dataset} object to filter
        @param whitespace: a tag pattern or list of tag patterns for whitespace normalisation
        @param upper: a tag pattern or list of tag patterns for conversion to uppercase
        @param lower: a tag pattern or list of tag patterns for conversion to lowercase
        @param date: a tag pattern or list of tag patterns for date normalisation
        @param date_format: a date-format string for output, as used by strftime
        @param number: a tag pattern or list of tag patterns for number normalisation
        @param number_format: a number-format string for output, as used by format.
        @param laton: a list of tag patterns for normalising latitude/longitude.
        @param purge: if True, remove any dates, numbers, or lat/lon that can't be parsed during cleaning.
        @param queries: optional list of queries to select rows to be cleaned.
        """
        super(CleanDataFilter, self).__init__(source)
        self.whitespace = hxl.model.TagPattern.parse_list(whitespace)
        self.upper = hxl.model.TagPattern.parse_list(upper)
        self.lower = hxl.model.TagPattern.parse_list(lower)
        self.date = hxl.model.TagPattern.parse_list(date)
        self.date_format = date_format
        self.number = hxl.model.TagPattern.parse_list(number)
        self.number_format = number_format
        self.latlon = hxl.model.TagPattern.parse_list(latlon)
        self.purge = purge
        self.queries = self._setup_queries(queries)

        # We need to prescan for dates
        if date:
            self.source = self.source.cache();
            self.date_dayfirst = self._guess_dayfirst()

    def filter_row(self, row):
        """@returns: cleaned row data"""
        if hxl.model.RowQuery.match_list(row, self.queries):
            # if there are no queries, or row matches at least one
            columns = self.columns
            values = copy.copy(row.values)
            for i in range(min(len(values), len(columns))):
                values[i] = self._clean_value(values[i], columns[i])
            return values
        else:
            # otherwise, leave as-is
            return row.values

    def _guess_dayfirst(self):
        """Guess whether the default should be DD-MM-YYYY or MM-DD-YYYY
        @returns: true if we should default to dayfirst format
        """
        ddmm_count = 0
        mmdd_count = 0
        
        # prescan columns to speed things up
        indices = []
        for i, column in enumerate(self.source.columns):
            if hxl.TagPattern.match_list(column, self.date):
                indices.append(i)

        # if there are any matching columns, then prescan values
        if indices:
            for row in self.source:
                for i in indices:
                    value = row.values[i]
                    if value:
                        result = re.match(r'^[^\d]*(\d\d?)[^\d]+(\d\d?)[^\d].*$', value)
                        if result:
                            if int(result.group(1)) > 12:
                                ddmm_count += 1
                            elif int(result.group(2)) > 12:
                                mmdd_count += 1

        return (ddmm_count >= mmdd_count)

    
    def _clean_value(self, value, column):
        """Clean a single value, using the column def for guidance.
        @returns: a single cleaned value
        """
        value = str(value)

        # Whitespace (-w)
        if self._match_patterns(self.whitespace, column):
            value = re.sub('^\s+', '', value)
            value = re.sub('\s+$', '', value)
            value = re.sub('\s+', ' ', value)

        # Uppercase (-u)
        if self._match_patterns(self.upper, column):
            value = value.upper()

        # Lowercase (-l)
        if self._match_patterns(self.lower, column):
            value = value.lower()

        # Date
        if self._match_patterns(self.date, column):
            if value:
                if hxl.datatypes.is_date(value):
                    if self.date_format is not None:
                        value = dateutil.parser.parse(value).strftime(self.date_format)
                    else:
                        value = hxl.datatypes.normalise_date(value, self.date_dayfirst)
                else:
                    logger.warning('Cannot parse %s as a date', str(value))
                    if self.purge:
                        value = ''

        # Number
        if self._match_patterns(self.number, column):

            def try_number(s):
                try:
                    n = float(s)
                    if self.number_format:
                        return format(n, self.number_format)
                    elif n.is_integer():
                        return str(int(n))
                    else:
                        return str(n)
                except:
                    return None

                # fixme - get much smarter about numbers
            if value:
                n = try_number(value)
                if n is None:
                    s = re.sub('[^\de.]+', '', value)
                    n = try_number(s) # OK, try again
                if n is not None:
                    value = n
                else:
                    logger.warning('Cannot parse {} as a number', str(value))
                    if self.purge:
                        value = ''

        # Latlon
        if self._match_patterns(self.latlon, column):
            if 'lat' in column.attributes:
                lat = hxl.geo.parse_lat(value)
                if lat is not None:
                    value = format(lat, '0.4f')
                else:
                    logger.warning('Cannot parse %s as a latitude', str(value))
                    if self.purge:
                        value = ''
            elif 'lon' in column.attributes:
                lon = hxl.geo.parse_lon(value)
                if lon is not None:
                    value = format(lon, '0.4f')
                else:
                    logger.warning('Cannot parse %s as a longitude', str(value))
                    if self.purge:
                        value = ''
            elif 'coord' in column. attributes:
                coord = hxl.geo.parse_coord(value)
                if coord is not None:
                    value = '{:.4f},{:.4f}'.format(coord[0], coord[1])
                else:
                    logger.warning('Cannot parse %s as geographical coordinates', str(value))
                    if self.purge:
                        value = ''
        
        return value

    def _match_patterns(self, patterns, column):
        """Test if a column matches a list of patterns.
        @param patterns: a list of tag patterns to match
        @param column: the column definition to test
        @returns: C{True} if the column matches at least one pattern in the list
        """
        if not patterns:
            return False
        else:
            for pattern in patterns:
                if pattern.match(column):
                    return True
            return False

    @staticmethod
    def _load(source, spec):
        """Create a new clean-data filter from a dict spec.
        @returns: a L{CleanDataFilter} object
        """
        return CleanDataFilter(
            source=source,
            whitespace=opt_arg(spec,'whitespace', []),
            upper=opt_arg(spec, 'upper', []),
            lower=opt_arg(spec, 'lower', []),
            date=opt_arg(spec, 'date', []),
            date_format=opt_arg(spec, 'date_format', '%Y-%m-%d'),
            number=opt_arg(spec, 'number', []),
            number_format=opt_arg(spec, 'number_format', None),
            latlon=opt_arg(spec, 'latlon', []),
            purge=opt_arg(spec, 'purge', False),
            queries=opt_arg(spec, 'queries', [])
        )


class ColumnFilter(AbstractStreamingFilter):
    """Composable filter class to remove columns from a HXL dataset.

    This filter supports removing columns based on either a whitelist
    or a blacklist of L{tag patterns<hxl.model.TagPattern>}. It
    supports the L{hxl.model.Dataset.with_columns} and
    L{hxl.model.Dataset.without_columns} convenience methods and the
    L{hxl.scripts.hxlcut} command-line script.

    Remove all columns matching the pattern "#contact+email" from the
    dataset::

      filter = ColumnFilter(hxl.data(url), exclude_tags='contact+email')

      # or

      filter = hxl.data(url).without_columns('contact+email')

    Remove all columns I{except} those matching the patterns '#org',
    '#sector', and '#activity'::

      filter = ColumnFilter(hxl.data(url), include_tags=['org', 'sector', 'activity'])

      # or

      filter = hxl.data(url).with_columns(['org', 'sector', 'activity'])

    @see: L{RowFilter}
    """

    def __init__(self, source, include_tags=[], exclude_tags=[], skip_untagged=False):
        """Construct a column filter.
        @param source: a L{hxl.model.Dataset}
        @param include_tags: a whitelist of L{tag patterns<hxl.model.TagPattern>} objects to include
        @param exclude_tags: a blacklist of tag patterns objects to exclude
        @param skip_untagged: True if all columns without HXL hashtags should be removed
        """
        super(ColumnFilter, self).__init__(source)
        self.include_tags = hxl.model.TagPattern.parse_list(include_tags)
        self.exclude_tags = hxl.model.TagPattern.parse_list(exclude_tags)
        self.skip_untagged = skip_untagged
        self.indices = [] # saved indices for columns to include

    def filter_columns(self):
        """@returns: filtered list of column definitions"""
        columns_in = self.source.columns
        columns_out = []
        for i in range(len(columns_in)):
            if self._test_column(columns_in[i]):
                columns_out.append(copy.deepcopy(columns_in[i]))
                self.indices.append(i) # save index to avoid retesting for data
        return columns_out

    def filter_row(self, row):
        """@returns: filtered list of row values"""
        values = []
        for i in self.indices:
            try:
                values.append(row.values[i])
            except IndexError:
                pass # don't add anything
        return values

    def _test_column(self, column):
        """Test whether a  column should be included in the output.
        If there is a whitelist, it must be in the whitelist; if there is a blacklist, it must not be in the blacklist.
        @param column: the L{hxl.model.Column} to test
        @returns: True if the column should be included
        """
        if self.include_tags:
            # whitelist
            for pattern in self.include_tags:
                if pattern.match(column):
                    # succeed as soon as we match an included pattern
                    return True
            # fail if there was a whitelist and we didn't match
            return False

        if self.exclude_tags or self.skip_untagged:
            # skip untagged columns?
            if self.skip_untagged and not column.tag:
                return False
            # blacklist
            for pattern in self.exclude_tags:
                if pattern.match(column):
                    # fail as soon as we match an excluded pattern
                    return False

        # not a whitelist, and no reason to exclude
        return True

    @staticmethod
    def _load(source, spec):
        """Create a filter object from a JSON-like spec.
        Note that there are two JSON filter definitions for this class: "with_columns" and "without_columns".
        @param spec: the specification
        @returns: a L{ColumnFilter} object
        """
        if spec.get('filter') == 'with_columns':
            return ColumnFilter(
                source=source,
                include_tags=req_arg(spec, 'whitelist')
            )
        else:
            return ColumnFilter(
                source=source,
                exclude_tags=req_arg(spec, 'blacklist'),
                skip_untagged=opt_arg(spec, 'skip_untagged')
            )


class CountFilter(AbstractCachingFilter):
    """Composable filter class to aggregate rows in a HXL dataset.

    This class supports the L{hxl.model.Dataset.count} convenience
    method and the L{hxl.scripts.hxlcount} command-line script.

    This is a L{caching filter<AbstractCachingFilter>} that performs
    aggregate actions such as counting, summing, and averaging across
    multiple rows of data. For example, it can reduce a dataset to a
    list of the number of times that each organisation or sector
    appears. This is the main filter for producing reports, or the
    data underlying charts and other visualisations; it is also useful
    for anonymising data by rolling it up to higher levels of
    abstraction.

    This example counts the number of rows for each organisation::

      filter = CountFilter(hxl.data(url), 'org')

      # or

      filter = hxl.data(url).count('org')

    You can do multiple levels of counting like this::

      filter = hxl.data(url).count(['org', 'sector'])

    You can also use the I{queries} argument to limit the counting to
    specific fields. This example will count only the rows where C{#adm1} is set to "Coast"::

      filter = hxl.data(url).count('org', queries='adm1=Coast')
    """

    def __init__(self, source, patterns, aggregators=None, queries=[]):
        """Construct a new count filter
        If the caller does not supply any aggregators, use "count() as Count#meta+count"
        @param source: a L{hxl.model.Dataset}
        @param patterns: a single L{tag pattern<hxl.model.TagPattern>} or list of tag patterns that, together, form a unique key for counting.
        @param aggregators: one or more Aggregator objects or string representations to define the output.
        @param queries: an optional list of L{row queries<hxl.model.RowQuery>} to filter the rows being counted.
        """
        super().__init__(source)
        self.patterns = hxl.model.TagPattern.parse_list(patterns)
        if not aggregators:
            aggregators = 'count() as Count#meta+count'
        self.aggregators = Aggregator.parse_list(aggregators)
        self.queries = self._setup_queries(queries)

    def filter_columns(self):
        """@returns: the filtered columns"""
        columns = []

        # Add columns being counted
        for pattern in self.patterns:
            column = pattern.find_column(self.source.columns)
            if column:
                columns.append(copy.deepcopy(column))
            else:
                columns.append(hxl.Column())

        # Add generated columns
        for aggregator in self.aggregators:
            columns.append(aggregator.column)
            
        return columns

    def filter_rows(self):
        """@returns: the filtered row values"""

        raw_data = []

        # each item is a sequence containing a tuple of key values and an _Aggregator object
        for aggregate in self._aggregate_data():
            raw_data.append(
                list(aggregate[0]) + [aggregator.value if aggregator.value is not None else '' for aggregator in aggregate[1]]
            )
            
        return raw_data

    def _aggregate_data(self):
        """Read the entire source dataset and produce saved aggregate data.
        @returns: the aggregated values as raw data
        """
        aggregators = {}

        # read the whole source dataset at once
        for row in self.source:
            # will always match if there are no queries
            if hxl.model.RowQuery.match_list(row, self.queries):
                # get the values in the order we need them
                values = [hxl.datatypes.normalise_space(row.get(pattern, default='')) for pattern in self.patterns]
                # make a dict key for the aggregator
                key = tuple(values)
                if not key in aggregators:
                    aggregators[key] = [copy.copy(aggregator) for aggregator in self.aggregators]
                for aggregator in aggregators[key]:
                    aggregator.evaluate_row(row)

        # sort the aggregators by their keys
        return sorted(aggregators.items())

    @staticmethod
    def _load(source, spec):
        """Create a new count filter from a dict spec.
        @param spec: the JSON-like spec
        @returns: a new L{CountFilter} object
        """
        return CountFilter(
            source = source,
            patterns=opt_arg(spec, 'patterns'),
            aggregators=opt_arg(spec, 'aggregators', None),
            queries=opt_arg(spec, 'queries', [])
        )


class DeduplicationFilter(AbstractStreamingFilter):
    """Composable filter to deduplicate a HXL dataset.

    Removes duplicate lines from a dataset, where "duplicate" is
    optionally defined by a set of keys specified by the user. As a
    result, not all values in duplicate rows will necessarily be
    identical. The filter will always return the *first* matching row
    of a set of duplicates.

    Supports the hxldedup command-line script.

    TODO: add more-sophisticated matching, edit distance, etc.
    """

    def __init__(self, source, patterns=None, queries=[]):
        """
        Constructor
        @param source: the upstream source dataset
        @param patterns: if provided, a list of tag patterns for columns to use for uniqueness testing.
        @param filters: optional list of filter queries for columns to be considered for deduplication.
        """
        super().__init__(source)
        self.patterns = hxl.model.TagPattern.parse_list(patterns)
        self.seen_map = set() # row signatures that we've seen so far
        self.queries = self._setup_queries(queries)

    def filter_row(self, row):
        """@returns: the row's values, or C{None} if it's a duplicate"""
        if hxl.model.RowQuery.match_list(row, self.queries):
            if not row:
                return None
            key = row.key(self.patterns)
            if key in self.seen_map:
                return None
            # if we get to here, we haven't seen the row before
            self.seen_map.add(key)
            return copy.copy(row.values)
        else:
            return row.values

    @staticmethod
    def _load(source, spec):
        """Create a dedup filter from a dict spec.
        @param source: the upstream source
        @param spec: the JSON-like spec
        @returns: a L{DeduplicationFilter} object
        """
        return DeduplicationFilter(
            source = source,
            patterns=opt_arg(spec, 'patterns', []),
            queries=opt_arg(spec, 'queries', [])
        )

        
class ExplodeFilter(AbstractBaseFilter):
    """Explode a wide (series) dataset into a long version.

    Every set of identically-tagged columns that contain the +label
    attribute in their hashtag will get their own row, with the header
    text and the value side by side.  For example,

    Country,2015,2014,2013
    #country,#affected+label,#affected+label,#affected+label
    Cameroon,100,150,120

    will be converted to

    Country,Header,Value
    #country,#affected+header,#affected+value
    Cameroon,2015,100
    Cameroon,2014,150
    Cameroon,2015,120

    (You can use the RenameFilter to change the names and hashtags of
    the generated columns.)

    @see: hxl.model.Dataset.explode
    """

    def __init__(self, source, header_attribute='header', value_attribute='value'):
        """
        Constructor
        @param source: the upstream source dataset
        @param header_attribute: the attribute to add to the hashtag for the column with the former header (default: 'header')
        @param value_attribute: the attribute to add to the hashtag for the column with the former header (default: 'value')
        """
        super(ExplodeFilter, self).__init__(source)
        self.header_attribute = header_attribute
        """Attribute to add to exploded values from the header"""
        self.value_attribute = value_attribute
        """Attribute to add to the original value from the data cell"""
        self._generator = None
        self._plan = self._make_plan()

    def filter_columns(self):
        """@returns: the new (exploded) column headers"""
        columns = []
        for spec in self._plan:
            if isinstance(spec, list):
                model_column = self.source.columns[spec[0]]
                columns.append(copy.deepcopy(model_column).remove_attribute('label').add_attribute(self.header_attribute))
                columns.append(copy.deepcopy(model_column).remove_attribute('label').add_attribute(self.value_attribute))
            else:
                columns.append(self.source.columns[spec])
        return columns

    def __iter__(self):
        """Custom iterator to produce exploded rows."""
        for row in self.source:
            for values in self._expand(row, self._plan):
                yield hxl.model.Row(self.columns, values)

    def _expand(self, row, plan, values_in=[]):
        """Recursive generator for the row data.
        https://wiki.python.org/moin/Generators
        @param row: the row to expland
        @param plan: the pre-generated expansion plan
        @param values_in: the input values
        """
        if not plan: # terminal condition
            yield values_in
        else:
            spec = plan[0]
            plan = plan[1:]
            if isinstance(spec, list): # multiple branches
                for index in spec:
                    values = values_in + [row.columns[index].header, row.values[index]]
                    for values_out in self._expand(row, plan, values):
                        yield values_out
            else: # continue on a single branch
                values = values_in + [row.values[spec]]
                for values_out in self._expand(row, plan, values):
                    yield values_out

    def _make_plan(self):
        """Create an expansion plan
        The plan is a list of integers, representing columns in the original source.
        Some items are lists of integers, representing variants to show for multiple rows.
        @returns: an expansion plan for the row
        """
        plan = []
        groups = {}
        for index, column in enumerate(self.source.columns):
            if 'label' in column.attributes:
                if column not in groups:
                    plan.append([index])
                    groups[column] = len(plan) - 1;
                else:
                    plan[groups.get(column)].append(index)
            else:
                plan.append(index)
        return plan

    @staticmethod
    def _load(source, spec):
        """Create an explode filter from a dict spec.
        @param source: the upstream source
        @param spec: the JSON-like filter specification
        @returns: an L{ExplodeFilter} object
        """
        return ExplodeFilter(
            source=source,
            header_attribute=opt_arg(spec, 'header_attribute', 'header'),
            value_attribute=opt_arg(spec, 'value_attribute', 'value')
        )


class MergeDataFilter(AbstractStreamingFilter):
    """Composable filter class to merge values from two HXL datasets.

    Merges the values for the *last* matching row in the merge
    dataset. Can use patterns to match multiple cells for merging
    (using all candidate columns when there are muliple columns
    matching the same hashtag pattern). Can overwrite existing columns
    and values.

    Warning: this filter may store a large amount of data in memory,
    depending on the merge.

    Usage:

    <pre>
    MergeDataFilter(source, merge_source=merge_source, keys='adm1+code', tags='adm1+name')
    </pre>

    <pre>
    hxl.data(url).merge_data(merge_source=merge_source, keys='adm1+code', tags='adm1+name')
    </pre>

    (Add the column matching #adm1+name from the merge dataset to the
    source dataset, syncing the rows using the value of #adm1+code in
    each dataset.)

    @see hxl.model.Dataset.merge_data
    @see hxl.scripts.hxlmerge_main
    """

    def __init__(self, source, merge_source, keys, tags, replace=False, overwrite=False, queries=[]):
        """
        Constructor.
        @param source: the HXL data source.
        @param merge_source: a second HXL data source to merge into the first.
        @param keys: the shared key hashtags to use for the merge
        @param tags: the tags to include from the second dataset
        @param replace: if True, replace existing columns when possible
        @param overwrite: if True, overwrite non-empty values in existing columns
        @param queries: optional list of filter queries for rows to be considered from the merge dataset.
        """
        super().__init__(source)
        self.merge_source = merge_source
        """The source dataset for pulling merged values"""
        self.keys = hxl.model.TagPattern.parse_list(keys)
        """The shared keys for merging"""
        self.merge_tags = hxl.model.TagPattern.parse_list(tags)
        """The merged values to pull from the merge_source"""
        self.replace = replace
        """If True, replace columns when possible"""
        self.overwrite = overwrite
        """If true, overwrite non-empty values in existing columns"""
        self.queries = self._setup_queries(queries)
        """Query to filter rows to be merged"""
        self._merge_indices = []
        """Indices for mapping columns from merge source to output dataset
        [source_index, output_index, overwrite_ok]
        """
        self._merge_values = None
        """Dictionary of values from merge source, indexed by key."""

    def filter_columns(self):
        """Filter the columns to add newly-merged ones.  
        Note: this is called only once, the first time someone
        accesses the Dataset.columns property, then the result is saved for future use.
        As a side effect, builds the _merge_indices specs for generating the merged data.
        @see hxl.filters.AbstractBaseFilter.filter_columns
        """

        new_columns = list(self.source.columns)
        """The new column list to return."""
        
        merge_column_index = len(self.source.columns)
        """Target index for merging into the output dataset"""
            
        # Check every pattern
        for pattern in self.merge_tags:

            # Check the pattern against every column
            for index, column in enumerate(self.merge_source.columns):

                seen_replacement = False
                """We can replace inline exactly once for every pattern."""

                if pattern.match(column):

                    # Replace inside existing columns, if conditions are met
                    if self.replace and not seen_replacement and pattern.find_column(self.source.columns):
                        # TODO: check for closest match
                        # TODO: allow for multiple replacements per pattern
                        self._merge_indices.append([index, pattern.find_column_index(self.source.columns), self.overwrite])
                        seen_replacement = True

                    # Replace into a new column on the right
                    else:
                        new_columns.append(column)
                        self._merge_indices.append([index, merge_column_index, True])
                        merge_column_index += 1

        return new_columns

    def filter_row(self, row):
        """Set up a merged data row, replacing existing values if requested.
        Uses the _merge_indices map created by filter_columns.
        @param row: the data row to filter.
        @returns: a list of filtered values for the row.
        @see hxl.filters.AbstractStreamingFilter.filter_row
        """

        # First, check if we already have the merge map, and read it if not
        if self._merge_values is None:
            self._merge_values = self._read_merge()

        # Make an initial array of the correct length
        values = copy.copy(row.values)
        values += ([''] * (len(self.columns) - len(row.values)))

        # Look up the merge values, based on the keys
        for key in self._make_keys(row):
            merge_values = self._merge_values.get(key)
            if merge_values:
                for i, spec in enumerate(self._merge_indices):
                    if spec[2] or hxl.datatypes.is_empty(values[spec[1]]):
                        values[spec[1]] = merge_values[i]

        return values

    def _make_keys(self, row):
        """Return all possible key-value combinations for the row as tuples.
        @param row: the row from which to generate the key
        """
        candidate_values = []
        for pattern in self.keys:
            candidate_values.append(hxl.datatypes.normalise_string(value) for value in row.get_all(pattern, default=''))
        return [tuple(value) for value in list_product(candidate_values)]

    def _read_merge(self):
        """Read the second (merging) dataset into memory.
        Stores only the values necessary for the merge.
        Uses *last* matching row for each key (top to bottom).
        @returns: a map of merge values
        """
        
        self.columns # make sure we've created the _merge_indices map

        merge_values = {}
        """Map of keys to merge values from the merge source."""

        for row in self.merge_source:
            if hxl.model.RowQuery.match_list(row, self.queries):
                values = []

                # Save only the values we need
                for spec in self._merge_indices:
                    try:
                        values.append(row.values[spec[0]])
                    except IndexError:
                        values.append('')

                # Generate a key tuple and add to the map
                for key in self._make_keys(row):
                    merge_values[key] = values

        return merge_values

    @staticmethod
    def _load(source, spec):
        """Create a merge filter from a dict spec.
        @param source: the upstream data source
        @param spec: the JSON-like filter specification
        @returns: a L{MergeDataFilter} object
        """
        return MergeDataFilter(
            source=source,
            merge_source=req_arg(spec, 'merge_source'),
            keys=req_arg(spec, 'keys'),
            tags=req_arg(spec, 'tags'),
            replace=opt_arg(spec, 'replace', False),
            overwrite=opt_arg(spec, 'overwrite', False),
            queries=opt_arg(spec, 'queries', [])
        )


class RenameFilter(AbstractStreamingFilter):
    """
    Composable filter class to rename columns in a HXL dataset.

    This is the class supporting the hxlrename command-line utility.

    Usage:

    <pre>
    hxl.data(url).rename_columns('#foo:New header#bar')
    </pre>
    """

    def __init__(self, source, rename=[]):
        """
        Constructor
        @param source: the Dataset for the data.
        @param rename_map: map of tags to rename
        """
        super().__init__(source)
        if isinstance(rename, six.string_types):
            rename = [rename]
        self.rename = [RenameFilter.parse_rename(spec) for spec in rename]

    def filter_columns(self):
        """Rename requested columns."""
        return [self._rename_column(column) for column in self.source.columns]

    def filter_row(self, row):
        """@returns: a deep copy of the row's values"""
        return copy.copy(row.values)

    def _rename_column(self, column):
        """@returns: a copy of the column object, with a new name if needed"""
        for spec in self.rename:
            if spec[0].match(column):
                new_column = copy.deepcopy(spec[1])
                if new_column.header is None:
                    new_column.header = column.header
                return new_column
        return copy.deepcopy(column)

    RENAME_PATTERN = r'^\s*#?({token}(?:\s*[+-]{token})*):(?:([^#]*)#)?({token}(?:\s*[+]{token})*)\s*$'.format(token=hxl.datatypes.TOKEN_PATTERN)
    """Regular expression for parsing a rename pattern"""

    @staticmethod
    def parse_rename(s):
        """Parse a rename specification from the parameters.
        @param s: the specification to parse
        @returns: a tuple with the old pattern to match and new column spec
        @exception HXLFilterException: if the spec is not parseable
        """
        if isinstance(s, six.string_types):
            result = re.match(RenameFilter.RENAME_PATTERN, s)
            if result:
                pattern = hxl.model.TagPattern.parse(result.group(1))
                column = hxl.model.Column.parse('#' + result.group(3), header=result.group(2), use_exception=True)
                return (pattern, column)
            else:
                raise HXLFilterException("Bad rename expression: " + s)
        else:
            return s

    @staticmethod
    def _load(source, spec):
        """Create a rename filter from a dict spec.
        @param source: the upstream data source
        @param spec: the JSON-like filter specification
        @returns: a L{RenameFilter} object
        """
        return RenameFilter(
            source=source,
            rename=req_arg(spec, 'specs')
        )

    
class JSONPathFilter(AbstractStreamingFilter):
    """Extract values from a JSON string expression using JSONPath
    See http://goessner.net/articles/JsonPath/
    Optionally restrict to specific columns and/or rows
    """

    def __init__(self, source, path, patterns=None, queries=[]):
        """Constructor
        @param source: the upstream data source
        @param path: a JSONPath expression for extracting data
        @param patterns: a tag pattern or list of patterns for the columns to use (default to all)
        @param queries: a predicate or list of predicates for the rows to consider.
        """
        super().__init__(source)
        self.path = jsonpath_rw.parse(path)
        self.patterns = hxl.model.TagPattern.parse_list(patterns)
        self.queries = self._setup_queries(queries)
        self._indices = None

    def filter_row(self, row):
        if self._indices is None:
            self._indices = self._get_indices(self.patterns)

        values = list(row.values)

        if hxl.model.RowQuery.match_list(row, self.queries):
        
            for i in self._indices:
                try:
                    expr = json.loads(values[i])
                    results = [match.value for match in self.path.find(expr)]
                    if len(results) == 0:
                        values[i] = ''
                    elif len(results) == 1:
                        values[i] = hxl.datatypes.flatten(results[0])
                    else:
                        values[i] = hxl.datatypes.flatten(results)
                except ValueError as e:
                    logger.warning("Skipping invalid JSON expression '%s'", values[i])

        return values
    
    @staticmethod
    def _load(source, spec):
        """Create a JSONPath filter from a dict spec.
        @param source: the upstream data source
        @param spec: the JSON-like filter specification
        @returns: a L{RenameFilter} object
        """
        return JSONPathFilter(
            source=source,
            path=req_arg(spec, 'path'),
            patterns=opt_arg(spec, 'patterns'),
            queries=opt_arg(spec, 'queries')
        )

    
class FillDataFilter(AbstractStreamingFilter):
    """Fill empty cells in a dataset.
    By default, fill all empty cells with the closest non-empty value in a previous row.
    Optionally restrict to specific columns and/or rows.
    """

    def __init__(self, source, patterns=None, queries=[]):
        """Constructor
        @param source: the source dataset
        @param patterns: a tag pattern or list of patterns for the columns to fill (default to all)
        @param queries: restrict filling to rows matching one of these queries (default: fill all rows).
        """
        super().__init__(source)
        self.patterns = hxl.model.TagPattern.parse_list(patterns)
        self.queries = self._setup_queries(queries)
        self._saved = {}
        self._indices = None

    def filter_row(self, row):
        """@returns: row values with some empty values possibly filled in"""
        values = list(row.values)

        # Fill if there are no row queries, or this row matches one
        if self._indices is None:
            self._indices = self._get_indices(self.patterns)
        for i in self._indices:
            if values[i]:
                self._saved[i] = values[i]
            elif (not self.queries) or (hxl.model.RowQuery.match_list(row, self.queries)):
                values[i] = self._saved[i] if self._saved.get(i) else ''
                    
        return values

    @staticmethod
    def _load(source, spec):
        """Create a fill-data filter from a dict spec.
        @param source: the upstream data source
        @param spec: the JSON-like filter specification
        @returns: a L{FillDataFilter} object
        """
        return FillDataFilter(
            source=source,
            patterns=opt_arg(spec, 'patterns'),
            queries=opt_arg(spec, 'queries'),
        )

    
class ReplaceDataFilter(AbstractStreamingFilter):
    """
    Composable filter class to replace values in a HXL dataset.

    This is the class supporting the hxlreplace console script.

    Usage:

    <pre>
    hxl.data(url).replace_data('foo', 'bar', '#activity')
    </pre>
    """

    def __init__(self, source, replacements, queries=[]):
        """
        Constructor
        @param source: the HXL data source
        @param original: a string or regular expression to replace (string must match the whole value, not just part)
        @param replacements: list of replacement objects
        @param queries: optional list of filter queries for rows where replacements should be applied.
        """
        super(ReplaceDataFilter, self).__init__(source)
        self.replacements = replacements
        if isinstance(self.replacements, ReplaceDataFilter.Replacement):
            self.replacements = [self.replacements]
        self.queries = self._setup_queries(queries)

    def filter_row(self, row):
        """@returns: the row values with replacements"""
        if hxl.model.RowQuery.match_list(row, self.queries):
            values = copy.copy(row.values)
            for index, value in enumerate(values):
                for replacement in self.replacements:
                    value = replacement.sub(row.columns[index], value)
                    values[index] = value
            return values
        else:
            return row.values

    class Replacement:
        """Replacement specification."""

        def __init__(self, original, replacement, patterns=None, is_regex=False):
            """
            @param original: a string (case- and space-insensitive) or regular expression (sensitive) to replace
            @param replacement: the replacement string or regular expression substitution
            @param patterns: (optional) a list of tag patterns to limit the replacement to specific columns
            @param is_regex: (optional) True to use regular-expression processing (defaults to False)
            """
            self.original = original
            if replacement is None:
                self.replacement = ''
            else:
                self.replacement = replacement
            if patterns:
                self.patterns = hxl.model.TagPattern.parse_list(patterns)
            else:
                self.patterns = None
            self.is_regex = is_regex
            if not self.is_regex:
                self.original = hxl.datatypes.normalise_string(self.original)

        def sub(self, column, value):
            """
            Substitute inside the value, if appropriate.
            @param column: the column definition
            @param value: the cell value
            @returns: the value, possibly changed
            """
            if self.patterns and not hxl.model.TagPattern.match_list(column, self.patterns):
                return value
            elif self.is_regex:
                return re.sub(self.original, self.replacement, str(value))
            elif self.original == hxl.datatypes.normalise_string(value):
                return self.replacement
            else:
                return value

        @staticmethod
        def parse_map(source):
            """Parse a substitution map."""
            replacements = []
            for row in source:
                if row.get('#x_pattern'):
                    replacements.append(
                        ReplaceDataFilter.Replacement(
                            row.get('#x_pattern'), row.get('#x_substitution'),
                            row.get('#x_tag'), row.get('#x_regex')
                        ))
            return replacements

    @staticmethod
    def _load(source, spec):
        """Create a replace-data filter from a dict spec.
        @param source: the upstream data source
        @param spec: a JSON-like filter specification
        @returns: a L{ReplaceDataFilter} object
        """

        replacements = []

        if spec.get('filter') == 'replace_data_map':
            # using an external map
            replacements = ReplaceDataFilter.Replacement.parse_map(
                hxl.data(req_arg(spec, 'map_source'))
            )
        elif spec.get('filter') == 'replace_data':
            # simple replacement
            replacements = [
                ReplaceDataFilter.Replacement(
                    original=req_arg(spec, 'original'),
                    replacement=req_arg(spec, 'replacement'),
                    patterns=opt_arg(spec, 'pattern', None),
                    is_regex=opt_arg(spec, 'use_regex', False)
                )
            ]

        return ReplaceDataFilter(
            source=source,
            replacements=replacements,
            queries=opt_arg(spec, 'queries', [])
        )

        
class RowCountFilter(AbstractStreamingFilter):
    """
    Composable filter class to count lines (and nothing else)

    The output is identical to the input; the line count is
    stored in the filter itself.  As a result, there is no corresponding
    command-line utility.
    
    Usage:

    <pre>
    counter = hxl.data(url).row_counter();
    // process the filter
    print("{} lines".format(counter.row_count);
    </pre>
    """

    def __init__(self, source, queries=[]):
        super(RowCountFilter, self).__init__(source)
        self.row_count = 0
        self.queries = self._setup_queries(queries)

    def filter_row(self, row):
        if hxl.model.RowQuery.match_list(row, self.queries):
            self.row_count += 1
        return row.values
    

class RowFilter(AbstractStreamingFilter):
    """
    Composable filter class to select rows from a HXL dataset.

    Usage:

    <pre>
    # whitelist
    hxl.data(url).with_rows('org=OXFAM')

    # blacklist
    hxl.data(url).without_rows('org=OXFAM')
    </pre>
    """

    def __init__(self, source, queries=[], reverse=False, mask=[]):
        """
        Constructor
        @param source: the HXL data source
        @param queries: a series of predicates for rows to include or ignore
        @param reverse: True to reverse the sense of the select
        @param mask: a series of predicates to limit the rows to test (default: [] to test all)
        """
        super(RowFilter, self).__init__(source)
        self.queries = self._setup_queries(queries)
        self.mask = self._setup_queries(mask)
        self.reverse = reverse

        for query in self.queries:
            if query.needs_aggregate:
                if not self.source.is_cached:
                    self.source = self.source.cache()
                query.calc_aggregate(self.source)

    def filter_row(self, row):
        """Filter data row-wise.
        @param row: the row to filter
        @returns: the row's values as an array, or None if it fails the filters
        """
        if hxl.model.RowQuery.match_list(row, self.mask):
            if not hxl.model.RowQuery.match_list(row, self.queries, self.reverse):
                return None
        return row.values

    @staticmethod
    def _load(source, spec):
        """Construct a row filter from a dict spec."""
        
        reverse = False
        if spec.get('filter') == 'without_rows':
            reverse = True

        return RowFilter(
            source=source,
            queries=req_arg(spec, 'queries'),
            reverse=reverse,
            mask=opt_arg(spec, 'mask', [])
        )

    
class SortFilter(AbstractCachingFilter):
    """
    Composable filter class to sort a HXL dataset.

    This is the class supporting the hxlsort command-line utility.

    Usage:

    <pre>
    hxl.data(url).sort('sector,org,adm1')
    </pre>
    """

    def __init__(self, source, tags=[], reverse=False):
        """
        @param source: a HXL data source
        @param tags: list of TagPattern objects for sorting
        @param reverse: True to reverse the sort order
        """
        super(SortFilter, self).__init__(source)
        self.sort_tags = hxl.model.TagPattern.parse_list(tags)
        self.reverse = reverse
        self._iter = None

    def filter_rows(self):
        """Return a sorted list of values, row by row."""

        # Figure out the indices for sort keys
        indices = self._make_indices()

        def make_key(values):
            """Closure, to get the object reference into the key method."""
            return self._make_key(indices, values)

        return sorted(self.source.values, key=make_key, reverse=self.reverse)

    def _make_indices(self):
        """Determine the indices of the data to sort."""
        indices = []
        for pattern in self.sort_tags:
            index = pattern.find_column_index(self.columns)
            if index is not None:
                indices.append(index)
        return indices

    def _make_key(self, indices, values):
        """
        Make a sort key from a an array of values.
        @param indices: an array of indices for the sort key (if empty, use all values).
        @param values: an array of values to sort
        @returns: a sort key as a tuple
        """

        key = []

        if indices:
            for index in indices:
                key.append(SortFilter._make_sort_value(self.columns[index].tag, values[index]))
        else:
            # Sort everything, left to right
            for index, value in enumerate(values):
                key.append(SortFilter._make_sort_value(self.columns[index].tag, value))

        # convert the key to a tuple for sorting
        return tuple(key)

    @staticmethod
    def _make_sort_value(tag, value):
        """
        Make a special sort value

        The sort value is is a tuple of a numeric value (possibly inf)
        and the original string value. This will ensure that numeric
        values sort properly, and string values sort after them.
        """
        norm = hxl.datatypes.normalise_string(value)
        if tag == '#date':
            if hxl.datatypes.is_date(norm):
                return (float('inf'), hxl.datatypes.normalise_date(norm))
            else:
                return (float('inf'), norm)
        else:
            try:
                return (float(norm), norm)
            except:
                return (float('inf'), norm)

    @staticmethod
    def _load(source, spec):
        """Create a sort filter from a dict spec."""
        return SortFilter(
            source = source,
            tags=opt_arg(spec, 'keys', []),
            reverse=opt_arg(spec, 'reverse', False)
        )


#
# Compile a filter chain
#

LOAD_MAP = {
    'add_columns': AddColumnsFilter._load,
    'append': AppendFilter._load,
    'append_external_list': AppendFilter._load,
    'cache': CacheFilter._load,
    'clean_data': CleanDataFilter._load,
    'count': CountFilter._load,
    'dedup': DeduplicationFilter._load,
    'explode': ExplodeFilter._load,
    'fill_data': FillDataFilter._load,
    'jsonpath': JSONPathFilter._load,
    'merge_data': MergeDataFilter._load,
    'rename_columns': RenameFilter._load,
    'replace_data': ReplaceDataFilter._load,
    'replace_data_map': ReplaceDataFilter._load,
    'sort': SortFilter._load,
    'with_columns': ColumnFilter._load,
    'with_rows': RowFilter._load,
    'without_columns': ColumnFilter._load,
    'without_rows': RowFilter._load,
}
"""Static functions for creating filters from dicts (from JSON, typically)."""


def req_arg(spec, property):
    """Get a required property, and raise an exception if missing.
    @param spec: the JSON-like filter spec (a dict)
    @param property: the property name to retrieve
    @returns: the property value
    @exception HXLFilterException: if the property is not present
    """
    value = spec.get(property)
    if value is None:
        raise HXLFilterException("Missing required property: \"{}\"".format(property))
    return value


def opt_arg(spec, property, default_value=None):
    """Get an optional property, possibly with a default value.
    @param spec: the JSON-like filter spec (a dict)(
    @param property: the property name to retrieve
    @param default_value: the default value to return if not found
    @returns: the value if found, otherwise default_value/None
    """
    value = spec.get(property)
    if value is None:
        return default_value
    else:
        return value


def from_recipe(source, recipe):
    """Build a filter chain from a JSON-like list of filter specs.

    Each recipe dictionary contains the property 'filter', describing
    the filter type, and zero or more other properties providing
    parameters for the filter. Filter and parameter names are the same
    as the methods and arguments in hxl.model.Dataset.

    @param source: a HXL data source, URL, etc.
    @param recipe: a list of dictionaries, each describing a filter.
    @returns: the filter at the end of the new chain.
    """
    source = hxl.data(source)

    #
    # Clean up recipe if needed
    #
    if isinstance(recipe, six.string_types):
        # a JSON string (parse it first)
        recipe = json.loads(recipe)
    if isinstance(recipe, dict) and recipe.get('filter'):
        # a single filter (make it into a list)
        recipe = [recipe]

    # Process each filter in turn
    for spec in recipe:

        # Find the loader method
        type = req_arg(spec, 'filter')
        loader = LOAD_MAP.get(type)
        if not loader:
            raise HXLFilterException("Unknown filter type {}".format(type))

        # Create the filter
        source = loader(source, spec)
        
    return source


def list_product(lists, head=[]):
    """Generate the cartesian product of a list of lists 
    The elements of the result will be all possible combinations of the elements of
    the input lists.
    @param lists: a list of lists
    @return: the cross-product of the lists
    """
    if lists:
        result = []
        for item in lists[0]:
            tail = list_product(lists[1:], head + [item])
            result = result + tail
        return result
    else:
        return [head]


def is_sourcey(arg):
    """Convoluted method to try to distinguish a single HXL data source from a list of sources.
    Trying to recognise all the source types supported by hxl.io.make_input
    @param arg: the thing to test (we want to know if it's a single source or lists of sources)
    @returns: True if we think it's a single source; False otherwise.
    """

    # Not a list
    if ((not hasattr(arg, '__len__')) or
        isinstance(arg, dict) or
        isinstance(arg, six.string_types) or
        isinstance(arg, hxl.model.Dataset)):
        return True

    # Quick-and-dirty test for a list representation of a HXL dataset
    try:
        if (isinstance(arg[0], six.string_types)):
            return False
        elif ((not hasattr(arg[0][0], '__len__')) or isinstance(arg[0][0], six.string_types)):
            return True
    except:
        pass

    return False


# end

