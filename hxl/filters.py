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

import sys, re, six, abc, copy
import dateutil.parser

import hxl


#
# Filter-specific exception
#
class HXLFilterException(hxl.common.HXLException):
    """Base class for HXL filter exceptions.

    This subclass of L{hxl.common.HXLException} exists only to make it
    easier to distinguish filter-based exceptions in C{except:} clauses.

    """
    pass

#
# Base class for filters

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

        self.source = source
        """HXL data source for the filter"""

        self._filtered_column_cache = None

    @property
    def columns(self):
        """Return the filter's (possibly-modified) columns.

        By default, return the columns defined by the source HXL
        data. Child classes override the filter_columns() method to
        return something different.

        @return: a list of L{hxl.model.Column} objects

        """
        if self._filtered_column_cache is None:
            self._filtered_column_cache = self.filter_columns()
        return self._filtered_column_cache

    def filter_columns(self):
        """Return a new list of columns for the filtered dataset.

        By default, return the source HXL data's columns. Child
        classes override the filter_columns() method to return a
        different set of columns

        @return: a list of hxl.model.Column objects
        @see: L{AbstractStreamingFilter.filter_row}
        @see: L{AbstractCachingFilter.filter_rows}

        """
        return self.source.columns

    
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
    method to change, add, or suppress individual rows, like this::

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
        super(AbstractStreamingFilter, self).__init__(source)

    @abc.abstractmethod
    def filter_row(self, row):
        """Filter a single row of data.

        By default, this method returns the row's values,
        unchanged. Subclasses can use it to replace, add or suppress
        rows on the fly, without having to keep a copy of the entire
        dataset in memory.

        @param row: the original L{hxl.model.Row} object.  
        @return: A list of string values (I{not} a Row object) or
        C{None} to skip the row.
        @see: L{AbstractBaseFilter.filter_columns}
        """
        return row.values

    def __iter__(self):
        return AbstractStreamingFilter._Iterator(self)

    class _Iterator:
        """Internal iterator class to return the filtered rows."""

        def __init__(self, outer):
            """Create an iterator for a streaming filter
            @param outer: a reference to the parent object (an L{AbstractStreamingFilter}).
            """
            self.outer = outer
            self.source_iter = iter(self.outer.source)
            self.row_number = -1

        def __iter__(self):
            return self

        def __next__(self):
            """Return the next filtered row of data.  

            Uses the L{AbstractStreamingFilter.filter_row} method. The
            returned row is always a new object, so that if the client
            changes it, it won't change the version visible upstream
            in the filter chain.

            @return: a L{hxl.model.Row} object

            """
            # call this here, in case it caches any useful information
            columns = self.outer.columns
            for row in self.source_iter:
                # get a new list of filtered values
                values = self.outer.filter_row(row)
                if values is not None:
                    # keep looping if filter_row(row) returned None
                    self.row_number += 1
                    # create a new Row object
                    return hxl.model.Row(columns, values, self.row_number)
            # if we've finished the iteration, then we're out of rows, so stop
            raise StopIteration()

        next = __next__


class AbstractCachingFilter(AbstractBaseFilter):
    """Abstract base class for caching filters.

    A caching filter processes the entire dataset together in
    memory. This approach is much less efficient than a L{streaming
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
        super(AbstractCachingFilter, self).__init__(source)
        # save the rows here, for multiple iterations
        self._saved_rows = None

    def filter_rows(self):
        """Filter all of the data together.
        
        This method returns raw lists and strings, not objects from
        the L{hxl.model} module; for example, it could return the
        following for a three-row dataset::

          [['UNICEF', '20'], ['UNHCR', '15'], ['OXFAM', '25']]

        The I{AbstractCachingFilter} class will construct the
        appropriate objects around the data.
        
        @return: a list of lists of strings, one list for each row.
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

            @return: a L{hxl.model.Row} object
            """

            if self.values_iter is None:
                if self.outer._saved_rows is None:
                    # filter rows only once, when requested
                    self.outer._saved_rows = self.outer.filter_rows()
                self.values_iter = iter(self.outer._saved_rows)
            self.row_number += 1
            return hxl.model.Row(self.outer.columns, next(self.values_iter), self.row_number)
        
        next = __next__


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
        @param specs: a string or list of strings containing new column specifications, as described above (or a list of tuples as described in L{parse_spec})
        @param before: true to add new columns before existing ones (default C{False})

        """
        super(AddColumnsFilter, self).__init__(source)
        if isinstance(specs, six.string_types):
            specs = [specs]
        self.specs = [AddColumnsFilter.parse_spec(spec) for spec in specs]
        self.before = before
        self.const_values = None

    def filter_columns(self):
        """Internal: return the new columns list"""
        new_columns = [spec[0] for spec in self.specs]
        if self.before:
            new_columns = new_columns + self.source.columns
        else:
            new_columns = self.source.columns + new_columns
        self.const_values = [spec[1] for spec in self.specs]
        return new_columns

    def filter_row(self, row):
        """Internal: return each row with the new fixed value(s) attached."""
        values = copy.copy(row.values)
        if self.before:
            return self.const_values + values
        else:
            return values + self.const_values

    _SPEC_PATTERN = r'^\s*(?:([^#]*)#)?({token}(?:\s*\+{token})*)=(.*)\s*$'.format(token=hxl.common.TOKEN_PATTERN)

    @staticmethod
    def parse_spec(spec):
        """Parse a new-column specification.

        The format is I{<header text>}B{#}I{<tag and
        attributes>}B{=}I{<fixed value>}. Example::

          Country name#country=Malaysia

        @param spec: the string spec to parse, in the format above.
        @return: a tuple containing a L{hxl.model.Column} object and the fixed value.
        """

        if not isinstance(spec, six.string_types):
            return spec
        result = re.match(AddColumnsFilter._SPEC_PATTERN, spec)
        if result:
            header = result.group(1)
            tag = '#' + result.group(2)
            value = result.group(3)
            return (hxl.model.Column.parse(tag, header=header), value)
        else:
            raise HXLFilterException("Badly formatted new-column spec: " + spec)


class AppendFilter(AbstractBaseFilter):

    """Composable filter class to concatenate two datasets.

    Usage::
    
        filter = Appendfilter(hxl.data(url), hxl.data(url2))

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

    @see: L{MergeFilter}, which combines two datasets horizontally
    rather than vertically.

    """

    def __init__(self, source, append_source, add_columns=True, queries=[]):
        """Construct a new I{AppendFilter}
        @param source: a L{hxl.model.Dataset} object for the principal data
        @param append_source: a L{hxl.model.Dataset} object for the dataset to append (or a string containing a URL)
        @param add_columns: flag for adding extra columns in append_source but not source (default True)
        @param queries: optional list of L{hxl.model.RowQuery} objects
        (or a single strig) to select which rows to include from the
        second dataset

        """
        super(AppendFilter, self).__init__(source)
        # parameters
        self.append_source = hxl.data(append_source) # so that we can take a plain URL
        self.add_columns = add_columns
        self.queries = hxl.model.RowQuery.parse_list(queries)
        # internal properties
        self._column_positions = None
        self._template_row = None

    def filter_columns(self):
        """Internal: generate the columns for the combined dataset

        If add_columns is True, extend with any columns from the
        second dataset that don't appear in the first; otherwise,
        just return a copy of the columns from the source
        dataset.

        As a side-effect, create an empty template for each
        row of values, and create a position map from column
        numbers in the second (appended) dataset to column numbers
        in the output, for faster mapping.
        """
        
        columns_out = copy.deepcopy(self.source.columns)
        column_positions = {}
        original_tags = self.source.display_tags

        # see if there's a corresponding column in the source
        for i, column in enumerate(self.append_source.columns):
            for j, tag in enumerate(original_tags):
                if tag and (column.display_tag == tag):
                    # yes, there is one; clear it, so it's not reused
                    original_tags[j] = None
                    column_positions[i] = j
                    break
            else:
                # no -- we need to add a new column
                if self.add_columns:
                    column_positions[i] = len(columns_out)
                    columns_out.append(copy.deepcopy(column))
                else:
                    column_positions[i] = None

        # save the position map
        self._column_positions = column_positions

        # make an empty template for each row
        self._template_row = [''] * len(columns_out)

        # return the (usually cached) columns
        return columns_out

    def __iter__(self):
        self.columns # make sure this is triggered first
        return AppendFilter._Iterator(self)

    class _Iterator:
        """Custom iterator to return the contents of both sources, in sequence."""

        def __init__(self, outer):
            self.outer = outer
            self.source_iter = iter(outer.source)
            self.append_iter = iter(outer.append_source)

        def __iter__(self):
            return self

        def __next__(self):

            # Make a new row with the new columns
            row_out = hxl.model.Row(
                columns=self.outer.columns,
                values=copy.deepcopy(self.outer._template_row)
            )

            # Read from the original source first
            if self.source_iter is not None:
                try:
                    row_in = next(self.source_iter)
                    for i, value in enumerate(row_in):
                        row_out.values[i] = row_in.values[i]
                    return row_out
                except StopIteration:
                    # don't let the end of the first source finish the iteration
                    self.source_iter = None

            # Fall through to the append source

            # filtering through queries
            row_in = next(self.append_iter)
            while not hxl.model.RowQuery.match_list(row_in, self.outer.queries):
                row_in = next(self.append_iter)
            
            for i, value in enumerate(row_in):
                pos = self.outer._column_positions[i]
                if pos is not None:
                    row_out.values[pos] = value
            return row_out

        next = __next__


class CacheFilter(AbstractCachingFilter):
    """
    Composable filter class to cache HXL data in memory.

    Caching the data allows you to iterate over it more than once.

    This filter does nothing *but* cache the data, for cases where you
    plan to process it more than once.  You have the option to cache
    only part of the dataset (e.g. for a preview), in which case,
    the "overflow" property will be True if there were more columns.
    """

    def __init__(self, source, max_rows=None):
        """
        Constructor
        @param max_rows If >0, maximum number of rows to cache.
        """
        super(CacheFilter, self).__init__(source)
        self.max_rows = max_rows
        self.overflow = False

    def filter_columns(self):
        # local copy of the columns
        return copy.deepcopy(self.source.columns)

    def filter_rows(self):
        # may be limiting the number of rows read
        values = []
        max_rows = self.max_rows
        for row in self.source:
            if max_rows is not None:
                max_rows -= 1
                if max_rows < 0:
                    self.overflow = True
                    break
            values.append(row.values)
        return values


class CleanDataFilter(AbstractStreamingFilter):
    """
    Filter for cleaning values in HXL data.
    Can normalise whitespace, convert to upper/lowercase, and fix dates and numbers.
    TODO: clean up lat/lon coordinates
    """

    def __init__(self, source, whitespace=False, upper=[], lower=[], date=[], number=[], queries=[]):
        """
        Construct a new data-cleaning filter.
        @param source: the HXLDataSource
        @param whitespace: list of TagPatterns for normalising whitespace.
        @param upper: list of TagPatterns for converting to uppercase.
        @param lower: list of TagPatterns for converting to lowercase.
        @param date: list of TagPatterns for normalising dates.
        @param number: list of TagPatterns for normalising numbers.
        @param queries: optional list of queries to select rows to be cleaned.
        """
        super(CleanDataFilter, self).__init__(source)
        self.whitespace = hxl.model.TagPattern.parse_list(whitespace)
        self.upper = hxl.model.TagPattern.parse_list(upper)
        self.lower = hxl.model.TagPattern.parse_list(lower)
        self.date = hxl.model.TagPattern.parse_list(date)
        self.number = hxl.model.TagPattern.parse_list(number)
        self.queries = hxl.model.RowQuery.parse_list(queries)

    def filter_row(self, row):
        """Clean up values and pass on the row data."""
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

    def _clean_value(self, value, column):
        """Clean a single HXL value."""

        value = str(value)

        # Whitespace (-w)
        if self._match_patterns(self.whitespace, column):
            value = re.sub('^\s+', '', value)
            value = re.sub('\s+$', '', value)
            value = re.sub('\s+', ' ', value)

        # Uppercase (-u)
        if self._match_patterns(self.upper, column):
            if sys.version_info[0] > 2:
                value = value.upper()
            else:
                value = value.decode('utf8').upper().encode('utf8')

        # Lowercase (-l)
        if self._match_patterns(self.lower, column):
            if sys.version_info[0] > 2:
                value = value.lower()
            else:
                value = value.decode('utf8').lower().encode('utf8')

        # Date
        if self._match_patterns(self.date, column):
            if value:
                value = dateutil.parser.parse(value).strftime('%Y-%m-%d')

        # Number
        if self._match_patterns(self.number, column) and re.search('\d', value):
            # fixme - get much smarter about numbers
            if value:
                value = re.sub('[^\de.]+', '', value)
                value = re.sub('^0+', '', value)
                value = re.sub('(\..*)0+$', '\g<1>', value)
                value = re.sub('\.$', '', value)
        return value

    def _match_patterns(self, patterns, column):
        """Test if a column matches a list of patterns."""
        if not patterns:
            return False
        else:
            for pattern in patterns:
                if pattern.match(column):
                    return True
            return False


class ColumnFilter(AbstractStreamingFilter):
    """
    Composable filter class to filter columns in a HXL dataset.

    Usage:

    <pre>
    # blacklist columns
    hxl.data(url).without_columns('contact+email')

    # whitelist columns
    hxl.data(url).with_columns(['org', 'sector', 'adm1'])
    </pre>
    """

    def __init__(self, source, include_tags=[], exclude_tags=[]):
        """
        @param source a HXL data source
        @param include_tags a whitelist of TagPattern objects to include
        @param exclude_tags a blacklist of TagPattern objects to exclude
        """
        super(ColumnFilter, self).__init__(source)
        self.include_tags = hxl.model.TagPattern.parse_list(include_tags)
        self.exclude_tags = hxl.model.TagPattern.parse_list(exclude_tags)
        self.indices = [] # saved indices for columns to include

    def filter_columns(self):
        """
        Remove any columns in the blacklist or not in the whitelist.
        Save the indices in self.cached_indices for row filtering.
        """
        columns_in = self.source.columns
        columns_out = []
        for i in range(len(columns_in)):
            if self._test_column(columns_in[i]):
                columns_out.append(copy.deepcopy(columns_in[i]))
                self.indices.append(i) # save index to avoid retesting for data
        return columns_out

    def filter_row(self, row):
        """Remove values from a row for any column that's been removed."""
        values = []
        for i in self.indices:
            try:
                values.append(row.values[i])
            except IndexError:
                pass # don't add anything
        return values

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


class CountFilter(AbstractCachingFilter):
    """
    Composable filter class to aggregate rows in a HXL dataset.

    This is the class supporting the hxlcount command-line utility.

    WARNING: this filter reads the entire source dataset before
    producing output, and may need to hold a large amount of data in
    memory, depending on the number of unique combinations counted.

    Usage:

    <pre>
    filter = source.count(['#org', '#sector'])
    </pre>
    """

    def __init__(self, source, patterns, aggregate_pattern=None, count_spec='Count#meta+count', queries=[]):
        """
        Constructor
        @param source the HXL data source
        @param patterns a list of strings or TagPattern objects that form a unique key together
        @param aggregate_pattern an optional tag pattern calculating numeric aggregate values.
        @param count_spec the tag spec for the count column (defaults to 'Count#meta+count').
        @param filters an optional list of query filters for rows to be counted.
        """
        super(CountFilter, self).__init__(source)
        self.patterns = hxl.model.TagPattern.parse_list(patterns)
        self.aggregate_pattern = hxl.model.TagPattern.parse(aggregate_pattern) if aggregate_pattern else None
        self.count_column = CountFilter.parse_spec(count_spec)
        self.queries = hxl.model.RowQuery.parse_list(queries)

    def filter_columns(self):
        """Generate the columns for the report."""
        columns = []

        # Add columns being counted
        for pattern in self.patterns:
            column = pattern.find_column(self.source.columns)
            if column:
                columns.append(copy.deepcopy(column))
            else:
                columns.append(hxl.Column())

        # Add column to hold count
        columns.append(self.count_column)

        # If we're aggregating, add the aggregate columns
        if self.aggregate_pattern is not None:
            columns.append(hxl.model.Column.parse('#meta+sum', header='Sum'))
            columns.append(hxl.model.Column.parse('#meta+average', header='Average (mean)'))
            columns.append(hxl.model.Column.parse('#meta+min', header='Minimum value'))
            columns.append(hxl.model.Column.parse('#meta+max', header='Maximum value'))
            
        return columns


    def __iter__(self):
        return CountFilter.Iterator(self)

    SPEC_PATTERN = r'^\s*(?:([^#]*)#)?({token}(?:\s*\+{token})*)\s*$'.format(token=hxl.common.TOKEN_PATTERN)

    @staticmethod
    def parse_spec(spec):
        if not isinstance(spec, six.string_types):
            return spec
        result = re.match(CountFilter.SPEC_PATTERN, spec)
        if result:
            header = result.group(1)
            tag = '#' + result.group(2)
            return hxl.model.Column.parse(tag, header=header)
        else:
            raise HXLFilterException("Badly formatted column spec: " + spec)


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

            row = hxl.model.Row(self.outer.columns)
            row.values = values
            return row

        next = __next__

        def _aggregate(self):
            """
            Read the entire source dataset and produce saved aggregate data.
            """
            aggregators = {}
            for row in self.iterator:
                if hxl.model.RowQuery.match_list(row, self.outer.queries):
                    values = [str(row.get(pattern, default='')) for pattern in self.outer.patterns]
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
                value = row.get(self.pattern, default='')
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


class DeduplicationFilter(AbstractStreamingFilter):
    """
    Composable filter to deduplicate a HXL dataset.

    Supports the hxldedup command-line script.

    TODO: add more-sophisticated matching, edit distance, etc.
    """

    def __init__(self, source, patterns=None, queries=[]):
        """
        Constructor
        @param source the upstream source dataset
        @param patterns if provided, a list of tag patterns for columns to use for uniqueness testing.
        @param filters optional list of filter queries for columns to be considered for deduplication.
        """
        super(DeduplicationFilter, self).__init__(source)
        self.patterns = hxl.model.TagPattern.parse_list(patterns)
        self.seen_map = set() # row signatures that we've seen so far
        self.queries = hxl.model.RowQuery.parse_list(queries)

    def filter_row(self, row):
        """Filter out any rows we've seen before."""
        if hxl.model.RowQuery.match_list(row, self.queries):
            if not row:
                return None
            key = self._make_key(row)
            if key in self.seen_map:
                return None
            # if we get to here, we haven't seen the row before
            self.seen_map.add(key)
            return copy.copy(row.values)
        else:
            return row.values

    def _is_key(self, col):
        """Check if a column is part of the key for deduplication."""
        if self.patterns:
            for pattern in self.patterns:
                if pattern.match(col):
                    return True
            return False
        else:
            return True

    def _make_key(self, row):
        """Create a tuple key for a row."""
        key = []
        for i, value in enumerate(row.values):
            if self._is_key(row.columns[i]):
                key.append(hxl.common.normalise_string(value))
        return tuple(key)

        
class MergeDataFilter(AbstractStreamingFilter):
    """
    Composable filter class to merge values from two HXL datasets.

    This is the class supporting the hxlmerge command-line utility.

    Warning: this filter may store a large amount of data in memory, depending on the merge.

    Usage:

    <pre>
    hxl.data(url).merge(merge_source=merge_source, keys='adm1_id', tags='adm1')
    </pre>
    """

    def __init__(self, source, merge_source, keys, tags, replace=False, overwrite=False, queries=[]):
        """
        Constructor.
        @param source the HXL data source.
        @param merge_source a second HXL data source to merge into the first.
        @param keys the shared key hashtags to use for the merge
        @param tags the tags to include from the second dataset
        @param filters optional list of filter queries for rows to be considered from the merge dataset.
        """
        super(MergeDataFilter, self).__init__(source)
        self.merge_source = merge_source
        self.keys = hxl.model.TagPattern.parse_list(keys)
        self.merge_tags = hxl.model.TagPattern.parse_list(tags)
        self.replace = replace
        self.overwrite = overwrite
        self.queries = hxl.model.RowQuery.parse_list(queries)

        self.merge_map = None

    def filter_columns(self):
        """Filter the columns to add newly-merged ones."""
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
                new_columns.append(hxl.model.Column(tag=pattern.tag, attributes=pattern.include_attributes, header=header))
        return self.source.columns + new_columns

    def filter_row(self, row):
        """Set up a merged data row, replacing existing values if requested."""
        
        # First, check if we already have the merge map, and read it if not
        if self.merge_map is None:
            self.merge_map = self._read_merge()

        # Make a copy of the values
        values = copy.copy(row.values)

        # Look up the merge values, based on the --keys
        merge_values = self.merge_map.get(self._make_key(row), {})

        # Go through the merge tags
        for pattern in self.merge_tags:
            value = merge_values.get(pattern)
            # force always to empty string (not None)
            if not value:
                value = ''
            # Try to substitute in place?
            if self.replace:
                index = pattern.find_column_index(self.source.columns)
                if index is not None:
                    if self.overwrite or not row.values[index]:
                        values[index] = value
                    continue

            # otherwise, fall through
            values.append(value)
        return values

    def _make_key(self, row):
        """
        Make a tuple key for a row.
        """
        values = []
        for pattern in self.keys:
            values.append(hxl.common.normalise_string(row.get(pattern, default='')))
        return tuple(values)

    def _read_merge(self):
        """
        Read the second (merging) dataset into memory.
        Stores only the values necessary for the merge.
        @return a map of merge values
        """
        merge_map = {}
        for row in self.merge_source:
            if hxl.model.RowQuery.match_list(row, self.queries):
                values = {}
                for pattern in self.merge_tags:
                    values[pattern] = row.get(pattern, default='')
                merge_map[self._make_key(row)] = values
        return merge_map


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
        @param source the Dataset for the data.
        @param rename_map map of tags to rename
        """
        super(RenameFilter, self).__init__(source)
        if isinstance(rename, six.string_types):
            rename = [rename]
        self.rename = [RenameFilter.parse_rename(spec) for spec in rename]

    def filter_columns(self):
        """Rename requested columns."""
        return [self._rename_column(column) for column in self.source.columns]

    def filter_row(self, row):
        """Row will be rebuilt with proper columns."""
        return copy.copy(row.values)

    def _rename_column(self, column):
        """Rename a column if requested."""
        for spec in self.rename:
            if spec[0].match(column):
                new_column = copy.deepcopy(spec[1])
                if new_column.header is None:
                    new_column.header = column.header
                return new_column
        return copy.deepcopy(column)
    RENAME_PATTERN = r'^\s*#?({token}(?:\s*[+-]{token})*):(?:([^#]*)#)?({token}(?:\s*[+]{token})*)\s*$'.format(token=hxl.common.TOKEN_PATTERN)

    @staticmethod
    def parse_rename(s):
        """Parse a rename specification from the parameters."""
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
        @param source the HXL data source
        @param original a string or regular expression to replace (string must match the whole value, not just part)
        @param replacements list of replacement objects
        @param filters optional list of filter queries for rows where replacements should be applied.
        """
        super(ReplaceDataFilter, self).__init__(source)
        self.replacements = replacements
        if isinstance(self.replacements, ReplaceDataFilter.Replacement):
            self.replacements = [self.replacements]
        self.queries = hxl.model.RowQuery.parse_list(queries)

    def filter_row(self, row):
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

        def __init__(self, original, replacement, pattern=None, is_regex=False):
            """
            @param original a string (case- and space-insensitive) or regular expression (sensitive) to replace
            @param replacement the replacement string or regular expression substitution
            @param pattern (optional) a tag pattern to limit the replacement to specific columns
            @param is_regex (optional) True to use regular-expression processing (defaults to False)
            """
            self.original = original
            self.replacement = replacement
            if pattern:
                self.pattern = hxl.model.TagPattern.parse(pattern)
            else:
                self.pattern = None
            self.is_regex = is_regex
            if not self.is_regex:
                self.original = hxl.common.normalise_string(self.original)

        def sub(self, column, value):
            """
            Substitute inside the value, if appropriate.
            @param column the column definition
            @param value the cell value
            @return the value, possibly changed
            """
            if self.pattern and not self.pattern.match(column):
                return value
            elif self.is_regex:
                return re.sub(self.original, self.replacement, value)
            elif self.original == hxl.common.normalise_string(value):
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

        
class RowCountFilter(AbstractStreamingFilter):
    """
    Composable filter class to count lines.

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
        self.queries = hxl.model.RowQuery.parse_list(queries)

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

    def __init__(self, source, queries=[], reverse=False):
        """
        Constructor
        @param source the HXL data source
        @param queries a series for parsed queries
        @param reverse True to reverse the sense of the select
        """
        super(RowFilter, self).__init__(source)
        self.queries = hxl.model.RowQuery.parse_list(queries)
        self.reverse = reverse

    def filter_row(self, row):
        if self.match_row(row):
            return row.values
        else:
            return None

    def match_row(self, row):
        """Check if any of the queries matches the row (implied OR)."""
        for query in self.queries:
            if query.match_row(row):
                return not self.reverse
        return self.reverse

    
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
        @param source a HXL data source
        @param tags list of TagPattern objects for sorting
        @param reverse True to reverse the sort order
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
        @param indices - an array of indices for the sort key (if empty, use all values).
        @param values - an array of values to sort
        @return a sort key as a tuple
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
        norm = hxl.common.normalise_string(value)
        if tag == '#date':
            try:
                return (float('inf'), dateutil.parser.parse(norm).strftime('%Y-%m-%d'))
            except:
                return (float('inf'), norm)
        else:
            try:
                return (float(norm), norm)
            except:
                return (float('inf'), norm)


# end

