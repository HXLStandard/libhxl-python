"""
Data model for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import abc, copy, csv, dateutil, json, logging, operator, re, six

import hxl

logger = logging.getLogger(__name__)


class TagPattern(object):
    """Pattern for matching a HXL hashtag and attributes
    #tag matches #tag with any attributes
    #tag+foo matches #tag with foo among its attributes
    #tag-foo matches #tag with foo *not* among its attributes
    #tag+foo-bar matches #tag with foo but not bar
    """

    # Regular expression to match a HXL tag pattern (including '-' to exclude attributes)
    PATTERN = r'^\s*#?({token}|\*)((?:\s*[+-]{token})*)\s*(!)?\s*$'.format(token=hxl.datatypes.TOKEN_PATTERN)

    def __init__(self, tag, include_attributes=[], exclude_attributes=[], is_absolute=False):
        """Like a column, but has a whitelist and a blacklist.
        @param tag: the basic hashtag (without attributes)
        @param include_attributes: a list of attributes that must be present
        @param exclude_attributes: a list of attributes that must not be present
        """
        self.tag = tag
        """HXL hashtag, or "#*" for a wildcard"""

        self.include_attributes = set(include_attributes)
        """Set of all attributes that must be present"""
        
        self.exclude_attributes = set(exclude_attributes)
        """Set of all attributes that must not be present"""
        
        self.is_absolute = is_absolute
        """True if this pattern is absolute (no extra attributes allowed)"""

    def is_wildcard(self):
        return self.tag == '#*'

    def match(self, column):
        """Check whether a Column matches this pattern.
        @param column: the column to check
        @returns: True if the column is a match
        """
        if column.tag and (self.is_wildcard() or self.tag == column.tag):
            # all include_attributes must be present
            if self.include_attributes:
                for attribute in self.include_attributes:
                    if attribute not in column.attributes:
                        return False
            # all exclude_attributes must be absent
            if self.exclude_attributes:
                for attribute in self.exclude_attributes:
                    if attribute in column.attributes:
                        return False
            # if absolute, then only specified attributes may be present
            if self.is_absolute:
                for attribute in column.attributes:
                    if attribute not in self.include_attributes:
                        return False
            return True
        else:
            return False

    def get_matching_columns(self, columns):
        """Return a list of columns that match the pattern.
        @param columns: a list of L{hxl.model.Column} objects
        @returns: a list (possibly empty)
        """
        result = []
        for column in columns:
            if self.match(column):
                result.append(column)
        return result

    def find_column_index(self, columns):
        """Get the index of the first matching column.
        @param columns: a list of columns to check
        @returns: the 0-based index of the first matching column, or None for no match
        """
        for i in range(len(columns)):
            if self.match(columns[i]):
                return i
        return None

    def find_column(self, columns):
        """Check whether there is a match in a list of columns."""
        for column in columns:
            if self.match(column):
                return column
        return None

    def __repr__(self):
        s = self.tag
        if self.include_attributes:
            for attribute in self.include_attributes:
                s += '+' + attribute
        if self.exclude_attributes:
            for attribute in self.exclude_attributes:
                s += '-' + attribute
        return s

    __str__ = __repr__

    @staticmethod
    def parse(s):
        """Parse a single tag pattern, like #tag+foo-bar."""

        if not s:
            # edge case: null value
            raise hxl.HXLException('Attempt to parse empty tag pattern')
        elif isinstance(s, TagPattern):
            # edge case: already parsed
            return s

        result = re.match(TagPattern.PATTERN, s)
        if result:
            tag = '#' + result.group(1).lower()
            include_attributes = set()
            exclude_attributes = set()
            attribute_specs = re.split(r'\s*([+-])', result.group(2))
            for i in range(1, len(attribute_specs), 2):
                if attribute_specs[i] == '+':
                    include_attributes.add(attribute_specs[i + 1].lower())
                else:
                    exclude_attributes.add(attribute_specs[i + 1].lower())
            if result.group(3) == '!':
                is_absolute = True
                if exclude_attributes:
                    raise ValueError('Exclusions not allowed in absolute patterns')
            else:
                is_absolute = False
            return TagPattern(
                tag,
                include_attributes=include_attributes,
                exclude_attributes=exclude_attributes,
                is_absolute=is_absolute
            )
        else:
            raise hxl.HXLException('Malformed tag: ' + s)

    @staticmethod
    def parse_list(specs):
        """
        Normalise a list of tag specs.
        Split if a comma-separated string.
        Convert every element to a TagPattern
        @param specs the raw input
        @return normalised list of tag patterns
        """
        if not specs:
            return []
        if isinstance(specs, six.string_types):
            specs = specs.split(',')
        return [TagPattern.parse(spec) for spec in specs]

    @staticmethod
    def match_list(column, patterns):
        """Test if a column matches any of the patterns in a list.
        @param column: the column to test
        @param patterns: a list of zero or more patterns.
        @returns: True if there is a match
        """
        for pattern in patterns:
            if pattern.match(column):
                return True
        return False


class Dataset(object):
    """Abstract base class for a HXL data source.

    Any source of parsed HXL data inherits from this class: that
    includes Dataset, HXLReader, and the various filters in the
    hxl.old_filters package.  The contract of a Dataset is that it will
    provide a columns property and a next() method to read through the
    rows.

    The child class must implement the columns() method as a property
    and the __iter__() method to make itself iterable.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        """Constructor."""
        super().__init__()

    @abc.abstractmethod
    def __iter__(self):
        """Get the iterator over the rows.
        @returns: an iterator that returns L{hxl.model.Row} objects
        """
        raise RuntimeException("child class must implement __iter__() method")

    @property
    def is_cached(self):
        """Test whether the source data is cached (replayable).
        By default, this is False, but some subclasses may override.
        @returns: C{True} if the input is cached (replayable); C{False} otherwise.
        """
        return False

    @property
    @abc.abstractmethod
    def columns(self):
        """Get the column definitions for the dataset.
        @returns: a list of Column objects.
        """
        raise RuntimeException("child class must implement columns property method")

    @property
    def headers(self):
        """Return a list of header strings (for a spreadsheet row).
        """
        return [column.header if column else '' for column in self.columns]

    @property
    def tags(self):
        """Get all hashtags (without attributes) as a list
        @returns: a list of base hashtags for the dataset columns
        """
        return [column.tag if column else '' for column in self.columns]

    @property
    def display_tags(self):
        """Return a list of display tags.
        @returns: a list of strings containing the hashtag and attributes for each column
        """
        return [column.display_tag if column else '' for column in self.columns]

    @property
    def has_headers(self):
        """Report whether any non-empty header strings exist.
        @returns: C{True} if there is at least one column with a non-empty header string
        """
        for column in self.columns:
            if column.header:
                return True
        return False

    @property
    def values(self):
        """Get all values for the dataset at once, in an array of arrays.
        This method can be highly inefficient for large datasets.
        @returns: an array of arrays of scalar values
        """
        return [row.values for row in self]

    def get_value_set(self, tag_pattern=None, normalise=False):
        """Return the set of all values in a dataset (optionally matching a tag pattern for a single column)
        Warning: this method can be highly inefficient for large datasets.
        @param tag_pattern: (optional) return values only for columns matching this tag pattern.
        @param normalise: (optional) normalise the strings with hxl.datatypes.normalise (default: False)
        @returns: a Python set of values
        """
        value_set = set([])
        if tag_pattern:
            tag_pattern = TagPattern.parse(tag_pattern)
        for row in self:
            if tag_pattern:
                new_values = row.get_all(tag_pattern)
            else:
                new_values = row.values
            if normalise:
                new_values = [hxl.datatypes.normalise(s) for s in new_values]
            else:
                new_values = [hxl.datatypes.normalise_space(s) for s in new_values]
            value_set.update(new_values)
        return value_set

    #
    # Aggregates
    #

    def _get_minmax(self, pattern, op):
        """Calculate the extreme min/max value for a tag pattern
        Will iterate through the dataset, and use values from multiple matching columns.
        Uses numbers, dates, or strings for comparison, based on the first non-empty value found.
        @param pattern: the L{hxl.model.TagPattern} to match
        @param op: operator_lt or operator_gt
        @returns: the minimum value according to the '<' operator, or None if no values found
        """
        pattern = TagPattern.parse(pattern)
        target_value = None
        type = None
        for row in self:
            for value in row.get_all(pattern):
                value = hxl.datatypes.normalise_string(value)
                if hxl.datatypes.is_empty(value):
                    continue # don't care about empty cells
                if pattern.tag == '#date' or type == 'date':
                    if re.match(r'^\d\d\d\d$', value):
                        # special case for year
                        type = 'date'
                    else:
                        if hxl.datatypes.is_date(value):
                            value = hxl.datatypes.normalise_date(value)
                            type = 'date'
                if type is None or type == 'number':
                    try:
                        value = float(value)
                        type = 'number'
                    except ValueError:
                        if type == 'number': # not a number
                            value = None
                if type is None:
                    type = 'string'
                    value = str(value)
                if value is not None:
                    if target_value is None or op(value, target_value):
                        target_value = value
        return target_value
        

    def min(self, pattern):
        """Calculate the minimum value for a tag pattern
        Will iterate through the dataset, and use values from multiple matching columns.
        Uses numbers, dates, or strings for comparison, based on the first non-empty value found.
        @param pattern: the L{hxl.model.TagPattern} to match
        @returns: the minimum value according to the '<' operator, or None if no values found
        """
        return self._get_minmax(pattern, operator.lt)

    def max(self, pattern):
        """Calculate the maximum value for a tag pattern
        Will iterate through the dataset, and use values from multiple matching columns.
        @param pattern: the L{hxl.model.TagPattern} to match
        @returns: the minimum value according to the '<' operator, or None if no values found
        """
        return self._get_minmax(pattern, operator.gt)

    #
    # Utility
    #

    def validate(self, schema=None, callback=None):
        """
        Validate the current dataset.
        @param schema (optional) the pre-compiled schema, schema filename, URL, file object, etc. Defaults to a built-in schema.
        @param callback (optional) a function to call with each error or warning. Defaults to collecting errors in an array and returning them.
        """
        return hxl.schema(schema, callback).validate(self)

    def recipe(self, recipe):
        """Parse a recipe (JSON or a list of dicts) and create the appropriate filters.
        @param recipe: a list of dicts, a single dict, or a JSON literal string.
        @return: the new end filter.
        """
        import hxl.filters
        return hxl.filters.from_recipe(self, recipe)

    #
    # Filters
    #

    def append(self, append_sources, add_columns=True, queries=[]):
        """Append additional datasets.
        @param append_sources: a list of sources to append
        @param add_columns: if True (default), include any extra columns in the append sources
        @param queries: a list of row queries to select rows for inclusion from the append sources.
        @returns: a new HXL source for chaining
        """
        import hxl.filters
        return hxl.filters.AppendFilter(self, append_sources, add_columns=add_columns, queries=queries)

    def append_external_list(self, source_list_url, add_columns=True, queries=[]):
        """Append additional datasets from an external list
        @param source_list_url: URL of a HXL dataset containing a list of sources to append.
        @param add_columns: if True (default), include any extra columns in the append sources.
        @param queries: a list of row queries to select rows for inclusion from the append sources.
        @returns: a new HXL source for chaining
        """
        import hxl.filters
        append_sources = hxl.filters.AppendFilter.parse_external_source_list(source_list_url)
        return hxl.filters.AppendFilter(self, append_sources, add_columns=add_columns, queries=queries)

    def cache(self):
        """Add a caching filter to the dataset."""
        import hxl.filters
        return hxl.filters.CacheFilter(self)

    def dedup(self, patterns=[], queries=[]):
        """Deduplicate a dataset."""
        import hxl.filters
        return hxl.filters.DeduplicationFilter(self, patterns=patterns, queries=queries)

    def with_columns(self, whitelist):
        """Select matching columns."""
        import hxl.filters
        return hxl.filters.ColumnFilter(self, include_tags=whitelist)

    def without_columns(self, blacklist=None, skip_untagged=False):
        """Select non-matching columns."""
        import hxl.filters
        return hxl.filters.ColumnFilter(self, exclude_tags=blacklist, skip_untagged=skip_untagged)

    def with_rows(self, queries, mask=[]):
        """Select matching rows.
        @param queries: a predicate or list of predicates for rows to include
        @param mask: a predicate or list of predicates for rows to test (default: [] to test all)
        @return: a filtered version of the source
        """
        import hxl.filters
        return hxl.filters.RowFilter(self, queries=queries, reverse=False, mask=mask)

    def without_rows(self, queries, mask=[]):
        """Select non-matching rows.
        @param queries: a predicate or list of predicates for rows to ignore
        @param mask: a predicate or list of predicates for rows to test (default: [] to test all)
        @return: a filtered version of the source
        """
        import hxl.filters
        return hxl.filters.RowFilter(self, queries=queries, reverse=True, mask=mask)

    def sort(self, keys=None, reverse=False):
        """Sort the dataset (caching)."""
        import hxl.filters
        return hxl.filters.SortFilter(self, tags=keys, reverse=reverse)

    def count(self, patterns=[], aggregators=None, queries=[]):
        """Count values in the dataset (caching)."""
        import hxl.filters
        return hxl.filters.CountFilter(
            self, patterns=patterns, aggregators=aggregators, queries=queries
        )

    def row_counter(self, queries=[]):
        """Count the number of rows while streaming."""
        import hxl.filters
        return hxl.filters.RowCountFilter(self, queries=queries)

    def replace_data(self, original, replacement, pattern=None, use_regex=False, queries=[]):
        """Replace values in a HXL dataset."""
        import hxl.filters
        replacement = hxl.filters.ReplaceDataFilter.Replacement(original, replacement, pattern, use_regex)
        return hxl.filters.ReplaceDataFilter(self, [replacement], queries=queries)

    def replace_data_map(self, map_source, queries=[]):
        """Replace values in a HXL dataset."""
        import hxl.filters
        replacements = hxl.filters.ReplaceDataFilter.Replacement.parse_map(hxl.data(map_source))
        return hxl.filters.ReplaceDataFilter(self, replacements, queries=queries)

    def add_columns(self, specs, before=False):
        """Add fixed-value columns to a HXL dataset."""
        import hxl.filters
        return hxl.filters.AddColumnsFilter(self, specs=specs, before=before)

    def rename_columns(self, specs):
        """Changes headers and tags on a column."""
        import hxl.filters
        return hxl.filters.RenameFilter(self, specs)

    def clean_data(
            self, whitespace=[], upper=[], lower=[], date=[], date_format=None,
            number=[], number_format=None, latlon=[], purge=False, queries=[]
    ):
        """Clean data fields."""
        import hxl.filters
        return hxl.filters.CleanDataFilter(
            self,
            whitespace=whitespace,
            upper=upper,
            lower=lower,
            date=date, date_format=date_format,
            number=number, number_format=number_format,
            latlon=latlon,
            purge=purge,
            queries=queries
        )
    
    def merge_data(self, merge_source, keys, tags, replace=False, overwrite=False, queries=[]):
        """Merges values from a second dataset."""
        import hxl.filters
        return hxl.filters.MergeDataFilter(self, merge_source, keys, tags, replace, overwrite, queries=queries)

    def explode(self, header_attribute='header', value_attribute='value'):
        """Explodes a wide dataset into a long datasets.
        @param header_attribute: the attribute to add to the hashtag of the column with the former header (default 'header')
        @param value_attribute: the attribute to add to the hashtag of the column with the former value (default 'value')
        @return: filtered dataset.
        @see hxl.filters.ExplodeFilter
        """
        
        import hxl.filters
        return hxl.filters.ExplodeFilter(self, header_attribute, value_attribute)

    def jsonpath(self, path, patterns=[], queries=[]):
        """Parse the value as a JSON expression and extract data from it.
        See http://goessner.net/articles/JsonPath/
        @param path: a JSONPath expression for extracting data
        @param patterns: a tag pattern or list of patterns for the columns to use (default to all)
        @param queries: a predicate or list of predicates for the rows to consider.
        @returns: filtered dataset
        @see: hxl.filters.JSONPathFilter
        """
        import hxl.filters
        return hxl.filters.JSONPathFilter(self, path, patterns=patterns, queries=queries)

    def fill_data(self, patterns=[], queries=[]):
        """Fills empty cells in a column using the last non-empty value.
        @param patterns: a tag pattern or list of patterns for the columns to fill (default to all)
        @param queries: a predicate or list of predicates for rows to fill (leave any blank that don't match).
        @return filtered dataset
        @see hxl.filters.FillFilter
        """
        import hxl.filters
        return hxl.filters.FillDataFilter(self, patterns=patterns, queries=queries)

    #
    # Generators
    #

    def gen_raw(self, show_headers=True, show_tags=True):
        """Generate an array representation of a HXL dataset, one at a time."""
        if show_headers:
            yield self.headers
        if show_tags:
            yield self.display_tags
        for row in self:
            yield row.values

    def gen_csv(self, show_headers=True, show_tags=True):
        """Generate a CSV representation of a HXL dataset, one row at a time."""
        class TextOut:
            """Simple string output source to capture CSV"""
            def __init__(self):
                self.data = ''
            def write(self, s):
                self.data += s
            def get(self):
                data = self.data
                self.data = ''
                return data
        output = TextOut()
        writer = csv.writer(output)
        for raw in self.gen_raw(show_headers, show_tags):
            writer.writerow(raw)
            yield output.get()

    def gen_json(self, show_headers=True, show_tags=True, use_objects=False):
        """Generate a JSON representation of a HXL dataset, one row at a time."""
        is_first = True
        yield "[\n"
        if use_objects:
            for row in self:
                if is_first:
                    is_first = False
                    yield json.dumps(row.dictionary, sort_keys=True, indent=2)
                else:
                    yield ",\n" + json.dumps(row.dictionary, sort_keys=True, indent=2)
        else:
            for raw in self.gen_raw(show_headers, show_tags):
                if is_first:
                    is_first = False
                    yield json.dumps(raw)
                else:
                    yield ",\n" + json.dumps(raw)
        yield "\n]\n"


class Column(object):
    """
    The definition of a logical column in the HXL data.
    """ 

    # Regular expression to match a HXL tag
    PATTERN = r'^\s*(#{token})((?:\s*\+{token})*)\s*$'.format(token=hxl.datatypes.TOKEN_PATTERN)

    # To tighten debugging (may reconsider later -- not really a question of memory efficiency here)
    __slots__ = ['tag', 'attributes', 'attribute_list', 'header', 'column_number']

    def __init__(self, tag=None, attributes=(), header=None, column_number=None):
        """
        Initialise a column definition.
        @param tag: the HXL hashtag for the column (default: None)
        @param attributes: (optional) a sequence of attributes (default: ())
        @param header: (optional) the original plaintext header for the column (default: None)
        @param column_number: (optional) the zero-based column number
        """
        if tag:
            tag = tag.lower()
        self.tag = tag
        self.header = header
        self.column_number = column_number
        self.attributes = set([a.lower() for a in attributes])
        self.attribute_list = [a.lower() for a in attributes] # to preserve order

    @property
    def display_tag(self):
        """Default display version of a HXL hashtag.
        Attributes are not sorted.
        """
        return self.get_display_tag(sort_attributes=False)
    
    def get_display_tag(self, sort_attributes=False):
        """
        Generate a display version of the column hashtag
        @param sort_attributes: if True, sort attributes; otherwise, preserve the original order
        @return the reassembled HXL hashtag string, including language code
        """
        if self.tag:
            s = self.tag
            for attribute in sorted(self.attribute_list) if sort_attributes else self.attribute_list:
                s += '+' + attribute
            return s
        else:
            return ''

    def has_attribute(self, attribute):
        """Check if an attribute is present."""
        return (attribute in self.attribute_list)

    def add_attribute(self, attribute):
        """Add an attribute to the column."""
        if attribute not in self.attributes:
            self.attributes.add(attribute)
            self.attribute_list.append(attribute)
        return self

    def remove_attribute(self, attribute):
        """Remove an attribute from the column."""
        if attribute in self.attributes:
            self.attributes.remove(attribute)
            self.attribute_list.remove(attribute)
        return self

    def __hash__(self):
        """Make columns usable in a dictionary.
        Only the hashtag and attributes are used.
        """
        hash_value = hash(self.tag)
        for attribute in self.attributes:
            hash_value += hash(attribute)
        return hash_value

    def __eq__(self, other):
        """Test for comparison with another object.
        For equality, only the hashtag and attributes have to be the same."""
        try:
            return (self.tag == other.tag and self.attributes == other.attributes)
        except:
            return False

    def __repr__(self):
        return self.display_tag

    __str__ = __repr__

    @staticmethod
    def parse(raw_string, header=None, use_exception=False, column_number=None):
        """
        Attempt to parse a full hashtag specification.
        """
        # Already parsed?
        if isinstance(raw_string, Column):
            return raw_string
        
        # Pattern for a single tag
        result = re.match(Column.PATTERN, raw_string)
        if result:
            tag = result.group(1)
            attribute_string = result.group(2)
            if attribute_string:
                attributes = re.split(r'\s*\+', attribute_string.strip().strip('+'))
            else:
                attributes = []
            return Column(tag=tag, attributes=attributes, header=header, column_number=column_number)
        else:
            if use_exception:
                raise hxl.HXLException("Malformed tag expression: " + raw_string)
            else:
                return None

    @staticmethod
    def parse_spec(raw_string, default_header=None, use_exception=False, column_number=None):
        """Attempt to parse a single-string header/hashtag spec"""
        # Already parsed?
        if isinstance(raw_string, Column):
            return raw_string
        
        matches = re.match(r'^(.*)(#.*)$', raw_string)
        if matches:
            header = matches.group(1) if matches.group(1) else default_header
            return Column.parse(matches.group(2), header=header, column_number=column_number)
        else:
            return Column.parse('#' + raw_string, header=default_header, column_number=column_number)

class Row(object):
    """
    An iterable row of values in a HXL dataset.
    """

    # Predefine the slots for efficiency (may reconsider later)
    __slots__ = ['columns', 'values', 'row_number', 'source_row_number']

    def __init__(self, columns, values=[], row_number=None, source_row_number=None):
        """
        Set up a new row.
        @param columns: The column definitions (array of Column objects).
        @param values: (optional) The string values for the row (default: [])
        @param row_number: (optional) The zero-based logical row number in the input dataset, if available (default: None)
        @param source_row_number: (optional) The zero-based source row number in the input dataset, if available (default: None)
        """
        self.columns = columns
        self.values = copy.copy(values)
        self.row_number = row_number
        self.source_row_number = source_row_number

    def append(self, value):
        """
        Append a value to the row.
        @param value The new value to append.
        @return The new value
        """
        self.values.append(value)
        return value

    def get(self, tag, index=None, default=None, parsed=False):
        """
        Get a single value for a tag in a row.
        If no index is provided ("None"), return the first non-empty value.
        @param tag: A TagPattern or a string value for a tag.
        @param index: The zero-based index if there are multiple values for the tag (default: None)
        @param default: The default value if not found (default: None). Never parsed, even if parsed=True
        @param parsed: If true, use attributes as hints to try to parse the value (e.g. number, list, date)
        @return The value found, or the default value provided. If parsed=True, the return value will be a list (default: False)
        """

        # FIXME - move externally, use for get_all as well, and support numbers and dates
        def parse(column, value):
            if parsed:
                if column.has_attribute('list'):
                    return re.split("\s*,\s*", value)
                else:
                    return [value]
            return value

        if type(tag) is TagPattern:
            pattern = tag
        else:
            pattern = TagPattern.parse(tag)

        for i, column in enumerate(self.columns):
            if i >= len(self.values):
                break
            if pattern.match(column):
                if index is None:
                    # None (the default) is a special case: it means look
                    # for the first truthy value
                    if self.values[i]:
                        return parse(column, self.values[i])
                else:
                    # Otherwise, look for a specific index
                    if index == 0:
                        return parse(column, self.values[i])
                    else:
                        index = index - 1
        return default

    def get_all(self, tag, default=None):
        """
        Get all values for a specific tag in a row
        @param tag A TagPattern or a string value for a tag.
        @return An array of values for the HXL hashtag.
        """

        if type(tag) is TagPattern:
            pattern = tag
        else:
            pattern = TagPattern.parse(tag)

        result = []
        for i, column in enumerate(self.columns):
            if i >= len(self.values):
                break
            if pattern.match(column):
                value = self.values[i]
                if default is not None and not value:
                    value = default
                result.append(value)
        return result

    def key(self, patterns=None):
        """Generate a unique key tuple for the row, based on a list of tag patterns
        @param patterns: a list of L{TagPattern} objects, or a parseable string
        @returns: the key as a tuple (might be empty)
        """
        if patterns:
            patterns = TagPattern.parse_list(patterns)

        def in_key(col):
            if not patterns:
                return True
            for pattern in patterns:
                if pattern.match(col):
                    return True
            return False

        key = []
        for i, value in enumerate(self.values):
            if i < len(self.columns) and in_key(self.columns[i]):
                key.append(hxl.datatypes.normalise(value, self.columns[i]))
        return tuple(key)

    @property
    def dictionary(self):
        """Return the row as a Python dict.
        The keys will be HXL hashtags and attributes, normalised per HXL 1.1.
        If two or more columns have the same hashtags and attributes, only the first will be included.
        @return: The row as a Python dictionary.
        """
        data = {}
        for i, col in enumerate(self.columns):
            key = col.get_display_tag(sort_attributes=True)
            if key and (not key in data) and (i < len(self.values)):
                data[key] = self.values[i]
        return data

    def __getitem__(self, index):
        """
        Array-access method to make this class iterable.
        @param index The zero-based index of a value to look up.
        @return The value if it exists.
        @exception IndexError if the index is out of range.
        """
        return self.values[index]

    def __str__(self):
        """
        Create a string representation of a row for debugging.
        """
        s = '<Row';
        for column_number, value in enumerate(self.values):
            s += "\n  " + str(self.columns[column_number]) + "=" + str(value)
        s += "\n>"
        return s


class RowQuery(object):
    """Query to execute against a row of HXL data."""

    def __init__(self, pattern, op, value, is_aggregate=False):
        """Constructor
        @param pattern: the L{TagPattern} to match in the row
        @param op: the operator function to use for comparison
        @param value: the value to compare against
        @param is_aggregate: if True, the value is a special placeholder like "min" or "max" that needs to be calculated
        """
        self.pattern = TagPattern.parse(pattern)
        self.op = op
        self.value = value

        self.is_aggregate=is_aggregate
        self.needs_aggregate = False
        """Need to calculate an aggregate value"""
        
        if is_aggregate:
            self.needs_aggregate = True

        # calculate later
        self.date_value = None
        self.number_value = None
        self._saved_indices = None

    def calc_aggregate(self, dataset):
        """Calculate the aggregate value that we need for the row query
        Substitute the special values "min" and "max" with aggregates.
        @param dataset: the HXL dataset to use (must be cached)
        """
        if not self.needs_aggregate:
            logger.warning("no aggregate calculation needed")
            return # no need to calculate
        if not dataset.is_cached:
            raise HXLException("need a cached dataset for calculating an aggregate value")
        if self.value == 'min':
            self.value = dataset.min(self.pattern)
            self.op = operator.eq
        elif self.value == 'max':
            self.value = dataset.max(self.pattern)
            self.op = operator.eq
        elif self.value == 'not min':
            self.value = dataset.min(self.pattern)
            self.op = operator.ne
        elif self.value == 'not max':
            self.value = dataset.max(self.pattern)
            self.op = operator.ne
        else:
            raise HXLException("Unrecognised aggregate: {}".format(value))
        self.needs_aggregate = False
                               
    def match_row(self, row):
        """Check if a key-value pair appears in a HXL row"""

        # fail if we need an aggregate and haven't calculated it
        if self.needs_aggregate and not self.aggregate_is_calculated:
            raise HXLException("must call calc_aggregate before matching an 'is min' or 'is max' condition")

        # initialise is this is the first time matching for the row query
        if self._saved_indices is None:
            if self.pattern.tag == '#date' and hxl.datatypes.is_date(self.value):
                self.date_value = hxl.datatypes.normalise_date(self.value)
            elif hxl.datatypes.is_number(self.value):
                self.number_value = hxl.datatypes.normalise_number(self.value)

        # try all the matching column values
        indices = self._get_saved_indices(row.columns)
        for i in indices:
            if i < len(row.values) and self.match_value(row.values[i], self.op):
                return True
        return False

    def match_value(self, value, op):
        """Try matching as dates, then as numbers, then as simple strings"""
        if self.date_value is not None and hxl.datatypes.is_date(value):
            return op(hxl.datatypes.normalise_date(value), self.date_value)
        elif self.number_value is not None and hxl.datatypes.is_number(value):
            return op(hxl.datatypes.normalise_number(value), self.number_value)
        else:
            return self.op(hxl.datatypes.normalise_string(value), hxl.datatypes.normalise_string(self.value))

    def _get_saved_indices(self, columns):
        """Cache the column tests, so that we run them only once."""
        # FIXME - assuming that the columns never change
        self._saved_indices = []
        for i in range(len(columns)):
            if self.pattern.match(columns[i]):
                self._saved_indices.append(i)
        return self._saved_indices

    @staticmethod
    def parse(query):
        """Parse a filter expression"""
        if isinstance(query, RowQuery):
            # already parsed
            return query
        parts = re.split(r'([<>]=?|!?=|!?~|\bis\b)', hxl.datatypes.normalise_string(query), maxsplit=1)
        pattern = TagPattern.parse(parts[0])
        op_name = hxl.datatypes.normalise_string(parts[1])
        op = RowQuery.OPERATOR_MAP.get(op_name)
        value = hxl.datatypes.normalise_string(parts[2])
        is_aggregate = False
        # special handling for aggregates (FIXME)
        if op_name == 'is' and value in ('min', 'max', 'not min', 'not max'):
            is_aggregate = True
        return RowQuery(pattern, op, value, is_aggregate)

    @staticmethod
    def parse_list(queries):
        """Parse a single query spec or a list of specs."""
        if queries:
            if not hasattr(queries, '__len__') or isinstance(queries, six.string_types):
                # make a list if needed
                queries = [queries]
            return [hxl.model.RowQuery.parse(query) for query in queries]
        else:
            return []

    @staticmethod
    def match_list(row, queries=None, reverse=False):
        """See if any query in a list matches a row."""
        if not queries:
            # no queries = pass
            return True
        else:
            # otherwise, must match at least one
            for query in queries:
                if query.match_row(row):
                    return not reverse
            return reverse

    @staticmethod
    def operator_re(s, pattern):
        """Regular-expression comparison operator."""
        return re.search(pattern, s)

    @staticmethod
    def operator_nre(s, pattern):
        """Regular-expression negative comparison operator."""
        return not re.search(pattern, s)

    @staticmethod
    def operator_is(s, condition):
        """Advanced tests
        Note: this won't be called for aggregate values like "is min" or "is not max";
        for these, the aggregate will already be calculated, and a simple comparison
        operator substituted by L{calc_aggregate}.
        """
        if condition == 'empty':
            return hxl.datatypes.is_empty(s)
        elif condition == 'not empty':
            return not hxl.datatypes.is_empty(s)
        elif condition == 'number':
            return hxl.datatypes.is_number(s)
        elif condition == 'not number':
            return not hxl.datatypes.is_number(s)
        elif condition == 'date':
            return (hxl.datatypes.is_date(s))
        elif condition == 'not date':
            return (hxl.datatypes.is_date(s) is False)
        else:
            raise hxl.HXLException('Unknown is condition: {}'.format(condition))
    

    # Constant map of comparison operators
    OPERATOR_MAP = {
        '=': operator.eq,
        '!=': operator.ne,
        '<': operator.lt,
        '<=': operator.le,
        '>': operator.gt,
        '>=': operator.ge,
    }


# Extra static initialisation
RowQuery.OPERATOR_MAP['~'] = RowQuery.operator_re
RowQuery.OPERATOR_MAP['!~'] = RowQuery.operator_nre
RowQuery.OPERATOR_MAP['is'] = RowQuery.operator_is


# end
