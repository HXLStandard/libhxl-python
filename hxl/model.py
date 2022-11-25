"""Main data-model classes for the Humanitarian Exchange Language (HXL).

This module defines the basic classes for working with HXL data. Other
modules have classes derived from these (e.g. in
[hxl.filters](filters.html) or [hxl.input](io.html)). The core class is
[Dataset](#hxl.model.Dataset), which defines the operations available
on a HXL dataset, including convenience methods for chaining filters.

Typical usage:

    source = hxl.data("https://example.org/data.csv")
    # returns a hxl.model.Dataset object

    result = source.with_lines("#country+name=Kenya").sort()
    # a filtered/sorted view of the data


This code is released into the Public Domain and comes with NO WARRANTY.

"""

import abc, copy, csv, dateutil, hashlib, json, logging, operator, re, six

import hxl

from hxl.util import logup

logger = logging.getLogger(__name__)


# Cut off for fuzzy detection of a hashtag row
# At least this percentage of cells must parse as HXL hashtags
FUZZY_HASHTAG_PERCENTAGE = 0.5


class TagPattern(object):
    """Pattern for matching a HXL hashtag and attributes

    - the pattern "#*" matches any hashtag/attribute combination
    - the pattern "#*+foo" matches any hashtag with the foo attribute
    - the pattern "#tag" matches #tag with any attributes
    - the pattern "#tag+foo" matches #tag with foo among its attributes
    - the pattern "#tag-foo" matches #tag with foo *not* among its attributes
    - the pattern "#tag+foo-bar" matches #tag with foo but not bar
    - the pattern "#tag+foo+bar!" matches #tag with exactly the attributes foo and bar, but *no others*

    The normal way to create a tag pattern is using the
    [parse()](#hxl.model.TagPattern.parse) method rather than the
    constructor:

        pattern = hxl.model.TagPattern.parse("#affected+f-children")

    Args:
        tag: the basic hashtag (without attributes)
        include_attributes: a list of attributes that must be present
        exclude_attributes: a list of attributes that must not be present
        is_absolute: if True, no attributes are allowed except those in _include_attributes_

    """


    PATTERN = r'^\s*#?({token}|\*)((?:\s*[+-]{token})*)\s*(!)?\s*$'.format(token=hxl.datatypes.TOKEN_PATTERN)
    """Constant: regular expression to match a HXL tag pattern.
    """

    def __init__(self, tag, include_attributes=[], exclude_attributes=[], is_absolute=False):
        self.tag = tag

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
        """Parse a single tag-pattern string.

            pattern = TagPattern.parse("#affected+f-children")

        The [parse_list()](#hxl.model.TagPattern.parse_list) method
        will call this method to parse multiple patterns at once.

        Args:
            s: the tag-pattern string to parse

        Returns:
            A TagPattern object

        """

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
        """Parse a list of tag-pattern strings.

        If _specs_ is a list of already-parsed TagPattern objects, do
        nothing. If it's a list of strings, apply
        [parse()](#hxl.model.TagPattern.parse) to each one. If it's a
        single string with multiple patterns separated by commas,
        split the string, then parse the patterns.

            patterns = TagPattern.parse_list("#affected+f,#inneed+f")
            # or
            patterns = TagPattern.parse_list("#affected+f", "#inneed+f")

        Args:
            specs: the raw input (a list of strings, or a single string with commas separating the patterns)

        Returns:
            A list of TagPattern objects.

        """
        if not specs:
            return []
        if isinstance(specs, six.string_types):
            specs = specs.split(',')
        return [TagPattern.parse(spec) for spec in specs]

    @staticmethod
    def match_list(column, patterns):
        """Test if a column matches any of the patterns in a list.

        This is convenient to use together with [parse_list()](hxl.model.TagPattern.parse_list):

            patterns = TagPattern.parse_list(["#affected+f", "#inneed+f"])
            if TagPattern.match_list(column, patterns):
                print("The column matched one of the patterns")

        Args:
            column: the column to test
            patterns: a list of zero or more patterns.

        Returns:
            True if there is a match

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
    def columns_hash(self):
        """Generate a hash across all of the columns in the dataset.

        This function helps detect whether two HXL documents are of
        the same type, even if they contain different data (e.g. the
        HXL API output for the same humanitarian dataset in two
        different months or two different countries).

        It takes into account text headers, hashtags, the order of
        attributes, and the order of columns. Whitespace is
        normalised, and null values are treated as empty strings. The
        MD5 hash digest is generated from a UTF-8 encoded version of
        each header.

        @returns: a 32-character hex-formatted MD5 hash string

        """
        return hxl.Column.hash_list(self.columns)

    @property
    def data_hash(self):
        """Generate a hash for the entire dataset.

        This function allows checking if two HXL datasets are
        functionally identical. It takes into account text headers,
        hashtags, the order of attributes, and the order of
        columns. Whitespace is normalised, and null values are treated
        as empty strings. The MD5 hash digest is generated from a
        UTF-8 encoded version of each header and data cell.

        @returns: a 32-character hex-formatted MD5 hash string
        """
        md5 = hashlib.md5()
        # text header row
        for column in self.columns:
            md5.update(hxl.datatypes.normalise_space(column.header).encode('utf-8'))
        # hashtag row
        for column in self.columns:
            md5.update(hxl.datatypes.normalise_space(column.display_tag).encode('utf-8'))
        # data rows
        for row in self:
            for value in row:
                md5.update(hxl.datatypes.normalise_space(value).encode('utf-8'))
        return md5.hexdigest()

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


    def get_column_indices(self, tag_patterns, columns):
        """Get a list of indices that match the tag patterns provided
        @param tag_patterns: a list of tag patterns or a string version of the list
        @param columns: a list of columns
        @returns: a (possibly-empty) list of 0-based indices
        """
        patterns = TagPattern.parse_list(tag_patterns)
        indices = []
        for i, column in enumerate(columns):
            for pattern in patterns:
                if pattern.match(column):
                    indices.push(i)
        return indices

    #
    # Aggregates
    #

    def _get_minmax(self, pattern, op):
        """Calculate the extreme min/max value for a tag pattern
        Will iterate through the dataset, and use values from multiple matching columns.
        Uses numbers, dates, or strings for comparison, based on the first non-empty value found.
        @param pattern: the L{hxl.model.TagPattern} to match
        @param op: operator_lt or operator_gt
        @returns: the extreme value according to operator supplied, or None if no values found
        """
        pattern = TagPattern.parse(pattern)
        result_raw = None # what's actually in the dataset
        result_normalised = None # normalised version for comparison

        # Look at every row
        for row in self:
            # Look at every matching value in every row
            for i, value in enumerate(row.get_all(pattern)):
                # ignore empty values
                if hxl.datatypes.is_empty(value):
                    continue

                # make a normalised value for comparison
                normalised = hxl.datatypes.normalise(value, row.columns[i])

                # first non-empty value is always a match
                if result_normalised is None:
                    result_raw = value
                    result_normalised = normalised
                else:
                    # try comparing the normalised types first, then strings on failure
                    try:
                        if op(normalised, result_normalised):
                            result_raw = value
                            result_normalised = normalised
                    except TypeError:
                        if op(str(normalised), str(result_normalised)):
                            result_raw = value
                            result_normalised = normalised

        return result_raw

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
        logup('Loading append list', {"list": source_list_url}, level='debug')
        logger.debug("Loading append list from %s...", source_list_url)
        append_sources = hxl.filters.AppendFilter.parse_external_source_list(source_list_url)
        logup('Done loading', {"list": source_list_url}, level='debug')
        logger.debug("Done loading")
        return hxl.filters.AppendFilter(self, append_sources, add_columns=add_columns, queries=queries)

    def cache(self):
        """Add a caching filter to the dataset."""
        import hxl.filters
        return hxl.filters.CacheFilter(self)

    def dedup(self, patterns=[], queries=[]):
        """Deduplicate a dataset."""
        import hxl.filters
        return hxl.filters.DeduplicationFilter(self, patterns=patterns, queries=queries)

    def with_columns(self, includes):
        """Select matching columns."""
        import hxl.filters
        return hxl.filters.ColumnFilter(self, include_tags=includes)

    def without_columns(self, excludes=None, skip_untagged=False):
        """Select non-matching columns."""
        import hxl.filters
        return hxl.filters.ColumnFilter(self, exclude_tags=excludes, skip_untagged=skip_untagged)

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
        """Merges values from a second dataset.
        @param merge_source: the second HXL data source
        @param keys: a single tagspec or list of tagspecs for the shared keys
        @param tags: the tags to copy over from the second dataset
        @param replace: if True, replace existing columns when present
        @param overwrite: if True, overwrite individual values in existing columns when available
        @param queries: optional row queries to control the merge
        """
        import hxl.filters
        return hxl.filters.MergeDataFilter(self, merge_source, keys, tags, replace, overwrite, queries=queries)

    def expand_lists(self, patterns=None, separator="|", correlate=False, queries=[]):
        """Expand lists by repeating rows.
        By default, applies to every column with a +list attribute, and uses "|" as the separator.
        @param patterns: a single tag pattern or list of tag patterns for columns to expand
        @param separator: the list-item separator
        """
        import hxl.filters
        return hxl.filters.ExpandListsFilter(self, patterns=patterns, separator=separator, correlate=correlate, queries=queries)

    def explode(self, header_attribute='header', value_attribute='value'):
        """Explodes a wide dataset into a long datasets.
        @param header_attribute: the attribute to add to the hashtag of the column with the former header (default 'header')
        @param value_attribute: the attribute to add to the hashtag of the column with the former value (default 'value')
        @return: filtered dataset.
        @see hxl.filters.ExplodeFilter
        """

        import hxl.filters
        return hxl.filters.ExplodeFilter(self, header_attribute, value_attribute)

    def implode(self, label_pattern, value_pattern):
        """Implodes a long dataset into a wide dataset
        @param label_pattern: the tag pattern to match the label column
        @param value_pattern: the tag pattern to match the
        @return: filtered dataset.
        @see hxl.filters.ImplodeFilter
        """
        import hxl.filters
        return hxl.filters.ImplodeFilter(self, label_pattern=label_pattern, value_pattern=value_pattern)

    def jsonpath(self, path, patterns=[], queries=[], use_json=True):
        """Parse the value as a JSON expression and extract data from it.
        See http://goessner.net/articles/JsonPath/
        @param path: a JSONPath expression for extracting data
        @param patterns: a tag pattern or list of patterns for the columns to use (default to all)
        @param queries: a predicate or list of predicates for the rows to consider.
        @param use_json: if True, serialise multiple results as JSON lists.
        @returns: filtered dataset
        @see: hxl.filters.JSONPathFilter
        """
        import hxl.filters
        return hxl.filters.JSONPathFilter(self, path, patterns=patterns, queries=queries, use_json=use_json)

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
    def hash_list(columns):
        """Generate a hash across all of the columns in the dataset.

        This function helps detect whether two HXL documents are of
        the same type, even if they contain different data (e.g. the
        HXL API output for the same humanitarian dataset in two
        different months or two different countries).

        It takes into account text headers, hashtags, the order of
        attributes, and the order of columns. Whitespace is
        normalised, and null values are treated as empty strings. The
        MD5 hash digest is generated from a UTF-8 encoded version of
        each header.

        @returns: a 32-character hex-formatted MD5 hash string

        """
        md5 = hashlib.md5()
        for column in columns:
            md5.update(hxl.datatypes.normalise_space(column.header).encode('utf-8'))
        for column in columns:
            md5.update(hxl.datatypes.normalise_space(column.display_tag).encode('utf-8'))
        return md5.hexdigest()

    @staticmethod
    def parse(raw_string, header=None, use_exception=False, column_number=None):
        """ Attempt to parse a full hashtag specification.
        @param raw_string: the string representation of the tagspec
        @param header: the text header to include
        @param use_exception: if True, throw an exception for a malformed tagspec
        @returns: None if the string is empty, False if it's malformed (and use_exception is False), or a Column object otherwise
        """

        # Already parsed?
        if isinstance(raw_string, Column):
            return raw_string

        # Empty string?
        if hxl.datatypes.is_empty(raw_string):
            return None

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
                logup('Not a HXL hashtag spec', {"string": raw_string}, level='debug')
                logger.debug("Not a HXL hashtag spec: %s", raw_string)
                return False

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

    @staticmethod
    def parse_list(raw_row, previous_row=None):
        """Try parsing a raw data row as a HXL hashtag row.

        Args:
            raw_row (list): a raw row from a ``hxl.input.AbstractInput`` object
            previous_row (list): the previous raw row, for extracting headers

        Returns:
            list: a list of hxl.model.Column objects if successfully parsed; None otherwise.

        """
        # how many values we've seen
        nonEmptyCount = 0

        # the logical column number
        hashtags_found = 0

        columns = []
        failed_hashtags = []

        for source_column_number, raw_string in enumerate(raw_row):
            if previous_row and source_column_number < len(previous_row):
                header = previous_row[source_column_number]
            else:
                header = None
            if not hxl.datatypes.is_empty(raw_string):
                raw_string = hxl.datatypes.normalise_string(raw_string)
                nonEmptyCount += 1
                column = hxl.model.Column.parse(raw_string, header=header, column_number=source_column_number)
                if column:
                    columns.append(column)
                    hashtags_found += 1
                    continue
                elif column is False:
                    failed_hashtags.append(raw_string)

            columns.append(hxl.model.Column(header=header, column_number=source_column_number))

        # Have we seen at least FUZZY_HASHTAG_PERCENTAGE?
        if (nonEmptyCount > 0) and ((hashtags_found/float(nonEmptyCount)) >= FUZZY_HASHTAG_PERCENTAGE):
            if len(failed_hashtags) > 0:
                logup('Skipping column(s) with malformed hashtag specs', {"hastags": ', '.join(failed_hashtags)}, level='error')
                logger.error('Skipping column(s) with malformed hashtag specs: %s', ', '.join(failed_hashtags))
            return columns
        else:
            return None


class Row(object):
    """ An iterable row of values in a HXL dataset.

    If a value is part of a merged area, and not in the top left position, it will be a MergedCell object.
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
                    return re.split(r'\s*,\s*', value)
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

    def key(self, patterns=None, indices=None):
        """Generate a unique key tuple for the row, based on a list of tag patterns
        @param patterns: a list of L{TagPattern} objects, or a parseable string
        @returns: the key as a tuple (might be empty)
        """

        key = []

        # if the user doesn't provide indices, get indices from the pattern
        if not indices and patterns:
            indices = get_column_indices(patterns, self.columns)

        if indices:
            # if we have indices, use them to build the key
            for i in indices:
                if i < len(self.values):
                    key.append(hxl.datatypes.normalise(self.values[i], self.columns[i]))
        else:
            # if there are still no indices, use the whole row for the key
            for i, value in enumerate(self.values):
                key.append(hxl.datatypes.normalise(value, self.columns[i]))

        return tuple(key) # make it into a tuple so that it's hashable


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

        # if the value is a formula, extract it
        self.formula = None
        result = re.match(r'^{{(.+)}}$', hxl.datatypes.normalise_space(value))
        if result:
            self.formula = result.group(1)

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
            logup('no aggregate calculation needed', level='warning')
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
        if self._saved_indices is None or self.formula:

            # if it's a row formula, evaluate first
            if self.formula:
                value = hxl.formulas.eval.eval(row, self.formula)
            else:
                value = self.value

            if self.pattern.tag == '#date':
                try:
                    self.date_value = hxl.datatypes.normalise_date(value)
                except ValueError:
                    self.date_value = None

            try:
                self.number_value = hxl.datatypes.normalise_number(value)
            except ValueError:
                self.number_value = None

            self.string_value = hxl.datatypes.normalise_string(value)

        # try all the matching column values
        indices = self._get_saved_indices(row.columns)
        for i in indices:
            if i < len(row.values) and self.match_value(row.values[i], self.op):
                return True
        return False


    def match_value(self, value, op):
        """Try matching as dates, then as numbers, then as simple strings"""
        if self.date_value is not None:
            try:
                return op(hxl.datatypes.normalise_date(value), self.date_value)
            except ValueError:
                pass

        if self.number_value is not None:
            try:
                return op(hxl.datatypes.normalise_number(value), self.number_value)
            except:
                pass

        return self.op(hxl.datatypes.normalise_string(value), self.string_value)

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


# Static functions

def get_column_indices(tag_patterns, columns):
    """Get a list of column indices that match the tag patterns provided
    @param tag_patterns: a list of tag patterns or a string version of the list
    @param columns: a list of columns
    @returns: a (possibly-empty) list of 0-based indices
    """
    tag_patterns = TagPattern.parse_list(tag_patterns)
    columns = [Column.parse(column) for column in columns]
    indices = []
    for i, column in enumerate(columns):
        for pattern in tag_patterns:
            if pattern.match(column):
                indices.append(i)
    return indices


# Extra static initialisation
RowQuery.OPERATOR_MAP['~'] = RowQuery.operator_re
RowQuery.OPERATOR_MAP['!~'] = RowQuery.operator_nre
RowQuery.OPERATOR_MAP['is'] = RowQuery.operator_is


# end
