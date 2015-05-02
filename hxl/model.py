"""
Data model for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import abc
import copy
import re
import csv
import json
import six

import hxl
from hxl.common import HXLException, normalise_string


class TagPattern(object):
    """
    Pattern for matching a tag.

    #tag matches #tag with any attributes
    #tag+foo matches #tag with foo among its attributes
    #tag-foo matches #tag with foo *not* among its attributes
    #tag+foo-bar matches #tag with foo but not bar
    """

    # Regular expression to match a HXL tag pattern (including '-' to exclude attributes)
    PATTERN = r'^\s*#?({token})((?:\s*[+-]{token})*)\s*$'.format(token=hxl.common.TOKEN)

    def __init__(self, tag, include_attributes=None, exclude_attributes=None):
        """Like a column, but has a whitelist and a blacklist."""
        self.tag = tag
        self.include_attributes = include_attributes
        self.exclude_attributes = exclude_attributes

    def match(self, column):
        """Check whether a Column matches this pattern."""
        if self.tag == column.tag:
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
            return True
        else:
            return False

    def find_column_index(self, columns):
        """Get the index of the first matching column."""
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

    def get_value(self, row):
        """Return the first matching value for this pattern."""
        for i in range(min(len(row.columns), len(row.values))):
            if self.match(row.columns[i]):
                return row.values[i]
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
            raise HXLException('Attempt to parse empty tag pattern')
        elif isinstance(s, TagPattern):
            # edge case: already parsed
            return s

        result = re.match(TagPattern.PATTERN, s)
        if result:
            tag = '#' + result.group(1)
            include_attributes = []
            exclude_attributes = []
            attribute_specs = re.split(r'\s*([+-])', result.group(2))
            for i in range(1, len(attribute_specs), 2):
                if attribute_specs[i] == '+':
                    include_attributes.append(attribute_specs[i + 1])
                else:
                    exclude_attributes.append(attribute_specs[i + 1])
            return TagPattern(tag, include_attributes=include_attributes, exclude_attributes=exclude_attributes)
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


class Dataset(object):
    """
    Abstract base class for a HXL data source.

    Any source of parsed HXL data inherits from this class: that
    includes Dataset, HXLReader, and the various filters in the
    hxl.old_filters package.  The contract of a Dataset is that it will
    provide a columns property and a next() method to read through the
    rows.

    The child class must implement the columns() method as a property
    and the next() method to iterate through rows of data.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __iter__(self):
        """
        Get the iterator over the rows.
        """
        return

    @property
    @abc.abstractmethod
    def columns(self):
        """
        Get the column definitions for the dataset.
        @return a list of Column objects.
        """
        return

    @property
    def headers(self):
        """
        Return a list of header strings (for a spreadsheet row).
        """
        return [column.header for column in self.columns]

    @property
    def tags(self):
        """
        Return a list of tags.
        """
        return [column.tag for column in self.columns]

    @property
    def display_tags(self):
        """
        Return a list of display tags.
        """
        return [column.display_tag for column in self.columns]

    @property
    def has_headers(self):
        """
        Report whether any non-empty header strings exist.
        """
        for column in self.columns:
            if column.header:
                return True
        return False

    @property
    def values(self):
        """
        Get all values for the dataset at once, in an array of arrays.
        This method can be highly inefficient for large datasets.
        """
        return [row.values for row in self]

    def get_value_set(self, tag_pattern=None, normalise=False):
        """
        Return the set of all values in a dataset (optionally matching a tag pattern).
        This method can be highly inefficient for large datasets.
        @param tag_pattern (optional) return values only for columns matching this tag pattern.
        @param normalise (optional) normalise the strings with hxl.common.normalise_string (default: False)
        @return a Python set of values
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
                new_values = [normalise_string(s) for s in new_values]
            value_set.update(new_values)
        return value_set


    #
    # Utility
    #

    def validate(self, schema=None, callback=None):
        """
        Validate the current dataset.
        @param schema (optional) the pre-compiled schema, schema filename, URL, file object, etc. Defaults to a built-in schema.
        @param callback (optional) a function to call with each error or warning. Defaults to collecting errors in an array and returning them.
        """
        return hxl.schema.hxl_schema(schema, callback).validate(self)

    #
    # Filters
    #

    def append(self, append_source, add_columns=True):
        """Append a second dataset."""
        return hxl.filters.AppendFilter(self, append_source, add_columns)

    def cache(self):
        """Add a caching filter to the dataset."""
        return hxl.filters.CacheFilter(self)

    def with_columns(self, whitelist):
        """Select matching columns."""
        return hxl.filters.ColumnFilter(self, include_tags=whitelist)

    def without_columns(self, blacklist):
        """Select non-matching columns."""
        return hxl.filters.ColumnFilter(self, exclude_tags=blacklist)

    def with_rows(self, queries):
        """Select matching rows."""
        return hxl.filters.RowFilter(self, queries=queries, reverse=False)

    def without_rows(self, queries):
        """Select non-matching rows."""
        return hxl.filters.RowFilter(self, queries=queries, reverse=True)

    def sort(self, keys=None, reverse=False):
        """Sort the dataset (caching)."""
        return hxl.filters.SortFilter(self, tags=keys, reverse=reverse)

    def count(self, patterns, aggregate_pattern=None):
        """Count values in the dataset (caching)."""
        return hxl.filters.CountFilter(self, patterns=patterns, aggregate_pattern=aggregate_pattern)

    def replace_data(self, original, replacement, patterns=None, use_regex=False):
        """Replace values in a HXL dataset."""
        return hxl.filters.ReplaceDataFilter(self, original, replacement, patterns, use_regex)

    def add_columns(self, specs, before=False):
        """Add fixed-value columns to a HXL dataset."""
        return hxl.filters.AddColumnsFilter(self, specs=specs, before=before)

    def rename_columns(self, specs):
        """Changes headers and tags on a column."""
        return hxl.filters.RenameFilter(self, specs)

    def clean_data(self, whitespace=[], upper=[], lower=[], date=[], number=[]):
        """Clean data fields."""
        return hxl.filters.CleanDataFilter(self, whitespace=whitespace, upper=upper, lower=lower, date=date, number=number)

    def merge_data(self, merge_source, keys, tags, replace=False, overwrite=False):
        """Merges values from a second dataset."""
        return hxl.filters.MergeDataFilter(self, merge_source, keys, tags, replace, overwrite)

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

    def gen_json(self, show_headers=True, show_tags=True):
        """Generate a JSON representation of a HXL dataset, one row at a time."""
        yield "[\n"
        for raw in self.gen_raw():
            yield json.dumps(raw)
        yield "]\n"

class Column(object):
    """
    The definition of a logical column in the HXL data.
    """ 

    # Regular expression to match a HXL tag
    PATTERN = r'^\s*(#{token})((?:\s*\+{token})*)\s*$'.format(token=hxl.common.TOKEN)

    # To tighten debugging (may reconsider later -- not really a question of memory efficiency here)
    __slots__ = ['tag', 'attributes', 'header']

    def __init__(self, tag=None, attributes=(), header=None):
        """
        Initialise a column definition.
        @param tag the HXL hashtag for the column (default: None)
        @param attributes a sequence of attributes (default: ())
        @param header the original plaintext header for the column (default: None)
        """
        self.tag = tag
        self.header = header
        self.attributes = set(attributes)

    @property
    def display_tag(self):
        """
        Generate a display version of the column hashtag
        @return the reassembled HXL hashtag string, including language code
        """
        if self.tag:
            s = self.tag
            for attribute in self.attributes:
                s += '+' + attribute
            return s
        else:
            return None

    def __repr__(self):
        return self.display_tag

    __str__ = __repr__

    @staticmethod
    def parse(raw_string, header=None, use_exception=False):
        """
        Attempt to parse a full hashtag specification.
        """
        # Pattern for a single tag
        result = re.match(Column.PATTERN, raw_string)
        if result:
            tag = result.group(1)
            attribute_string = result.group(2)
            if attribute_string:
                attributes = re.split(r'\s*\+', attribute_string.strip().strip('+'))
            else:
                attributes = []
            return Column(tag=tag, attributes=attributes, header=header)
        else:
            if use_exception:
                raise hxl.HXLException("Malformed tag expression: " + raw_string)
            else:
                return None

class Row(object):
    """
    An iterable row of values in a HXL dataset.
    """

    # Predefine the slots for efficiency (may reconsider later)
    __slots__ = ['columns', 'values', 'row_number']

    def __init__(self, columns, values=[], row_number=None):
        """
        Set up a new row.
        @param columns The column definitions (array of Column objects).
        @param values (optional) The string values for the row (default: [])
        @param row_number (optional) The logical row number in the input dataset (default: None)
        """
        self.columns = columns
        self.values = copy.copy(values)
        self.row_number = row_number

    def append(self, value):
        """
        Append a value to the row.
        @param value The new value to append.
        @return The new value
        """
        self.values.append(value)
        return value

    def get(self, tag, index=0, default=None):
        """
        Get a single value for a tag in a row.
        @param tag A TagPattern or a string value for a tag.
        @param index The zero-based index if there are multiple values for the tag (default: 0)
        @param default The default value if not found (default: None)
        @return The value found, or the default value provided.
        """

        if type(tag) is TagPattern:
            pattern = tag
        else:
            pattern = TagPattern.parse(tag)

        for i, column in enumerate(self.columns):
            if i >= len(self.values):
                break
            if pattern.match(column):
                if index == 0:
                    return self.values[i]
                else:
                    index = index - 1
        return default

    def get_all(self, tag):
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
                result.append(self.values[i])
        return result

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

# end
