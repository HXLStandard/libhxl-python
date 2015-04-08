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
import hxl

class DataProvider(object):
    """
    Abstract base class for a HXL data source.

    Any source of parsed HXL data inherits from this class: that
    includes Dataset, HXLReader, and the various filters in the
    hxl.filters package.  The contract of a DataProvider is that it will
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
        return list(map(lambda column: column.header, self.columns))

    @property
    def tags(self):
        """
        Return a list of tags.
        """
        return list(map(lambda column: column.tag, self.columns))

    @property
    def display_tags(self):
        """
        Return a list of display tags.
        """
        return list(map(lambda column: column.display_tag, self.columns))

    @property
    def has_headers(self):
        """
        Report whether any non-empty header strings exist.
        """
        for column in self.columns:
            if column.header:
                return True
        return False

    def with_columns(self, whitelist):
        import hxl.filters.cut
        if not hasattr(whitelist, '__len__') or isinstance(whitelist, str):
            whitelist = [whitelist]
        return hxl.filters.cut.CutFilter(self, include_tags=map(TagPattern.parse, whitelist))

    def without_columns(self, blacklist):
        import hxl.filters.cut
        if not hasattr(blacklist, '__len__') or isinstance(blacklist, str):
            blacklist = [blacklist]
        return hxl.filters.cut.CutFilter(self, exclude_tags=map(TagPattern.parse, blacklist))

class Dataset(DataProvider):
    """
    In-memory HXL dataset.
    """

    def __init__(self, url=None, columns=[], rows=[]):
        """
        Initialise a dataset.
        @param url The dataset's URL (default: None).
        """
        self.url = url
        self.columns = copy.copy(columns)
        self.rows = copy.copy(rows)

    def __str__(self):
        """
        Create a string representation of a dataset for debugging.
        @return A debugging string.
        """
        if self.url:
            return '<Dataset ' + self.url + '>'
        else:
            return '<Dataset>'


class TagPattern(object):
    """
    Pattern for matching a tag.

    #tag matches #tag with any attributes
    #tag+foo matches #tag with foo among its attributes
    #tag-foo matches #tag with foo *not* among its attributes
    #tag+foo-bar matches #tag with foo but not bar
    """

    # Regular expression to match a HXL tag pattern (including '-' to exclude attributes)
    PATTERN = r'^\s*#?({token})((?:[+-]{token})*)\s*$'.format(token=hxl.TOKEN)

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
        """Parse a single tagspec, like #tag+foo-bar."""
        if isinstance(s, TagPattern):
            return s
        result = re.match(TagPattern.PATTERN, s)
        if result:
            tag = '#' + result.group(1)
            include_attributes = []
            exclude_attributes = []
            attribute_specs = re.split(r'([+-])', result.group(2))
            for i in range(1, len(attribute_specs), 2):
                if attribute_specs[i] == '+':
                    include_attributes.append(attribute_specs[i + 1])
                else:
                    exclude_attributes.append(attribute_specs[i + 1])
            return TagPattern(tag, include_attributes=include_attributes, exclude_attributes=exclude_attributes)
        else:
            raise hxl.HXLException('Malformed tag: ' + s)

    @staticmethod
    def parse_list(s):
        """Parse a comma-separated list of tagspecs."""
        if s:
            return [TagPattern.parse(spec) for spec in s.split(',')]
        else:
            return []


class Column(object):
    """
    The definition of a logical column in the HXL data.
    """ 

    # Regular expression to match a HXL tag
    PATTERN = r'^\s*(#{token})((?:\+{token})*)\s*$'.format(token=hxl.TOKEN)

    # To tighten debugging (may reconsider later -- not really a question of memory efficiency here)
    __slots__ = ['column_number', 'source_column_number', 'tag', 'lang', 'header', 'attributes']

    def __init__(self, column_number=None, source_column_number=None, tag=None, header=None, attributes=None):
        """
        Initialise a column definition.
        @param column_number the logical column number (default: None)
        @param source_column_number the raw column number in the source dataset (default: None)
        @param tag the HXL hashtag for the column (default: None)
        @param header the original plaintext header for the column (default: None)
        """
        self.column_number = column_number
        self.source_column_number = source_column_number
        self.tag = tag
        self.header = header
        if attributes:
            self.attributes = set(attributes)
        else:
            self.attributes = {}

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
    def parse(raw_string, header=None, column_number=None, source_column_number=None, use_exception=False):
        """
        Attempt to parse a full hashtag specification.
        """
        # Pattern for a single tag
        result = re.match(Column.PATTERN, raw_string)
        if result:
            tag = result.group(1)
            attribute_string = result.group(2)
            if attribute_string:
                attributes = attribute_string[1:].split('+')
            else:
                attributes = []
            return Column(column_number=column_number, source_column_number=source_column_number, tag=tag, attributes=attributes, header=header)
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
    __slots__ = ['columns', 'values', 'row_number', 'source_row_number']

    def __init__(self, columns, row_number=None, source_row_number=None, values=[]):
        """
        Set up a new row.
        @param columns The column definitions (array of Column objects).
        @param row_number The logical row number in the input dataset (default: None)
        @param source_row_number The original row number in the raw source dataset (default: None)
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

    def map(self, function):
        """
        Map a function over a row and return the result.
        
        The function returns a new list constructed from the return
        values of the mapping function, which must take two arguments
        (the value, and the Column object).
        @param function The mapping function.
        @return A new array of values, after the mapping
        """
        values = []
        for index, value in enumerate(self.values):
            values.append(function(value, self.columns[index]))
        return values

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
        s += "\n  row_number: " + str(self.row_number)
        s += "\n  source_row_number: " + str(self.source_row_number)
        for column_number, value in enumerate(self.values):
            s += "\n  " + str(self.columns[column_number]) + "=" + str(value)
        s += "\n>"
        return s

# end
