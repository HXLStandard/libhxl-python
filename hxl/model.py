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

class HXLDataProvider(object):
    """
    Abstract base class for a HXL data source.

    Any source of parsed HXL data inherits from this class: that
    includes HXLDataset, HXLReader, and the various filters in the
    hxl.filters package.  The contract of a HXLDataProvider is that it will
    provide a columns property and a next() method to read through the
    rows.

    The child class must implement the columns() method as a property
    and the next() method to iterate through rows of data.
    """

    __metaclass__ = abc.ABCMeta

    def __iter__(self):
        """
        The object itself is an iterator.
        """
        return self

    @property
    @abc.abstractmethod
    def columns(self):
        """
        Get the column definitions for the dataset.
        @return a list of HXLColumn objects.
        """
        return

    @abc.abstractmethod
    def __next__(self):
        """
        Iterable function to return the next row of HXL values.
        @return an iterable HXLRow
        @exception StopIteration exception at end of the rows.
        """
        return

    next = __next__

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
    def displayTags(self):
        """
        Return a list of display tags.
        """
        return list(map(lambda column: column.displayTag, self.columns))

    @property
    def hasHeaders(self):
        """
        Report whether any non-empty header strings exist.
        """
        for column in self.columns:
            if column.header:
                return True
        return False

class HXLDataset(HXLDataProvider):
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
            return '<HXLDataset ' + self.url + '>'
        else:
            return '<HXLDataset>'


class HXLColumn(object):
    """
    The definition of a logical column in the HXL data.
    """ 

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
    def displayTag(self):
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
        return self.displayTag

    __str__ = __repr__


class HXLRow(object):
    """
    An iterable row of values in a HXL dataset.
    """

    # Predefine the slots for efficiency (may reconsider later)
    __slots__ = ['columns', 'values', 'row_number', 'source_row_number']

    def __init__(self, columns, row_number=None, source_row_number=None, values=[]):
        """
        Set up a new row.
        @param columns The column definitions (array of HXLColumn objects).
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
        @param tag The HXL tag for the value.
        @param index The zero-based index if there are multiple values for the tag (default: 0)
        @param default The default value if not found (default: None)
        @return The value found, or the default value provided.
        """
        for i, column in enumerate(self.columns):
            if i >= len(self.values):
                break
            if column.tag == tag:
                if index == 0:
                    return self.values[i]
                else:
                    index = index - 1
        return default

    def getAll(self, tag):
        """
        Get all values for a specific tag in a row
        @param tag The HXL tag for the value(s).
        @return An array of values for the HXL hashtag.
        """
        result = []
        for i, column in enumerate(self.columns):
            if i >= len(self.values):
                break
            if column.tag == tag:
                result.append(self.values[i])
        return result

    def map(self, function):
        """
        Map a function over a row and return the result.
        
        The function returns a new list constructed from the return
        values of the mapping function, which must take two arguments
        (the value, and the HXLColumn object).
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
        s = '<HXLRow';
        s += "\n  row_number: " + str(self.row_number)
        s += "\n  source_row_number: " + str(self.source_row_number)
        for column_number, value in enumerate(self.values):
            s += "\n  " + str(self.columns[column_number]) + "=" + str(value)
        s += "\n>"
        return s

# end
