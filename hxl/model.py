"""
Data model for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import abc
from copy import copy

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

    @property
    def headers(self):
        """
        Return a list of header strings (for a spreadsheet row).
        """
        return list(map(lambda column: column.headerText, self.columns))

    @property
    def tags(self):
        """
        Return a list of tags.
        """
        return list(map(lambda column: column.hxlTag, self.columns))

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
            if column.headerText:
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
        self.columns = copy(columns)
        self.rows = copy(rows)

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
    __slots__ = ['columnNumber', 'sourceColumnNumber', 'hxlTag', 'languageCode', 'headerText']

    def __init__(self, columnNumber=None, sourceColumnNumber=None, hxlTag=None, languageCode=None, headerText=None):
        """
        Initialise a column definition.
        @param columnNumber the logical column number (default: None)
        @param sourceColumnNumber the raw column number in the source dataset (default: None)
        @param hxlTag the HXL hashtag for the column (default: None)
        @param languageCode the ISO 639- language code for the column, e.g. "es" (default: None)
        @param headerText the original plaintext header for the column (default: None)
        """
        self.columnNumber = columnNumber
        self.sourceColumnNumber = sourceColumnNumber
        self.hxlTag = hxlTag
        self.languageCode = languageCode
        self.headerText = headerText

    @property
    def displayTag(self):
        """
        Generate a display version of the column hashtag
        @return the reassembled HXL hashtag string, including language code
        """
        if (self.hxlTag):
            if (self.languageCode):
                return self.hxlTag + '/' + self.languageCode
            else:
                return self.hxlTag
        else:
            return None

    def __str__(self):
        """
        Create a string representation of a column header for debugging.
        """
        tag = self.displayTag
        if tag:
            return '<HXLColumn ' + str(tag) + '>'
        else:
            return '<HXLColumn>'


class HXLRow(object):
    """
    An iterable row of values in a HXL dataset.
    """

    # Predefine the slots for efficiency (may reconsider later)
    __slots__ = ['columns', 'values', 'rowNumber', 'sourceRowNumber']

    def __init__(self, columns, rowNumber=None, sourceRowNumber=None, values=[]):
        """
        Set up a new row.
        @param columns The column definitions (array of HXLColumn objects).
        @param rowNumber The logical row number in the input dataset (default: None)
        @param sourceRowNumber The original row number in the raw source dataset (default: None)
        """
        self.columns = columns
        self.values = copy(values)
        self.rowNumber = rowNumber
        self.sourceRowNumber = sourceRowNumber

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
            if column.hxlTag == tag:
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
            if column.hxlTag == tag:
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
        s += "\n  rowNumber: " + str(self.rowNumber)
        s += "\n  sourceRowNumber: " + str(self.sourceRowNumber)
        for columnNumber, value in enumerate(self.values):
            s += "\n  " + str(self.columns[columnNumber]) + "=" + str(value)
        s += "\n>"
        return s

# end
