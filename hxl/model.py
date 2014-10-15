"""
Data model for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

class HXLColumn(object):
    """
    The definition of a logical column in the HXL data.
    """ 

    __slots__ = ['columnNumber', 'sourceColumnNumber', 'hxlTag', 'languageCode', 'headerText']

    def __init__(self, columnNumber, sourceColumnNumber, hxlTag=None, languageCode=None, headerText=None):
        self.columnNumber = columnNumber
        self.sourceColumnNumber = sourceColumnNumber
        self.hxlTag = hxlTag
        self.languageCode = languageCode
        self.headerText = headerText

    def getDisplayTag(self):
        """Generate a display version of the column hashtag"""
        if (self.hxlTag):
            if (self.languageCode):
                return self.hxlTag + '/' + self.languageCode
            else:
                return self.hxlTag
        else:
            return None

    def __str__(self):
        tag = self.getDisplayTag()
        if tag:
            return '<HXLColumn ' + str(tag) + '>'
        else:
            return '<HXLColumn>'

class HXLRow(object):
    """
    An iterable row of HXLValue objects in a HXL dataset.
    """

    __slots__ = ['columns', 'values', 'rowNumber', 'sourceRowNumber']

    def __init__(self, columns, rowNumber=None, sourceRowNumber=None):
        self.columns = columns
        self.values = []
        self.rowNumber = rowNumber
        self.sourceRowNumber = sourceRowNumber

    def append(self, value):
        self.values.append(value)

    def __getitem__(self, index):
        return HXLValue(self.columns[index], self.values[index])

    def __str__(self):
        s = '<HXLRow';
        s += "\n  rowNumber: " + str(self.rowNumber)
        s += "\n  sourceRowNumber: " + str(self.sourceRowNumber)
        for value in self.values:
            s += "\n  " + str(value)
        s += "\n>"
        return s

class HXLValue(object):
    """
    A single HXL value at the intersection of a row and column
    """

    __slots__ = ['column', 'content']

    def __init__(self, column, content):
        self.column = column
        self.content = content

    def __str__(self):
        s = '<HXLValue'
        if self.column:
            s += ' ' + str(self.column.hxlTag) + '=' + str(self.content)
        s += '>'
        return s

# end
