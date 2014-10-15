"""
Data model for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

class HXLColumn(object):
    """
    The definition of a logical column in the HXL data.
    """ 

    __slots__ = ['hxlTag', 'languageCode', 'headerText']

    def __init__(self, hxlTag=None, languageCode=None, headerText=None):
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

    __slots__ = ['values', 'rowNumber', 'sourceRowNumber', 'cachedData']

    def __init__(self, rowNumber=None, sourceRowNumber=None):
        self.values = []
        self.rowNumber = rowNumber
        self.sourceRowNumber = sourceRowNumber
        self.cachedData = []

    def append(self, value):
        self.values.append(value)

    @property
    def data(self):
        if self.cachedData:
            return self.cachedData
        for value in self:
            self.cachedData.append(value.content)
        return self.cachedData

    def __getitem__(self, index):
        return self.values[index]

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

    __slots__ = ['column', 'content', 'columnNumber', 'sourceColumnNumber']

    def __init__(self, column, content, columnNumber, sourceColumnNumber):
        self.column = column
        self.content = content
        self.columnNumber = columnNumber
        self.sourceColumnNumber = sourceColumnNumber

    def __str__(self):
        s = '<HXLValue'
        if self.column:
            s += ' ' + str(self.column.hxlTag) + '=' + str(self.content)
        s += '>'
        return s

# end
