"""
Data model for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

class HXLColumn:
    """
    The definition of a logical column in the HXL data.
    """ 
    hxlTag = None
    languageCode = None
    headerText = None

    def __init__(self, hxlTag=None, languageCode=None, headerText=None):
        self.hxlTag = hxlTag
        self.languageCode = languageCode
        self.headerText = headerText

    def getDisplayTag(self):
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
            return '<HXL col ' + str(tag) + '>'
        else:
            return '<HXL col>'

class HXLRow:
    """
    An iterable row of HXLValue objects in a HXL dataset.
    """
    values = None
    rowNumber = -1
    sourceRowNumber = -1

    def __init__(self, values, rowNumber=None, sourceRowNumber=None):
        self.values = values
        self.rowNumber = rowNumber
        self.sourceRowNumber = sourceRowNumber

    def __getitem__(self, index):
        return self.values[index]

    def __str__(self):
        s = '<HXL row';
        for value in self:
            s += "\n  " + value
        s += "\n>"
        return s

class HXLValue:
    """
    A single HXL value at the intersection of a row and column
    """
    column = None
    content = None
    columnNumber = -1
    sourceColumnNumber = -1

    def __init__(self, column, content, columnNumber, sourceColumnNumber):
        self.column = column
        self.content = content
        self.columnNumber = columnNumber
        self.sourceColumnNumber = sourceColumnNumber

    def __str__(self):
        s = '<HXL value'
        if self.column:
            s += ' ' + str(self.column.hashTag) + '=' + str(self.content)
        s += '>'
        return s

# end
