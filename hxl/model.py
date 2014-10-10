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

class HXLRow:
    """
    A row of data in a HXL dataset.

    Implements the iterator convention.
    """
    data = None
    rowNumber = -1
    sourceRowNumber = -1
    iteratorIndex = -1

    def __init__(self, data, rowNumber=None, sourceRowNumber=None):
        self.data = data
        self.rowNumber = rowNumber
        self.sourceRowNumber = sourceRowNumber

    def next(self):
        ++self.iteratorIndex
        return self.data[iteratorIndex]

    def __iter__(self):
        return self

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

