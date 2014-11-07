"""
Data model for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

class HXLDataset(object):
    """
    In-memory HXL document.
    """

    def __init__(self, url=None):
        self.columns = []
        self.rows = []
        self.cachedTags = None
        self.cachedHeaders = None

    @property
    def headers(self):
        """
        Get a simple list of HXL hashtags from the columns.
        """
        if self.cachedHeaders == None:
            self.cachedHeaders = map(lambda column: column.headerText, self.columns)
        return self.cachedHeaders

    @property
    def tags(self):
        """
        Get a simple list of HXL hashtags from the columns.
        """
        if self.cachedTags == None:
            self.cachedTags = map(lambda column: column.hxlTag, self.columns)
        return self.cachedTags

class HXLColumn(object):
    """
    The definition of a logical column in the HXL data.
    """ 

    __slots__ = ['columnNumber', 'sourceColumnNumber', 'hxlTag', 'languageCode', 'headerText']

    def __init__(self, columnNumber=-1, sourceColumnNumber=-1, hxlTag=None, languageCode=None, headerText=None):
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
    An iterable row of values in a HXL dataset.
    """

    __slots__ = ['columns', 'values', 'rowNumber', 'sourceRowNumber']

    def __init__(self, columns, rowNumber=None, sourceRowNumber=None):
        self.columns = columns
        self.values = []
        self.rowNumber = rowNumber
        self.sourceRowNumber = sourceRowNumber

    def append(self, value):
        self.values.append(value)

    def get(self, tag, index=0, default=None):
        for i, column in enumerate(self.columns):
            if column.hxlTag == tag:
                if index == 0:
                    return self.__getitem__(i)
                else:
                    index = index - 1
        return default

    def getAll(self, tag, index=0):
        result = []
        for i, column in enumerate(self.columns):
            if column.hxlTag == tag:
                result.append(self.__getitem__(i))
        if result:
            return result
        else:
            return False

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

# end
