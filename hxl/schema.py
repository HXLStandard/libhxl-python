"""
Validation code for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import urlparse
import re
from email.utils import parseaddr

class HXLSchemaRule(object):
    """
    Validation rule for a single HXL hashtag.
    """

    TYPE_TEXT = 1
    TYPE_NUM = 2
    TYPE_URL = 3
    TYPE_EMAIL = 4
    TYPE_PHONE = 5

    def __init__(self, hxlTag, minOccur=None, maxOccur=None, dataType=None, minValue=None, maxValue=None, valuePattern=None, valueEnumeration=None, caseSensitive=True):
        self.hxlTag = hxlTag
        self.minOccur = minOccur
        self.maxOccur = maxOccur
        self.dataType = dataType
        self.minValue = minValue
        self.maxValue = maxValue
        self.valuePattern = valuePattern
        self.valueEnumeration = valueEnumeration
        self.caseSensitive = caseSensitive

    def validateRow(self, row):
        numberSeen = 0
        values = row.getAll(self.hxlTag)
        if values:
            for value in values:
                if not self.validate(value):
                    return False
                if value:
                    numberSeen += 1
        if self.minOccur is not None and numberSeen < self.minOccur:
            return False
        elif self.maxOccur is not None and numberSeen > self.maxOccur:
            return False
        return True

    def validate(self, value):
        return self._testType(value) and self._testRange(value) and self._testPattern(value) and self._testEnumeration(value)

    def _testType(self, value):
        """Check the datatype."""
        if self.dataType == self.TYPE_NUM:
            try:
                float(value)
                return True
            except ValueError:
                return False
        elif self.dataType == self.TYPE_URL:
            pieces = urlparse.urlparse(value)
            return (pieces.scheme and pieces.netloc)
        elif self.dataType == self.TYPE_EMAIL:
            return re.match('^[^@]+@[^@]+$', value)
        elif self.dataType == self.TYPE_PHONE:
            return re.match('^\+?[0-9xX()\s-]{5,}$', value)
        else:
            return True

    def _testRange(self, value):
        if self.minValue is not None and float(value) < float(self.minValue):
            return False
        elif self.maxValue is not None and float(value) > float(self.maxValue):
            return False
        else:
            return True

    def _testPattern(self, value):
        if self.valuePattern:
            if self.caseSensitive:
                return re.match(self.valuePattern, value)
            else:
                return re.match(self.valuePattern, value, re.IGNORECASE)
        else:
            return True

    def _testEnumeration(self, value):
        if self.valueEnumeration is not None:
            if self.caseSensitive:
                return value in self.valueEnumeration
            else:
                value = value.upper()
                return value in map(lambda item: item.upper(), self.valueEnumeration)
        else:
            return True
                
class HXLSchema(object):
    """
    Schema against which to validate a HXL document.
    """

    def __init__(self, rules=[]):
        self.rules = rules

    def validateRow(self, row):
        for rule in self.rules:
            if not rule.validateRow(row):
                return False
        return True

# end



