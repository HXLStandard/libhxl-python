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
from parser import HXLReader

class HXLValidationException(Exception):
    """
    Data structure to hold a HXL validation error.
    """

    def __init__(self, rule, message, rowNumber=-1, columnNumber=-1, sourceRowNumber=-1, sourceColumnNumber=-1):
        self.rule = rule
        self.message = message
        self.rowNumber = rowNumber
        self.sourceRowNumber = sourceRowNumber
        self.columnNumber = columnNumber
        self.sourceColumnNumber = sourceColumnNumber

class HXLSchemaRule(object):
    """
    Validation rule for a single HXL hashtag.
    """

    TYPE_TEXT = 1
    TYPE_NUMBER = 2
    TYPE_URL = 3
    TYPE_EMAIL = 4
    TYPE_PHONE = 5

    def __init__(self, hxlTag, minOccur=None, maxOccur=None, dataType=None, minValue=None, maxValue=None,
                 valuePattern=None, valueEnumeration=None, caseSensitive=True, callback=None):
        self.hxlTag = hxlTag
        self.minOccur = minOccur
        self.maxOccur = maxOccur
        self.dataType = dataType
        self.minValue = minValue
        self.maxValue = maxValue
        self.valuePattern = valuePattern
        self.valueEnumeration = valueEnumeration
        self.caseSensitive = caseSensitive
        self.callback = callback

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

    def reportError(self, message, rowNumber = -1, columnNumber = -1, sourceRowNumber = -1, sourceColumnNumber = -1):
        if self.callback != None:
            self.callback(
                HXLValidationException(
                    rule=self, message=message,
                    rowNumber=rowNumber, columnNumber=columnNumber,
                    sourceRowNumber=sourceRowNumber, sourceColumnNumber=sourceColumnNumber
                    )
                )
        return False

    def _testType(self, value):
        """Check the datatype."""
        if self.dataType == self.TYPE_NUMBER:
            try:
                float(value)
                return True
            except ValueError:
                return self.reportError("Expected a number")
        elif self.dataType == self.TYPE_URL:
            pieces = urlparse.urlparse(value)
            if not (pieces.scheme and pieces.netloc):
                return self.reportError("Expected a URL")
        elif self.dataType == self.TYPE_EMAIL:
            if not re.match('^[^@]+@[^@]+$', value):
                return self.reportError("Expected an email address")
        elif self.dataType == self.TYPE_PHONE:
            if not re.match('^\+?[0-9xX()\s-]{5,}$', value):
                return self.reportError("Expected a phone number")
        
        return True

    def _testRange(self, value):
        if self.minValue is not None:
            if float(value) < float(self.minValue):
                return self.reportError("Value is less than " + str(self.minValue))
        if self.maxValue is not None:
            if float(value) > float(self.maxValue):
                return self.reportError("Value is great than " + str(self.maxValue))
        return True

    def _testPattern(self, value):
        if self.valuePattern:
            flags = 0
            if self.caseSensitive:
                flags = re.IGNORECASE
                if not re.match(self.valuePattern, value, flags):
                    self.reportError("Failed to match pattern " + str(self.valuePattern))
                    return False
        return True

    def _testEnumeration(self, value):
        if self.valueEnumeration is not None:
            if self.caseSensitive:
                if value not in self.valueEnumeration:
                    return self.reportError("Must be one of " + str(self.valueEnumeration))
            else:
                if value.upper() not in map(lambda item: item.upper(), self.valueEnumeration):
                    return self.reportError("Must be one of " + str(self.valueEnumeration) + " (case-insensitive)")
        return True

    def __str__(self):
        """String representation of a rule (for debugging)"""
        return "<HXL schema rule: " + self.hxlTag + ">"
                
class HXLSchema(object):
    """
    Schema against which to validate a HXL document.
    """

    def __init__(self, rules=[]):
        self.rules = rules

    # TODO add support for validating columns against rules, too
    # this is where to mention impossible conditions, or columns
    # without rules

    def validateRow(self, row):
        for rule in self.rules:
            if not rule.validateRow(row):
                return False
        return True

    def __str__(self):
        """String representation of a schema (for debugging)"""
        s = "<HXL schema\n"
        for rule in self.rules:
            s += "  " + str(rule) + "\n"
        s += ">"
        return s

def loadHXLSchema(input):
    """
    Load a HXL schema from the provided input stream.
    """
    schema = HXLSchema()

    def parseType(typeString):
        if typeString == 'text':
            return HXLSchemaRule.TYPE_TEXT
        elif typeString == 'number':
            return HXLSchemaRule.TYPE_NUMBER
        elif typeString == 'url':
            return HXLSchemaRule.TYPE_URL
        elif typeString == 'email':
            return HXLSchemaRule.TYPE_EMAIL
        elif typeString == 'phone':
            return HXLSchemaRule.TYPE_PHONE
        else:
            #TODO add warning
            return None

    def toInt(s):
        if s:
            return int(s)
        else:
            return None
        
    def toFloat(s):
        if s:
            return float(s)
        else:
            return None

    parser = HXLReader(input)
    for row in parser:
        rule = HXLSchemaRule(row.get('#x_tag'))
        rule.minOccur = toInt(row.get('#x_minoccur_num'))
        rule.maxOccur = toInt(row.get('#x_maxoccur_num'))
        rule.dataType = parseType(row.get('#x_datatype'))
        rule.minValue = toFloat(row.get('#x_minvalue_num'))
        rule.maxValue = toFloat(row.get('#x_maxvalue_num'))
        rule.valuePattern = re.compile(row.get('#x_pattern'))
        rule.valueEnumeration = row.get('#x_enumeration').split('|')
        rule.caseSensitive = int(row.get('#x_casesensitive'))
        schema.rules.append(rule)

    return schema

# end



