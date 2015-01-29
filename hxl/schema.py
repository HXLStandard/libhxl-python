"""
Validation code for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import re
import os
from copy import copy
from email.utils import parseaddr
from .parser import HXLReader
from .taxonomy import readTaxonomy

if sys.version_info[0] > 2:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


class HXLValidationException(Exception):
    """
    Data structure to hold a HXL validation error.
    """

    def __init__(self, message, rule = None, value = None, row = None, column = None):
        self.message = message
        self.rule = rule
        self.value = value
        self.row = row
        self.column = column

    def __str__(self):
        value = "<no value>"
        if self.value:
            value = str(re.sub('\s+', ' ', self.value))
            value = '"' + value[:10] + (value[10:] and '...') + '"'
        sourceRowNumber = "?"
        if self.row:
            sourceRowNumber = str(self.row.sourceRowNumber + 1)
        sourceColumnNumber = "?"
        if self.column:
            sourceColumnNumber = str(self.column.sourceColumnNumber + 1)
        return "E " + "(" + sourceRowNumber + "," + sourceColumnNumber + ") " + self.rule.hxlTag + " " + value + ": " + self.message

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
                 valuePattern=None, valueEnumeration=None, caseSensitive=True, taxonomy=None, taxonomyLevel=None,
                 callback=None):
        self.hxlTag = hxlTag
        self.minOccur = minOccur
        self.maxOccur = maxOccur
        self.dataType = dataType
        self.minValue = minValue
        self.maxValue = maxValue
        self.valuePattern = valuePattern
        self.valueEnumeration = valueEnumeration
        self.caseSensitive = caseSensitive
        self.taxonomy = taxonomy
        self.taxonomyLevel = taxonomyLevel
        self.callback = callback


    def validateRow(self, row):
        """
        Apply the rule to an entire HXLRow
        @param row the HXLRow to validate
        @return True if all matching values in the row are valid
        """

        numberSeen = 0
        result = True

        # Look up only the values that apply to this rule
        values = row.getAll(self.hxlTag)
        if values:
            for columnNumber, value in enumerate(values):
                if not self.validate(value, row, row.columns[columnNumber]):
                    result = False
                if value:
                    numberSeen += 1
        if self.minOccur is not None and numberSeen < self.minOccur:
            result = self._report_error(
                "Expected at least " + str(self.minOccur) + " instance(s) but found " + str(numberSeen),
                row = row
                )
        if self.maxOccur is not None and numberSeen > self.maxOccur:
            result = self._report_error(
                "Expected at most " + str(self.maxOccur) + " instance(s) but found " + str(numberSeen),
                row = row
                )
        return result


    def validate(self, value, row = None, column = None):
        """
        Apply the rule to a single value.
        @param value the value to validate
        @param row (optional) the HXLRow being validated
        @param column (optional) the HXLColumn being validated
        @return True if valid; false otherwise
        """

        if value is None or value == '':
            return True

        result = True
        if not self._test_type(value, row, column):
            result = False
        if not self._test_range(value, row, column):
            result = False
        if not self._test_pattern(value, row, column):
            result = False
        if not self._test_enumeration(value, row, column):
            result = False
        if not self._test_taxonomy(value, row, column):
            result = False

        return result

    def _report_error(self, message, value=None, row=None, column=None):
        """Report an error to the callback."""
        if self.callback != None:
            self.callback(
                HXLValidationException(
                    message=message,
                    rule=self,
                    value = value,
                    row = row,
                    column = column
                    )
                )
        return False

    def _test_type(self, value, row, column):
        """Check the datatype."""
        if self.dataType == self.TYPE_NUMBER:
            try:
                float(value)
                return True
            except ValueError:
                return self._report_error("Expected a number", value, row, column)
        elif self.dataType == self.TYPE_URL:
            pieces = urlparse(value)
            if not (pieces.scheme and pieces.netloc):
                return self._report_error("Expected a URL", value, row, column)
        elif self.dataType == self.TYPE_EMAIL:
            if not re.match('^[^@]+@[^@]+$', value):
                return self._report_error("Expected an email address", value, row, column)
        elif self.dataType == self.TYPE_PHONE:
            if not re.match('^\+?[0-9xX()\s-]{5,}$', value):
                return self._report_error("Expected a phone number", value, row, column)
        
        return True

    def _test_range(self, value, row, column):
        """Test against a numeric range (if specified)."""
        result = True
        try:
            if self.minValue is not None:
                if float(value) < float(self.minValue):
                    result = self._report_error("Value is less than " + str(self.minValue), value, row, column)
            if self.maxValue is not None:
                if float(value) > float(self.maxValue):
                    result = self._report_error("Value is great than " + str(self.maxValue), value, row, column)
        except ValueError:
            result = False
        return result

    def _test_pattern(self, value, row, column):
        """Test against a regular expression pattern (if specified)."""
        if self.valuePattern:
            flags = 0
            if self.caseSensitive:
                flags = re.IGNORECASE
            if not re.match(self.valuePattern, value, flags):
                self._report_error("Failed to match pattern " + str(self.valuePattern), value, row, column)
                return False
        return True

    def _test_enumeration(self, value, row, column):
        """Test against an enumerated set of values (if specified)."""
        if self.valueEnumeration is not None:
            if self.caseSensitive:
                if value not in self.valueEnumeration:
                    return self._report_error("Must be one of " + str(self.valueEnumeration), value, row, column)
            else:
                if value.upper() not in map(lambda item: item.upper(), self.valueEnumeration):
                    return self._report_error("Must be one of " + str(self.valueEnumeration) + " (case-insensitive)", value, row, column)
        return True

    def _test_taxonomy(self, value, row, column):
        """Test against a taxonomy (if specified)."""
        if self.taxonomy is not None:
            if not self.taxonomy.contains(value, self.taxonomyLevel):
                if self.taxonomyLevel is None:
                    return self._report_error("Not in taxonomy", value, row, column)
                else:
                    return self._report_error("Not in taxonomy at level " + str(self.taxonomyLevel), value, row, column)
        return True

    def __str__(self):
        """String representation of a rule (for debugging)"""
        return "<HXL schema rule: " + self.hxlTag + ">"
                
class HXLSchema(object):
    """
    Schema against which to validate a HXL document.
    """

    def __init__(self, rules=[], callback=None):
        self.rules = copy(rules)
        if callback is None:
            self.callback = self.showError
        else:
            self.callback = callback

    def showError(self, error):
        print >> sys.stderr, error
        

    # TODO add support for validating columns against rules, too
    # this is where to mention impossible conditions, or columns
    # without rules

    def validate(self, parser):
        result = True
        for row in parser:
            if not self.validateRow(row):
                result = False
        return result

    def validateRow(self, row):
        result = True
        for rule in self.rules:
            old_callback = rule.callback
            if self.callback:
                rule.callback = self.callback
            if not rule.validateRow(row):
                result = False
            rule.callback = old_callback
        return result

    def __str__(self):
        """String representation of a schema (for debugging)"""
        s = "<HXL schema\n"
        for rule in self.rules:
            s += "  " + str(rule) + "\n"
        s += ">"
        return s

def readHXLSchema(source=None, baseDir=None):
    """
    Load a HXL schema from the provided input stream, or load default schema.
    @param source HXL data source for the scheme (e.g. a HXLReader or filter)
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

    def toRegex(s):
        if s:
            return re.compile(s)
        else:
            return None

    def toTaxonomy(s):
        if s:
            if baseDir:
                path = os.path.join(baseDir, s)
            else:
                path = s
            return readTaxonomy(HXLReader(open(path, 'r')))
        else:
            return None

    if source is None:
        path = os.path.join(os.path.dirname(__file__), 'hxl-default-schema.csv');
        input = open(path, 'r')
        source = HXLReader(input)

    for row in source:
        rule = HXLSchemaRule(row.get('#x_tag'))
        rule.minOccur = toInt(row.get('#x_minoccur_num'))
        rule.maxOccur = toInt(row.get('#x_maxoccur_num'))
        rule.dataType = parseType(row.get('#x_datatype'))
        rule.minValue = toFloat(row.get('#x_minvalue_num'))
        rule.maxValue = toFloat(row.get('#x_maxvalue_num'))
        rule.valuePattern = toRegex(row.get('#x_pattern'))
        rule.taxonomy = toTaxonomy(row.get('#x_taxonomy'))
        rule.taxonomyLevel = toInt(row.get('#x_taxonomylevel_num'))
        s = row.get('#x_enumeration')
        if s:
            rule.valueEnumeration = s.split('|')
        rule.caseSensitive = toInt(row.get('#x_casesensitive'))
        schema.rules.append(rule)

    return schema

# end
