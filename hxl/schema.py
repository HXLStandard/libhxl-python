"""
Validation code for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import re
import os
from copy import copy
from email.utils import parseaddr

from hxl import HXLException
from hxl.model import TagPattern
from hxl.io import StreamInput, HXLReader
from hxl.taxonomy import readTaxonomy

if sys.version_info[0] > 2:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


class HXLValidationException(HXLException):
    """
    Data structure to hold a HXL validation error.
    """

    def __init__(self, message, rule=None, value=None, row=None, column=None, severity="error"):
        """Construct a new exception."""
        super(HXLValidationException, self).__init__(message)
        self.rule = rule
        self.value = value
        self.row = row
        self.column = column
        self.severity = severity

    def __str__(self):
        """Get a string rendition of this error."""
        s = ''

        if self.rule.tag_pattern:
            if self.value:
                s += '{}={} '.format(str(self.rule.tag_pattern), str(self.value))
            else:
                s += '{} '.format(str(self.rule.tag_pattern))

        if self.message:
            s += '- {}'.format(self.message)

        return s



        return s
        
        value = "<no value>"
        if self.value:
            value = str(re.sub('\s+', ' ', self.value))
            value = '"' + value[:10] + (value[10:] and '...') + '"'
        source_row_number = "?"
        if self.row:
            source_row_number = str(self.row.source_row_number + 1)
        source_column_number = "?"
        if self.column:
            source_column_number = str(self.column.source_column_number + 1)

        return "E " + "(" + source_row_number + "," + source_column_number + ") " + str(self.rule.tag_pattern) + " " + value + ": " + self.message

class SchemaRule(object):
    """
    Validation rule for a single HXL hashtag.
    """

    # allow datatypes (others ignored)
    DATATYPES = ['text', 'number', 'url', 'email', 'phone', 'date']

    def __init__(self, tag, minOccur=None, maxOccur=None, dataType=None, minValue=None, maxValue=None,
                 valuePattern=None, valueEnumeration=None, caseSensitive=True, taxonomy=None, taxonomyLevel=None,
                 callback=None, severity="error", description=None, required=False):
        if type(tag) is TagPattern:
            self.tag_pattern = tag
        else:
            self.tag_pattern = TagPattern.parse(tag)
        self.minOccur = minOccur
        self.maxOccur = maxOccur
        if dataType is None or dataType in self.DATATYPES:
            self.dataType = dataType
        else:
            raise HXLException('Unknown data type: {}'.format(dataType))
        self.minValue = minValue
        self.maxValue = maxValue
        self.valuePattern = valuePattern
        self.valueEnumeration = valueEnumeration
        self.caseSensitive = caseSensitive
        self.taxonomy = taxonomy
        self.taxonomyLevel = taxonomyLevel
        self.callback = callback
        self.severity = severity
        self.description = description
        self.required = required

    def validateColumns(self, columns):
        """
        Test whether the columns are present to satisfy this rule.
        """

        result = True
        
        if self.minOccur > 0:
            number_seen = 0
            for column in columns:
                if self.tag_pattern.match(column):
                    number_seen += 1
            if (self.required and number_seen < 1) or (number_seen < self.minOccur):
                if number_seen == 0:
                    self._report_error('column with this hashtag required but not found')
                else:
                    self._report_error('not enough columns with this hashtag (expected {} but found {})'.format(self.minOccur, number_seen))
                result = False
        
        return result

    def validateRow(self, row):
        """
        Apply the rule to an entire Row
        @param row the Row to validate
        @return True if all matching values in the row are valid
        """

        numberSeen = 0
        result = True

        # Look up only the values that apply to this rule
        values = row.getAll(self.tag_pattern)
        if values:
            for column_number, value in enumerate(values):
                if not self.validate(value, row, row.columns[column_number]):
                    result = False
                if value:
                    numberSeen += 1
        if self.required and numberSeen < 1:
            result = self._report_error(
                'A value for {} was required.'.format(self.tag_pattern),
                row = row
                )
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
        @param row (optional) the Row being validated
        @param column (optional) the Column being validated
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
                    column = column,
                    severity = self.severity
                    )
                )
        return False

    def _test_type(self, value, row, column):
        """Check the datatype."""
        if self.dataType == 'number':
            try:
                float(value)
                return True
            except ValueError:
                return self._report_error("Expected a number", value, row, column)
        elif self.dataType == 'url':
            pieces = urlparse(value)
            if not (pieces.scheme and pieces.netloc):
                return self._report_error("Expected a URL", value, row, column)
        elif self.dataType == 'email':
            if not re.match(r'^[^@]+@[^@]+$', value):
                return self._report_error("Expected an email address", value, row, column)
        elif self.dataType == 'phone':
            if not re.match(r'^\+?[0-9xX()\s-]{5,}$', value):
                return self._report_error("Expected a phone number", value, row, column)
        elif self.dataType == 'date':
            if not re.match(r'^\d\d\d\d(?:-[01]\d(?:-[0-3]\d)?)?$', value):
                return self._report_error("Expected an ISO date (YYYY, YYYY-MM, or YYYY-MM-DD)", value, row, column)
        
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
        return "<HXL schema rule: " + str(self.tag_pattern) + ">"
                
class Schema(object):
    """
    Schema against which to validate a HXL document.
    """

    def __init__(self, rules=[], callback=None):
        self.rules = copy(rules)
        if callback is None:
            self.callback = self.showError
        else:
            self.callback = callback

        self.impossible_rules = {}

    def showError(self, error):
        print >> sys.stderr, error
        

    # TODO add support for validating columns against rules, too
    # this is where to mention impossible conditions, or columns
    # without rules

    def validate(self, parser):
        result = True

        self.validateColumns(parser.columns)

        for row in parser:
            if not self.validateRow(row):
                result = False
        return result

    def validateColumns(self, columns):
        for rule in self.rules:
            old_callback = rule.callback
            if self.callback:
                rule.callback = self.callback
            if not rule.validateColumns(columns):
                self.impossible_rules[rule] = True
            rule.callback = old_callback

    def validateRow(self, row):
        result = True
        for rule in self.rules:
            if not rule in self.impossible_rules:
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

def readSchema(source=None, baseDir=None):
    if source is None:
        path = os.path.join(os.path.dirname(__file__), 'hxl-default-schema.csv');
        with open(path, 'r') as input:
            return _read_hxl_schema(HXLReader(StreamInput(input)), baseDir)
    else:
        return _read_hxl_schema(source, baseDir)

def _read_hxl_schema(source, baseDir):
    """
    Load a HXL schema from the provided input stream, or load default schema.
    @param source HXL data source for the scheme (e.g. a HXLReader or filter)
    """

    schema = Schema()

    def parseType(type):
        type = type.lower()
        type = re.sub(r'[^a-z_-]', '', type) # normalise
        if type in SchemaRule.DATATYPES:
            return type
        else:
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

    def toBoolean(s):
        if not s or s.lower() in ['0', 'n', 'no', 'f', 'false']:
            return False
        elif s.lower() in ['y', 'yes', 't', 'true']:
            return True
        else:
            raise HXLException('Unrecognised true/false value: {}'.format(s))


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
            with open(path, 'r') as input:
                return readTaxonomy(HXLReader(StreamInput(input)))
        else:
            return None

    for row in source:
        rule = SchemaRule(row.get('#valid_tag'))
        rule.minOccur = toInt(row.get('#valid_required+min'))
        rule.maxOccur = toInt(row.get('#valid_required+max'))
        rule.dataType = parseType(row.get('#valid_datatype'))
        rule.minValue = toFloat(row.get('#valid_value+min'))
        rule.maxValue = toFloat(row.get('#valid_value+max'))
        rule.valuePattern = toRegex(row.get('#valid_value+regex'))
        rule.taxonomy = toTaxonomy(row.get('#x_taxonomy'))
        rule.taxonomyLevel = toInt(row.get('#x_taxonomylevel_num'))
        rule.required = toBoolean(row.get('#valid_required-min-max'))
        rule.severity = row.get('#valid_severity') or 'error'
        rule.description = row.get('#description')
        s = row.get('#valid_value+list')
        if s:
            rule.valueEnumeration = re.split(r'\s*\|\s*', s)
        rule.caseSensitive = toBoolean(row.get('#valid_value+case'))
        schema.rules.append(rule)

    return schema

# end
