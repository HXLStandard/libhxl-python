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


if sys.version_info[0] > 2:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse

import hxl.common

def schema(origin=None, callback=None):
    """
    Convenience method for reading a HXL schema.
    If passed an existing Schema, simply returns it.
    @param origin a HXL data provider, file object, array, or string (representing a URL or file name).
    """

    if not origin:
        path = os.path.join(os.path.dirname(__file__), 'hxl-default-schema.csv');
        with hxl.data(path, True) as source:
            return parse_schema(source, callback)

    if isinstance(origin, Schema):
        # it's already a HXL schema
        return origin

    else:
        # create a schema
        return parse_schema(hxl.data(origin), callback)


class HXLValidationException(hxl.common.HXLException):
    """Data structure to hold a HXL validation error."""

    def __init__(self, message, rule=None, value=None, row=None, column=None):
        """Construct a new exception."""
        super(HXLValidationException, self).__init__(message)
        self.rule = rule
        self.value = value
        self.row = row
        self.column = column

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


class SchemaRule(object):
    """Validation rule for a single HXL hashtag."""

    # allow datatypes (others ignored)
    DATATYPES = ['text', 'number', 'url', 'email', 'phone', 'date']

    def __init__(self, tag, min_occur=None, max_occur=None,
                 data_type=None, min_value=None, max_value=None,
                 regex=None, enum=None, case_sensitive=True,
                 callback=None, severity="error", description=None,
                 required=False):
        if type(tag) is hxl.TagPattern:
            self.tag_pattern = tag
        else:
            self.tag_pattern = hxl.TagPattern.parse(tag)
        self.min_occur = min_occur
        self.max_occur = max_occur
        if data_type is None or data_type in self.DATATYPES:
            self.data_type = data_type
        else:
            raise hxl.HXLException('Unknown data type: {}'.format(data_type))
        self.min_value = min_value
        self.max_value = max_value
        self.regex = regex
        self.enum = enum
        self.case_sensitive = case_sensitive
        self.callback = callback
        self.severity = severity
        self.description = description
        self.required = required

    def validate_columns(self, columns):
        """Test whether the columns are present to satisfy this rule."""

        result = True
        
        if self.required or (self.min_occur is not None and int(self.min_occur) > 0):
            number_seen = 0
            for column in columns:
                if self.tag_pattern.match(column):
                    number_seen += 1
            if (self.required and (number_seen < 1)) or (self.min_occur is not None and number_seen < int(self.min_occur)):
                if number_seen == 0:
                    self._report_error('column with this hashtag required but not found')
                else:
                    self._report_error('not enough columns with this hashtag (expected {} but found {})'.format(self.min_occur, number_seen))
                result = False
        
        return result

    def validate_row(self, row):
        """
        Apply the rule to an entire Row
        @param row the Row to validate
        @return True if all matching values in the row are valid
        """

        number_seen = 0
        result = True

        # Look up only the values that apply to this rule
        values = row.get_all(self.tag_pattern)
        if values:
            for column_number, value in enumerate(values):
                if not self.validate(value, row, row.columns[column_number]):
                    result = False
                if value:
                    number_seen += 1
        if self.required and number_seen < 1:
            result = self._report_error(
                'A value for {} was required.'.format(self.tag_pattern),
                row = row
                )
        if self.min_occur is not None and number_seen < self.min_occur:
            result = self._report_error(
                "Expected at least " + str(self.min_occur) + " instance(s) but found " + str(number_seen),
                row = row
                )
        if self.max_occur is not None and number_seen > self.max_occur:
            result = self._report_error(
                "Expected at most " + str(self.max_occur) + " instance(s) but found " + str(number_seen),
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
        if self.data_type == 'number':
            if not hxl.common.is_number(value):
                return self._report_error("Expected a number", value, row, column)
        elif self.data_type == 'url':
            pieces = urlparse(value)
            if not (pieces.scheme and pieces.netloc):
                return self._report_error("Expected a URL", value, row, column)
        elif self.data_type == 'email':
            if not re.match(r'^[^@]+@[^@]+$', value):
                return self._report_error("Expected an email address", value, row, column)
        elif self.data_type == 'phone':
            if not re.match(r'^\+?[0-9xX()\s-]{5,}$', value):
                return self._report_error("Expected a phone number", value, row, column)
        elif self.data_type == 'date':
            if not re.match(r'^\d\d\d\d(?:-[01]\d(?:-[0-3]\d)?)?$', value):
                return self._report_error("Expected an ISO date (YYYY, YYYY-MM, or YYYY-MM-DD)", value, row, column)
        
        return True

    def _test_range(self, value, row, column):
        """Test against a numeric range (if specified)."""
        result = True
        try:
            if self.min_value is not None:
                if float(value) < float(self.min_value):
                    result = self._report_error("Value is less than " + str(self.min_value), value, row, column)
            if self.max_value is not None:
                if float(value) > float(self.max_value):
                    result = self._report_error("Value is great than " + str(self.max_value), value, row, column)
        except ValueError:
            result = False
        return result

    def _test_pattern(self, value, row, column):
        """Test against a regular expression pattern (if specified)."""
        if self.regex:
            flags = 0
            if self.case_sensitive:
                flags = re.IGNORECASE
            if not re.match(self.regex, value, flags):
                self._report_error("Failed to match pattern " + str(self.regex), value, row, column)
                return False
        return True

    def _test_enumeration(self, value, row, column):
        """Test against an enumerated set of values (if specified)."""
        if self.enum is not None:
            if self.case_sensitive:
                if value not in self.enum:
                    if len(self.enum) <= 7:
                        return self._report_error("Must be one of " + str(self.enum), value, row, column)
                    else:
                        return self._report_error("Not in allowed values", value, row, column)
            else:
                if value.upper() not in [item.upper() for item in self.enum]:
                    if len(self.enum) <= 7:
                        return self._report_error("Must be one of " + str(self.enum) + " (case-insensitive)", value, row, column)
                    else:
                        return self._report_error("Not in allowed values", value, row, column)
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
        self.callback = callback
        self.impossible_rules = {}

    # TODO add support for validating columns against rules, too
    # this is where to mention impossible conditions, or columns
    # without rules

    def validate(self, source):
        result = True
        if not self.validate_columns(source.columns):
            result = False
        for row in source:
            if not self.validate_row(row):
                result = False
        return result

    def validate_columns(self, columns):
        result = True
        for rule in self.rules:
            old_callback = rule.callback
            if self.callback:
                rule.callback = self.callback
            if not rule.validate_columns(columns):
                result = False
                self.impossible_rules[rule] = True
            rule.callback = old_callback
        return result

    def validate_row(self, row):
        result = True
        for rule in self.rules:
            if not rule in self.impossible_rules:
                old_callback = rule.callback
                if self.callback:
                    rule.callback = self.callback
                if not rule.validate_row(row):
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

def parse_schema(source, callback):
    """
    Load a HXL schema from the provided input stream, or load default schema.
    @param source HXL data source for the scheme (e.g. a HXLReader or filter)
    @param callback a callback function for reporting errors (receives a HXLValidationException)
    """

    schema = Schema(callback=callback)

    def parse_type(type):
        if type:
            type = type.lower()
            type = re.sub(r'[^a-z_-]', '', type) # normalise
        if type in SchemaRule.DATATYPES:
            return type
        else:
            return None

    def to_int(s):
        if s:
            return int(s)
        else:
            return None
        
    def to_float(s):
        if s:
            return float(s)
        else:
            return None

    def to_boolean(s):
        if not s or s.lower() in ['0', 'n', 'no', 'f', 'false']:
            return False
        elif s.lower() in ['y', 'yes', 't', 'true']:
            return True
        else:
            raise hxl.HXLException('Unrecognised true/false value: {}'.format(s))


    def to_regex(s):
        if s:
            return re.compile(s)
        else:
            return None

    for row in source:
        tag = row.get('#valid_tag')
        if tag:
            rule = SchemaRule(tag)
            rule.min_occur = to_int(row.get('#valid_required+min'))
            rule.max_occur = to_int(row.get('#valid_required+max'))
            rule.data_type = parse_type(row.get('#valid_datatype'))
            rule.min_value = to_float(row.get('#valid_value+min'))
            rule.max_value = to_float(row.get('#valid_value+max'))
            rule.regex = to_regex(row.get('#valid_value+regex'))
            rule.required = to_boolean(row.get('#valid_required-min-max'))
            rule.severity = row.get('#valid_severity') or 'error'
            rule.description = row.get('#description')
            
            rule.case_sensitive = to_boolean(row.get('#valid_value+case'))

            # Determine allowed values
            if row.get('#valid_value+list'):
                rule.enum = set(re.split(r'\s*\|\s*', row.get('#valid_value+list')))
            elif row.get('#valid_value+url'):
                value_source = hxl.data(row.get('#valid_value+url'), True)
                rule.enum = set(value_source.get_value_set(row.get('#valid_value+target_tag')))

            schema.rules.append(rule)

    return schema

# end
