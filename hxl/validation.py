"""
Validation code for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import hxl
import copy, logging, os, re, sys, urllib

logger = logging.getLogger(__name__)


class HXLValidationException(hxl.HXLException):
    """Data structure to hold a HXL validation error."""

    def __init__(self, message, rule=None, value=None, row=None, column=None, raw_value=None, suggested_value=None):
        """Construct a new exception."""
        super(HXLValidationException, self).__init__(message)
        self.rule = rule
        self.value = value
        self.row = row
        self.column = column
        self.raw_value = raw_value
        self.suggested_value = suggested_value

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
                 regex=None, enum=None, case_sensitive=False,
                 callback=None, severity="error", description=None,
                 required=False, unique=False, unique_key=None, correlation_key=None,
                 consistent_datatypes = False, check_whitespace=False):
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
        self.check_whitespace = check_whitespace
        self.consistent_datatypes = consistent_datatypes
        self.callback = callback
        self.severity = severity
        self.description = description
        self.required = required
        self.unique = unique
        self.unique_key = unique_key
        self.correlation_key = correlation_key

        self.value_url = None # set later, if needed
        """(Failed) URL for reading an external taxonomy"""
        
        self.value_url_error = None # set later, if needed
        """Error from trying to load an external taxonomy"""

        self._initialised = False
        self._enum_map = None
        
        self._unique_value_map = {}
        self._unique_key_map = {}
        self._correlation_map = {}
        self._suggestion_map = {}
        self._consistency_map = {}

    def init(self):
        """Initialisation method
        Call after all values have been set, but before use
        """

        if self.enum:
            self._enum_map = {}
            for value in self.enum:
                if self.case_sensitive:
                    self._enum_map[hxl.datatypes.normalise_space(value)] = value
                else:
                    self._enum_map[hxl.datatypes.normalise_string(value)] = value

        if self.unique_key:
            self.unique_key = hxl.model.TagPattern.parse_list(self.unique_key)
            self.unique_key.append(self.tag_pattern) # just in case
        if self.correlation_key:
            self.correlation_key = hxl.model.TagPattern.parse_list(self.correlation_key)

        self._initialised = True

    def _check_init(self):
        if not self._initialised:
            self.init()

    def finish(self):
        """Call at end of parse to get post-parse errors"""
        result = True
        if not self._finish_correlations():
            result = False
        if self.consistent_datatypes and (not self._finish_consistency()):
            result = False
        return result

    def _finish_correlations(self):
        """Check for correlation errors"""
        m = self._correlation_map # shortcut

        def sort_entries(e):
            """Sort entries by the number of locations in the second element"""
            return (len(e[1]), e[0],)

        result = True
        if m:
            #
            # Calculate the most-common value for each hashtag+key combo
            #
            expected_values = {}
            for hashtag in m:
                # collect hashtag+key+value+locations info into a map
                for value, keys in m[hashtag].items():
                    for key, locations in keys.items():
                        expected_values.setdefault(hashtag, {}).setdefault(key, {}).setdefault(value, 0)
                        expected_values[hashtag][key][value] += len(locations)
                # reduce the expected values to top value for each hashtag+key combo
                for key, values in expected_values[hashtag].items():
                    entries = sorted(values.items(), key=lambda e: e[1], reverse=True)
                    expected_values[hashtag][key] = entries[0][0] # this is the most-common value for the hashtag+key

            #
            # Process each matching hashtag/column found ...
            #
            for hashtag in m:
                # for each correlation key found ...
                for value, keys in m[hashtag].items():
                    # if there's more than one value found matching the correlation key, assume an error
                    if len(keys) > 1:
                        result = False

                        # get all the correlation key/location combinations, sorted with most-common first
                        key_locations = sorted(keys.items(), key=sort_entries, reverse=True)

                        # iterate through all but the most-common value for the key
                        for key, locations in key_locations[1:]:
                            # what value did we expect to find?
                            suggested_value = expected_values[hashtag][key]
                            if value == suggested_value:
                                suggested_value = None
                            for row, column in locations:
                                self._report_error(
                                    'wrong value for related column(s) ' + ', '.join([str(pattern) for pattern in self.correlation_key]),
                                    value=value,
                                    row=row,
                                    column=column,
                                    suggested_value=suggested_value
                                )
                    
        return result

    def _finish_consistency(self):
        result = True
        m = self._consistency_map
        if not m:
            return
        for hashtag in m:
            # each hashtag should be consistent
            types_found = sorted(m[hashtag].items(), key=lambda e: len(e[1]), reverse=True)
            if len(types_found) > 1:
                # We found more than one data type for the column
                result = False
                for type_data in types_found[1:]:
                    for entry in type_data[1]:
                        self._report_error(
                            'inconsistent datatype {} (expected {})'.format(type_data[0], types_found[0][0]),
                            row=entry[0],
                            column=entry[1],
                            value=entry[2]
                        )

        return result
                

    def validate_columns(self, columns):
        """Test whether the columns are present to satisfy this rule."""
        self._check_init()
        
        result = True

        # Did we fail to load an external URL?
        if self.value_url_error is not None:
            result = self._report_error(
                str("Error reading allowed values from {} ({})".format(self.value_url, str(self.value_url_error)))
            )

        # Are required columns present?
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
        self._check_init()

        # individual rules may change to False
        result = True

        #
        # Run cell-scope validations
        #

        for i, column in enumerate(row.columns):
            value = None
            if self.tag_pattern.match(column) and i < len(row.values):
                value = row.values[i]
                if not self.validate(value, row, column):
                    result = False

        #
        # Run row-scope validations if needed
        # #valid_required, #valid_required+min, #valid_required+max
        #
        if self.required or self.min_occur is not None or self.max_occur is not None:

            non_empty_value_count = 0
            first_empty_column = None
            last_non_empty_column = None

            for i, column in enumerate(row.columns):
                if self.tag_pattern.match(column):
                    if i >= len(row.values) or hxl.datatypes.is_empty(row.values[i]):
                        if first_empty_column is None:
                            first_empty_column = column
                    else:
                        non_empty_value_count += 1
                        last_non_empty_column = column

            if self.required and (non_empty_value_count < 1):
                result = self._report_error(
                    "A value is required",
                    row=row,
                    column=first_empty_column
                )

            if (self.min_occur is not None) and (non_empty_value_count < self.min_occur):
                result = self._report_error(
                    "Minimum of {} non-empty values required in the row".format(self.min_occur),
                    row=row,
                    column=first_empty_column
                )

            if (self.max_occur is not None) and (non_empty_value_count > self.max_occur):
                result = self._report_error(
                    "Maximum of {} non-empty values allowed in the row".format(self.max_occur),
                    row=row,
                    column=last_non_empty_column
                )

        #
        # Run dataset-scope validations
        #

        if self.unique_key is not None:
            key = row.key(self.unique_key)
            if self._unique_key_map.get(key):
                result = self._report_error(
                    "Duplicate row according to tag patterns " + str(self.unique_key),
                    row=row
                )
            else:
                self._unique_key_map[key] = True

        # track correlations here, then report at end of parse
        if self.correlation_key is not None:
            key = row.key(self.correlation_key) # make a tuple of other values involved
            for column_number, value in enumerate(row.values):
                if self.tag_pattern.match(row.columns[column_number]):
                    if hxl.datatypes.is_empty(value):
                        continue
                    hashtag = row.columns[column_number].display_tag
                    value = hxl.datatypes.normalise(value)
                    location = (row, row.columns[column_number],)
                    self._correlation_map.setdefault(hashtag, {}).setdefault(value, {}).setdefault(key, []).append(location)
                    break

        # track datatypes here, then report at end of parse
        if self.consistent_datatypes:
            for column_number, value in enumerate(row.values):
                column = row.columns[column_number]
                if self.tag_pattern.match(column):
                    if hxl.datatypes.is_empty(value):
                        continue
                    hashtag = row.columns[column_number].display_tag
                    type = hxl.datatypes.typeof(value, column)
                    entry = (row, column, value)
                    self._consistency_map.setdefault(hashtag, {}).setdefault(type, []).append(entry)

        return result


    def validate(self, raw_value, row = None, column = None):
        """
        Apply the rule to a single value.
        @param value the value to validate
        @param row (optional) the Row being validated
        @param column (optional) the Column being validated
        @return True if valid; false otherwise
        """
        self._check_init()

        if hxl.datatypes.is_empty(raw_value):
            return True

        if self.case_sensitive:
            value = hxl.datatypes.normalise_space(raw_value)
        else:
            value = hxl.datatypes.normalise_string(raw_value)

        result = True
        if not self._test_whitespace(value, row, column, raw_value=raw_value):
            result = False
        if not self._test_type(value, row, column, raw_value=raw_value):
            result = False
        if not self._test_range(value, row, column, raw_value=raw_value):
            result = False
        if not self._test_pattern(value, row, column, raw_value=raw_value):
            result = False
        if not self._test_enumeration(value, row, column, raw_value=raw_value):
            result = False
        if not self._test_unique(value, row, column, raw_value=raw_value):
            result = False

        return result

    def _report_error(self, message, value=None, row=None, column=None, raw_value=None, suggested_value=None):
        """Report an error to the callback."""
        if self.callback != None:
            e = HXLValidationException(
                message=message,
                rule=self,
                value = value,
                row = row,
                column = column,
                raw_value = None,
                suggested_value = suggested_value
            )
            self.callback(e)
        return False

    WHITESPACE_PATTERN = r'^(\s+.*|.*(\s\s|[\t\r\n]).*|\s+)$'
    """Pattern for irregular whitespace"""

    def _test_whitespace(self, value, row, column, raw_value):
        """Check for irregular whitespace
        Expect no leading or trailing whitespace, and only single spaces internally.
        Triggered by the check_whitespace flag
        """
        if self.check_whitespace and re.match(self.WHITESPACE_PATTERN, str(raw_value)):
            return self._report_error(
                message="Found extra whitespace",
                value=raw_value,
                row=row,
                column=column,
                suggested_value=hxl.datatypes.normalise_space(raw_value)
            )
        else:
            return True

    def _test_type(self, value, row, column, raw_value):
        """Check the datatype."""
        if self.data_type == 'number':
            if not hxl.datatypes.is_number(value):
                return self._report_error("Expected a number", raw_value, row, column)
        elif self.data_type == 'url':
            pieces = urllib.parse.urlparse(value)
            if not (pieces.scheme and pieces.netloc):
                return self._report_error("Expected a URL", raw_value, row, column)
        elif self.data_type == 'email':
            if not re.match(r'^[^@]+@[^@]+$', value):
                return self._report_error("Expected an email address", raw_value, row, column)
        elif self.data_type == 'phone':
            if not re.match(r'^\+?[0-9xX()\s-]{5,}$', value):
                return self._report_error("Expected a phone number", raw_value, row, column)
        elif self.data_type == 'date':
            if not hxl.datatypes.is_date(value):
                return self._report_error("Expected a date of some sort", raw_value, row, column)
        
        return True

    def _test_range(self, value, row, column, raw_value):
        """Test against a numeric range (if specified)."""
        result = True
        try:
            if self.min_value is not None:
                if float(value) < float(self.min_value):
                    result = self._report_error("Value is less than " + str(self.min_value), raw_value, row, column)
            if self.max_value is not None:
                if float(value) > float(self.max_value):
                    result = self._report_error("Value is great than " + str(self.max_value), raw_value, row, column)
        except ValueError:
            result = False
        return result

    def _test_pattern(self, value, row, column, raw_value):
        """Test against a regular expression pattern (if specified)."""
        if self.regex:
            flags = 0
            if self.case_sensitive:
                flags = re.IGNORECASE
            if not re.match(self.regex, value, flags):
                self._report_error("Failed to match pattern " + str(self.regex), value, row, column)
                return False
        return True

    def _test_enumeration(self, value, row, column, raw_value):
        """Test against an enumerated set of values (if specified)."""
        if self._enum_map is not None:
            if value not in self._enum_map:

                # do we already have a cached suggested value?
                suggested_value = self._suggestion_map.get(value)
                if suggested_value is None:
                    suggested_value = find_closest_match(value, self._enum_map)
                    self._suggestion_map[value] = suggested_value

                # do we have a raw version of the value?
                if suggested_value and suggested_value in self._enum_map:
                    suggested_value = self._enum_map[suggested_value]

                # if it's a short list, include it in the error message
                if len(self.enum) <= 7:
                    message = "Must be one of " + str(self.enum)
                else:
                    message = "Not in allowed values"

                # generate the error report
                return self._report_error(
                    message,
                    value=raw_value,
                    row=row,
                    column=column,
                    suggested_value=suggested_value
                )

        return True

    def _test_unique(self, value, row, column, raw_value):
        """Report if a value is not unique for a specific hashtag"""
        if self.unique:
            normalised_value = hxl.datatypes.normalise(value, column)
            if self._unique_value_map.get(normalised_value):
                return self._report_error(
                    "Found duplicate value",
                    value=raw_value,
                    row=row,
                    column=column
                )
            else:
                self._unique_value_map[normalised_value] = True
        return True

    def __str__(self):
        """String representation of a rule (for debugging)"""
        return "<HXL schema rule: " + str(self.tag_pattern) + ">"
                

class Schema(object):
    """Schema against which to validate a HXL document.
    Consists of a sequence of L{SchemaRule} objects to apply.
    """

    def __init__(self, rules=[], callback=None):
        self.rules = copy.copy(rules)
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
        if not self.validate_dataset():
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

    def validate_dataset(self):
        result = True
        for rule in self.rules:
            old_callback = rule.callback
            if self.callback:
                rule.callback = self.callback
            if not rule.finish():
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


    @staticmethod
    def parse(source=None, callback=None):
        """ Load a HXL schema from the provided input stream, or load default schema.
        @param source: HXL data source for the scheme (e.g. a HXLReader or filter); defaults to the built-in schema
        @param callback: a callback function for reporting errors (receives a HXLValidationException)
        """

        # Catch special cases

        if source is None:
            # Use the built-in default schema and recurse
            path = os.path.join(os.path.dirname(__file__), 'hxl-default-schema.csv');
            with hxl.data(path, True) as source:
                return Schema.parse(source, callback)

        if isinstance(source, Schema):
            # Already a schema; set the callback and return it
            source.callback = callback
            return source

        if not isinstance(source, hxl.model.Dataset):
            # Not already a dataset, so wrap it and recurse
            with hxl.data(source) as source:
                return Schema.parse(source, callback)

        # Main parsing

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
                rule.data_type = parse_type(row.get('#valid_datatype-consistent'))
                rule.check_whitespace = to_boolean(row.get('#valid_value+whitespace'))
                rule.min_value = to_float(row.get('#valid_value+min'))
                rule.max_value = to_float(row.get('#valid_value+max'))
                rule.regex = to_regex(row.get('#valid_value+regex'))
                rule.required = to_boolean(row.get('#valid_required-min-max'))
                rule.unique = to_boolean(row.get('#valid_unique-key'))
                rule.unique_key = row.get('#valid_unique+key')
                rule.correlation_key = row.get('#valid_correlation')
                rule.severity = row.get('#valid_severity') or 'error'
                rule.description = row.get('#description')

                rule.case_sensitive = to_boolean(row.get('#valid_value+case'))
                rule.consistent_datatypes = to_boolean(row.get('#valid_datatype+consistent'))

                # Determine allowed values
                if row.get('#valid_value+list'):
                    rule.enum = re.split(r'\s*\|\s*', row.get('#valid_value+list'))
                elif row.get('#valid_value+url'):
                    try:
                        value_url = row.get('#valid_value+url')
                        value_source = hxl.data(value_url)
                        target_tag = row.get(
                            '#valid_value+target_tag',
                            default=row.get('#valid_tag')
                        )
                        rule.enum = set(value_source.get_value_set(row.get('#valid_value+target_tag')))
                    except Exception as value_url_error:
                        rule.value_url = value_url
                        rule.value_url_error = value_url_error

                rule.init()

                schema.rules.append(rule)

        return schema

#
# Internal helper functions
#

def find_closest_match(s, allowed_values):
    """Find the closest match for a value from a list.
    This is not super efficient right now; look at
    https://en.wikipedia.org/wiki/Edit_distance for better algorithms.
    Uses a cutoff of len(s)/2 for candidate matches, and if two matches have the same 
    edit distance, prefers the one with the longer common prefix.
    @param s: the misspelled string to check
    @param allowed_values: a list of allowed values
    @return: the best match, or None if there was no candidate
    """
    best_match = None
    max_distance = len(s) / 2
    for value in allowed_values:
        distance = get_edit_distance(s, value)
        if (best_match is not None and distance > best_match[1]) or (distance > max_distance):
            continue
        prefix_len = get_common_prefix_len(s, value)
        if (best_match is None) or (distance < best_match[1]) or (distance == best_match[1] and prefix_len > best_match[2]):
            best_match = (value, distance, prefix_len,)
    if best_match is None:
        return None
    else:
        return best_match[0]

def get_common_prefix_len(s1, s2):
    """Return the longest common prefix of two strings
    Adopted from example in https://stackoverflow.com/questions/9114402/regexp-finding-longest-common-prefix-of-two-strings
    @param s1: the first string to compare
    @param s2: the second string to compare
    @returns: the length of the longest common prefix of the two strings
    """
    i = 0
    for i, (x, y) in enumerate(zip(s1, s2)):
        if x != y:
            break
    return i

def get_edit_distance(s1, s2):
    """Calculate the Levenshtein distance between two normalised strings
    Adopted from example in https://stackoverflow.com/questions/2460177/edit-distance-in-python
    See https://en.wikipedia.org/wiki/Edit_distance
    @param s1: the first string to compare
    @param s2: the second string to compare
    @returns: an integer giving the edit distance
    """
    if len(s1) > len(s2):
        s1, s2 = s2, s1
    distances = range(len(s1) + 1)
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
        distances = distances_
    return distances[-1]


#
# Exported functions
#
def schema(source=None, callback=None):
    """Convenience method for making a schema
    Imported into __init__, and usually called as hxl.schema(source, callback).
    The callback, if provided, will receive a HXLValidationException object for each error
    @param source: something that can be used as a HXL data source
    @param callback: the validation callback function to use
    """
    return Schema.parse(source, callback)


# end
