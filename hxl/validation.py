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

#
# Individual tests within a Schema Rule
#

class AbstractRuleTest(object):
    """Base class for a single test inside a validation rule.
    Safe assumptions for subclasses:
    - \I{start} gets called before parsing starts
    - \I{end} gets called after parsing ends
    - \I{validate_dataset} gets called once, before any calls to \I{validate_row}
    - \I{validate_row} gets called once for every row, before any calls to \I{validate_cell}
    - \I{validate_cell} gets called once for every non-empty maching column in the row
    - the columns for \I{validate_row} will always be the same between calls to init
    """

    def __init__(self, callback=None):
        """Set up a schema test.
        @param callback: a callback function to receive error reports
        """
        self.callback = callback

    @property
    def needs_cache(self):
        """Report whether this test requires a cached dataset.
        A cached dataset is one that can be processed more than
        once. It requires more memory and processing time, so return
        True only if absolutely necessary.
        @returns: True if the test requires a cached dataset.
        """
        return False

    def start(self):
        """Setup code to run before validating each dataset.
        This code should not report errors, since it hasn't seen the data yet.
        """
        return True

    def end(self):
        """Code to run after validating each dataset.
        Will report errors via the test's \I{callback}, if available.
        @raises HXLValidationException: for a validation
        @returns: True if there are no new validation errors
        """
        return True

    def validate_dataset(self, dataset, indices=None, tag_pattern=None):
        """Apply test at the dataset level
        Called before validate_row() or validate_value()
        Will report errors via the test's \I{callback}, if available.
        @param dataset: a hxl.model.Dataset object to validate
        @param indices: optional pre-compiled indices for columns matching tag_pattern
        @returns: True if there are no new validation errors.
        """
        return True

    def validate_row(self, row, indices=None, tag_pattern=None):
        """Apply test at the row level
        Called for each row before validate_cell() calls.
        Will report errors via the test's \I{callback}, if available.
        @param row: a hxl.model.Row object to validate
        @param indices: optional pre-compiled indices for columns matching tag_pattern
        @returns: True if there are no new validation errors.
        """
        return True

    def validate_cell(self, value, row, column):
        """Apply test at the cell level
        Called for each matching non-empty value.
        Will also report errors via the test's \I{callback}, if available.
        @param value: the non-empty value to validate
        @param row: a hxl.model.Row object for location
        @param column: a hxl.model.Column object for location
        @returns: True if there are no new validation errors.
        """
        return True

    def get_indices(self, indices, tag_pattern, columns):
        """Get a set of column indices by hook or by crook, based on what we have"""
        if indices is not None:
            return indices
        elif tag_pattern is not None:
            tag_pattern = hxl.model.TagPattern.parse(tag_pattern)
            return get_column_indices(tag_pattern, columns)
        else:
            raise HXLException("Internal error: rule test requires a tag pattern or a list of indices")

    def report_error(self, message, row=None, column=None, value=None, raw_value=None, suggested_value=None):
        """Report an error from this test, if there is a callback function available."""
        if self.callback:
            self.callback(HXLValidationException(
                message,
                value=value,
                row=row,
                column=column,
                raw_value=raw_value,
                suggested_value=suggested_value
            ))
        return False # for convenience


class RequiredTest(AbstractRuleTest):
    """Test min/max occurrence for #valid_required
    If the columns don't exist at all, report only a single error.
    Otherwise, report an error for each row where the test fails.
    """

    def __init__(self, min_occurs=None, max_occurs=None):
        """Constructor
        @param min: minimum occurrence required (or None)
        @param max: maximum occurrence allowed (or None)
        """
        super().__init__()
        self.min_occurs = min_occurs
        self.max_occurs = max_occurs
        self.test_rows = True

    def start(self):
        self.test_rows = True

    def validate_dataset(self, dataset, indices=None, tag_pattern=None):
        """Verify that we have enough matching columns to satisfy the test"""
        result = True
        indices = self.get_indices(indices, tag_pattern, dataset.columns)
        if self.min_occurs is not None and len(indices) < self.min_occurs:
            self.test_rows = False # no point testing individual rows
            result = self.report_error(
                "Expected at least {} matching column(s)".format(self.min_occurs)
            )
        return result

    def validate_row(self, row, indices=None, tag_pattern=None):
        """Check the number of non-empty occurrences in a row."""
        if not self.test_rows: # skip if there aren't enough columns
            return
        result = True
        indices = self.get_indices(indices, tag_pattern, row.columns)

        non_empty_count = 0
        first_empty_column = None
        last_nonempty_column = None

        # iterate through all values in matching columns
        for i in indices:
            if i >= len(row.values) or hxl.datatypes.is_empty(row.values[i]):
                if first_empty_column is None:
                    first_empty_column = row.columns[i]
            else:
                non_empty_count += 1
                last_nonempty_column = row.columns[i]

        if self.min_occurs is not None and non_empty_count < self.min_occurs:
            result = self.report_error(
                "Expected at least {} matching non-empty value(s)".format(self.min_occurs),
                row=row,
                column=first_empty_column
            )

        if self.max_occurs is not None and non_empty_count > self.max_occurs:
            result = self.report_error(
                "Expected at most {} matching non-empty value(s)".format(self.max_occurs),
                row=row,
                column=last_nonempty_column
            )

        return result

    
class DatatypeTest(AbstractRuleTest):
    """Test for #valid_datatype-consistent"""

    # allowed datatypes
    DATATYPES = ['text', 'number', 'url', 'email', 'phone', 'date']

    def __init__(self, datatype):
        super().__init__()
        datatype = hxl.datatypes.normalise_string(datatype)
        if datatype in DatatypeTest.DATATYPES:
            self.datatype = datatype
        else:
            raise hxl.HXLException("Unsupported datatype: {}".format(datatype))

    def validate_cell(self, value, row, column):
        result = True
        def report(message):
            return self.report_error(
                message,
                value=value,
                row=row,
                column=column
            )
        
        if self.datatype == 'number':
            if not hxl.datatypes.is_number(value):
                result = report("Expected a number")
        elif self.datatype == 'url':
            pieces = urllib.parse.urlparse(value)
            if not (pieces.scheme and pieces.netloc):
                result = report("Expected a URL")
        elif self.datatype == 'email':
            if not re.match(r'^[^@]+@[^@]+$', value):
                result = report("Expected an email address")
        elif self.datatype == 'phone':
            if not re.match(r'^\+?[0-9xX()\s-]{5,}$', value):
                result= report("Expected a phone number")
        elif self.datatype == 'date':
            if not hxl.datatypes.is_date(value):
                result = report("Expected a date")
        return result


class RangeTest(AbstractRuleTest):
    """Test for #valid_value+min and #valid_value+max"""

    def __init__(self, min_value=None, max_value=None):
        super().__init__()

        # normalise strings
        if min_value is not None:
            self.min_value = hxl.datatypes.normalise_string(min_value)
        else:
            self.min_value = None
        if max_value is not None:
            self.max_value = hxl.datatypes.normalise_string(max_value)
        else:
            self.max_value = None

        # precompile numbers and dates for efficiency
        self.min_num = None
        self.max_num = None
        self.min_date = None
        self.max_date = None
        if hxl.datatypes.is_number(min_value):
            self.min_num = hxl.datatypes.normalise_number(min_value)
        if hxl.datatypes.is_number(max_value):
            self.max_num = hxl.datatypes.normalise_number(max_value)
        if hxl.datatypes.is_date(min_value):
            self.min_date = hxl.datatypes.normalise_date(min_value)
        if hxl.datatypes.is_date(max_value):
            self.max_date = hxl.datatypes.normalise_date(max_value)

    def validate_cell(self, value, row, column):
        """Test that a value is >= min_value and/or <= max_value
        Includes special handling for numbers and dates.
        """
        def report(message):
            return self.report_error(
                message,
                value=value,
                row=row,
                column=column
            )

        # try as a date
        if column.tag == '#date' and (self.min_date is not None or self.max_date is not None):
            try:
                date_value = hxl.datatypes.normalise_date(value)
                if self.min_date is not None and date_value < self.min_date:
                    return report('Date must not be before {}'.format(self.min_date))
                elif self.max_date is not None and date_value > self.max_date:
                    return report('Date must not be after {}'.format(self.max_date))
                else:
                    return True
            except ValueError:
                pass # OK

        # try as a number
        if self.min_num is not None or self.max_num is not None:
            try:
                num_value = hxl.datatypes.normalise_number(value)
                if self.min_num is not None and num_value < self.min_num:
                    return report('Value must not be less than {}'.format(self.min_num))
                elif self.max_num is not None and num_value > self.max_num:
                    return report('Value must not be more than {}'.format(self.max_num))
                else:
                    return True
            except ValueError:
                pass # OK

        # try as a case-/whitespace-normalised string
        norm_value = hxl.datatypes.normalise_string(value)
        if self.min_value is not None and norm_value < self.min_value:
            return report('Value must not come before {}'.format(self.min_value))
        elif self.max_value is not None and norm_value > self.max_value:
            return report('Value must not come after {}'.format(self.max_value))
        else:
            return True


class WhitespaceTest(AbstractRuleTest):
    """Test for irregular whitespace in a cell #valid_value+whitespace"""

    PATTERN = r'^(\s+.*|.*(\s\s|[\t\r\n]).*|\s+)$'
    """Regular expression to detect irregular whitespace"""

    def validate_cell(self, value, row, column):
        """Is there irregular whitespace?"""
        if re.match(WhitespaceTest.PATTERN, value):
            return self.report_error(
                'Found extra whitespace',
                value=value,
                row=row,
                column=column,
                suggested_value=hxl.datatypes.normalise_space(value)
            )
        else:
            return True
        

class RegexTest(AbstractRuleTest):
    """Test that non-empty values match a regular expression #valid_value+regex
    TODO: case-(in)sensitive
    """

    def __init__(self, regex):
        super().__init__()
        self.regex = re.compile(regex)

    def validate_cell(self, value, row, column):
        if self.regex.search(value):
            return True
        else:
            return self.report_error(
                'Should match regular expression /{}/'.format(str(self.regex)),
                value=value,
                row=row,
                column=column
            )


class UniqueValueTest(AbstractRuleTest):
    """Test that individual values are unique #valid_unique-key"""

    def start(self):
        self.values_seen = set() # create the empty value set

    def end(self):
        self.values_seen = None # free some memory
        return True

    def validate_cell(self, value, row, column):
        """Report an error if we see the same (normalised) value twice"""
        norm_value = hxl.datatypes.normalise_string(value)
        if norm_value in self.values_seen:
            return self.report_error(
                "Duplicate value",
                value=value,
                row=row,
                column=column
            )
        else:
            self.values_seen.add(norm_value)
            return True


class UniqueRowTest(AbstractRuleTest):
    """Test for duplicate rows, optionally using a list of tag patterns as a key #valid_unique+key"""

    def __init__(self, tag_patterns=None):
        """Constructor
        If no tag patterns are supplied, test the whole row.
        @param tag_patterns: list of tag patterns to test
        """
        super().__init__()
        if tag_patterns is not None:
            self.tag_patterns = hxl.model.TagPattern.parse_list(tag_patterns)
        else:
            self.tag_patterns = None

    def start(self):
        self.keys_seen = set() # create the empty key set

    def end(self):
        self.keys_seen = None # free some memory
        return True

    def validate_row(self, row, indices=[], tag_pattern=None):
        key = row.key(self.tag_patterns)
        if key in self.keys_seen:
            return self.report_error(
                'Duplicate row according to key values {}'.format(str(key)),
                row=row
            )
        else:
            self.keys_seen.add(key)
            return True


class EnumerationTest(AbstractRuleTest):
    """Test against a list of enumerated values 
    #valid_value+list #valid_value+url #valid_value+case
    """

    def __init__(self, allowed_values, case_sensitive=False):
        super().__init__()
        self.case_sensitive = case_sensitive
        self.setup_tables(allowed_values)

    def setup_tables(self, allowed_values):
        self.suggested_value_cache = dict()
        self.cooked_value_set = set()

        if self.case_sensitive:
            for raw_value in allowed_values:
                raw_value = hxl.datatypes.normalise_space(raw_value)
                self.cooked_value_set.add(raw_value)
        else:
            self.raw_value_map = dict()
            for raw_value in allowed_values:
                raw_value = hxl.datatypes.normalise_space(raw_value)
                cooked_value = hxl.datatypes.normalise_string(raw_value)
                self.cooked_value_set.add(cooked_value)
                self.raw_value_map[cooked_value] = raw_value
                
    def validate_cell(self, value, row, column):
        if self.case_sensitive:
            cooked_value = hxl.datatypes.normalise_space(value)
        else:
            cooked_value = hxl.datatypes.normalise_string(value)

        if cooked_value in self.cooked_value_set:
            return True
        else:
            suggested_value = self.get_suggested_value(cooked_value)
            return self.report_error(
                "Value not allowed",
                value=value,
                row=row,
                column=column,
                suggested_value=suggested_value
            )

    def get_suggested_value(self, value):
        # try the cache first
        suggested_value = self.suggested_value_cache.get(value, False) # False to allow for None values
        if suggested_value is False:
            suggested_value = find_closest_match(value, self.cooked_value_set)
            if suggested_value is not None and not self.case_sensitive:
                # we need the original character case if case-insensitive
                suggested_value = self.raw_value_map[suggested_value]
            self.suggested_value_cache[value] = suggested_value
        return suggested_value

    
class CorrelationTest(AbstractRuleTest):
    """Test for correlations with other values
    Supply a list of tag patterns, and report any outliers that don't
    correlate with those columns.
    """

    def __init__(self, tag_patterns):
        """Constructor
        @param tag_patterns: a list of tag patterns for the correlation
        """
        super().__init__()
        self.tag_patterns = hxl.model.TagPattern.parse_list(tag_patterns)

    def start(self):
        """Build an empty correlation map"""
        self.correlation_map = dict()

    def end(self):
        """All the error reporting happens here.
        We don't know the most-common correlations for each key until the end.
        Use the most-common value for each correlation key as the suggested
        value for the others.
        """
        result = True
        for key, value_maps in self.correlation_map.items():
            if len(value_maps) > 1:
                result = False
                value_maps = sorted(
                    list(value_maps.items()),
                    key=lambda e: len(e[1]),
                    reverse=True
                )
                suggested_value = value_maps[0][0]
                for value_map in value_maps[1:]:
                    value = value_map[0]
                    for location in value_map[1]:
                        self.report_error(
                            "Unexpected value",
                            value=value,
                            row=location[0],
                            column=location[1],
                            suggested_value=suggested_value
                        )
        self.correlation_map = None
        return result

    def validate_row(self, row, indices=None, tag_pattern=None):
        """Row validation always succeeds
        We use this method to capture the correlation keys,
        so that we can report on them in the end() method.
        """

        indices = self.get_indices(indices, tag_pattern, row.columns)

        # Make the correlation key
        key = row.key(self.tag_patterns)

        # Record the locations
        # key -> value -> location
        for i in indices:
            if i < len(row.values) and not hxl.datatypes.is_empty(row.values[i]):
                value = row.values[i]
                column = row.columns[i]
                if not key in self.correlation_map:
                    self.correlation_map[key] = {}
                if not value in self.correlation_map[key]:
                    self.correlation_map[key][value] = []
                self.correlation_map[key][value].append((row, column,))

        # always succeed
        return True


#
# A single rule (containing one or more tests) within a schema
#

class SchemaRule(object):
    """A single rule within a schema.
    A rule contains one or more tests.
    """

    def __init__(self, tag_pattern,
                 callback=None, severity="error", description=None,
                 correlation_key=None,
                 consistent_datatypes = False):
        self.tag_pattern = hxl.TagPattern.parse(tag_pattern)
        """Tag pattern to match for the rule"""

        self.tests = []
        """List of \L{AbstractRuleTest} objects to apply as part of this rule"""

        self._saved_indices = None
        """List of saved column indices matching tag_pattern"""
        
        self.consistent_datatypes = consistent_datatypes
        self.callback = callback
        self.severity = severity
        self.description = description
        self.correlation_key = correlation_key

        self.value_url = None # set later, if needed
        """(Failed) URL for reading an external taxonomy"""
        
        self.value_url_error = None # set later, if needed
        """Error from trying to load an external taxonomy"""

        self._initialised = False
        
        self._correlation_map = {}
        self._suggestion_map = {}
        self._consistency_map = {}

    def start(self):
        """Initialisation method
        Call after all values have been set, but before use
        """

        self._saved_indices = None

        def test_callback(e):
            """Relay error reports from tests, with rule-level context added"""
            e.rule = self
            if self.callback:
                self.callback(e)

        # (re)initialise all the tests
        for test in self.tests:
            test.callback = test_callback # call back to here
            test.start()

        if self.correlation_key:
            self.correlation_key = hxl.model.TagPattern.parse_list(self.correlation_key)

        self._initialised = True

    def _check_init(self):
        if not self._initialised:
            self.start()

    def end(self):
        """Call at end of parse to get post-parse errors"""
        result = True

        # finish all the tests
        for test in self.tests:
            if not test.end():
                result = False

        if not self._finish_correlations():
            result = False
        if self.consistent_datatypes and (not self._finish_consistency()):
            result = False

        self._saved_indices = None
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
                

    def validate_dataset(self, dataset):
        """Test whether the columns are present to satisfy this rule."""
        self._check_init()
        
        result = True
        if self._saved_indices is None:
            self._saved_indices = get_column_indices(self.tag_pattern, dataset.columns)

        # Did we fail to load an external URL?
        if self.value_url_error is not None:
            result = self._report_error(
                str("Error reading allowed values from {} ({})".format(self.value_url, str(self.value_url_error)))
            )

        # run each of the tests
        for test in self.tests:
            if not test.validate_dataset(dataset, self._saved_indices):
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
        if self._saved_indices is None:
            self._saved_indices = get_column_indices(self.tag_pattern, row.columns)

        # run each test on the complete row, then on individual cells
        for test in self.tests:
            if not test.validate_row(row, self._saved_indices):
                result = False
            for i in self._saved_indices: # validate individual cells
                if i < len(row.values) and not hxl.datatypes.is_empty(row.values[i]):
                    if not test.validate_cell(row.values[i], row, row.columns[i]):
                        result = False
        #
        # Run dataset-scope validations
        #

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
        self.start()
        if not self.validate_dataset(source):
            result = False
        for row in source:
            if not self.validate_row(row):
                result = False
        if not self.end():
            result = False
        return result

    def start(self):
        """Initialise the validation run"""

        def rule_callback(e):
            """Relay rule callbacks"""
            if self.callback:
                self.callback(e)
                
        for rule in self.rules:
            rule.callback = rule_callback
            rule.start()

    def end(self):
        """Terminate the validation run"""
        result = True
        for rule in self.rules:
            if not rule.end():
                result = False
        return result

    def validate_dataset(self, dataset):
        """Validate just at the dataset level
        e.g. are required columns present
        @param dataset: the \L{hxl.model.Dataset} object to validate
        """
        result = True
        for rule in self.rules:
            if not rule.validate_dataset(dataset):
                result = False
        return result

    def validate_row(self, row):
        """Validate at the row and cell levels.
        Each rule will handle cell-level validation on its own,
        because it knows what columns to look at.
        @param row: the row to validate
        """
        result = True
        for rule in self.rules:
            if not rule.validate_row(row):
                result = False
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


        for row in source:
            tag_pattern = row.get('#valid_tag')
            if tag_pattern:
                rule = SchemaRule(tag_pattern)
                rule.severity = row.get('#valid_severity') or 'error'
                rule.description = row.get('#description')

                # for later use
                case_sensitive = to_boolean(row.get('#valid_value+case'))

                if to_boolean(row.get('#valid_required-min-max')):
                    rule.tests.append(RequiredTest(min_occurs=1, max_occurs=None))

                min_occurs = to_int(row.get('#valid_required+min'))
                max_occurs = to_int(row.get('#valid_required+max'))
                if min_occurs is not None or max_occurs is not None:
                    rule.tests.append(RequiredTest(min_occurs=min_occurs, max_occurs=max_occurs))

                datatype = row.get('#valid_datatype-consistent')
                if datatype is not None:
                    rule.tests.append(DatatypeTest(datatype))

                min_value = row.get('#valid_value+min')
                max_value = row.get('#valid_value+max')
                if min_value is not None or max_value is not None:
                    rule.tests.append(RangeTest(min_value=min_value, max_value=max_value))

                if to_boolean(row.get('#valid_value+whitespace')):
                    rule.tests.append(WhitespaceTest())

                regex = row.get('#valid_value+regex')
                if regex is not None:
                    rule.tests.append(RegexTest(regex))

                if to_boolean(row.get('#valid_unique-key')):
                    rule.tests.append(UniqueValueTest())

                key = row.get('#valid_unique+key')
                if hxl.datatypes.is_truthy(key):
                    # could be problematic if there's even a hashtag like #true or #yes
                    rule.tests.append(UniqueRowTest())
                elif not hxl.datatypes.is_empty(key):
                    rule.tests.append(UniqueRowTest(key))

                l = row.get('#valid_value+list')
                if not hxl.datatypes.is_empty(l):
                    allowed_values = re.split(r'\s*\|\s*', l)
                    if len(allowed_values) > 0:
                        rule.tests.append(EnumerationTest(allowed_values))

                url = row.get('#valid_value+url')
                if not hxl.datatypes.is_empty(url):
                    # default the target tag to the #valid_tag
                    target_tag = row.get('#valid_value+target_tag', default=tag_pattern)
                    try:
                        # read the values from an external dataset
                        source = hxl.data(url)
                        allowed_values = value_source.get_value_set(row.get('#valid_value+target_tag'))
                        if len(allowed_values) > 0:
                            rule.tests.add(EnumerationTest(allowed_values, case_sensitive))
                    except Exception as error:
                        # FIXME - this is kludgey and violates encapsulation
                        # note that we had a problem reading data, but don't stop
                        rule.value_url = url
                        rule.value_url_error = error

                # To be replaced
                rule.correlation_key = row.get('#valid_correlation')
                rule.consistent_datatypes = to_boolean(row.get('#valid_datatype+consistent'))

                schema.rules.append(rule)

        return schema

#
# Internal helper functions
#

def get_column_indices(tag_pattern, columns):
    """Return a list of column indices matching tag_pattern
    @param tag_pattern: the hxl.model.TagPattern to test
    @param columns: a sequence of hxl.model.Column objects to test
    @returns: a possibly-empty sequence of integer indices into columns
    """
    indices = []
    for i, column in enumerate(columns):
        if tag_pattern.match(column):
            indices.append(i)
    return indices


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
