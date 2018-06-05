"""Validation code for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki

A \L{Schema} is the top-level class for validating a HXL dataset. The
easiest way to create a schema is via the hxl.schema() method. The
validate() function validates a dataset. Here is a simple validation
example:

    def callback(e):
        print("Validation error:", e.message)

    source = hxl.data(data_url)
    if not hxl.schema(schema_url, callback=callback).validate(source):
        print('Validation failed')

Each schema contains one or more \L{SchemaRule} objects.

Each schema rule contains one or more objects implementing
\L{AbstractRuleTest}. To add a new test, create a class extending
AbstractRuleTest, override the methods you need (validate_cell() is
the most common), then add code to Schema.parse() method to parse and
create your new test from the HXL schema.

Validation tests go through the following workflow:

- needs_scan() to check if the test needs multiple passes
- start()
- scan_row(\L{hxl.model.Row}) for each row in the dataset (only if needs_scan() returned True)
- scan_cell(value, \L{hxl.model.Row}, L\{hxl.model.Column}) for each non-empty cell in each row (only if needs_scan() returned True)
- end_scan() (only if needs_scan() returned True)
- validate_dataset(\L{hxl.model.Dataset})
- validate_row(\L{hxl.model.Row}) for each row (row-level validations only)
- validate_cell(value, \L{hxl.model.Row}, \L{hxl.model.Column}) for each cell in each row
- end()

Any failure means that the entire test, rule, and schema fail
validation, though error reporting will continue to the end.
"""

import hxl
import base64, datetime, hashlib, logging, math, os, re, urllib

logger = logging.getLogger(__name__)


class HXLValidationException(hxl.HXLException):
    """Data structure to hold a HXL validation error.

    Normally, this exception isn't thrown, but is passed as a
    parameter to callbacks via \L{Schema}, \L{SchemaRule}, and classes
    extending \L{AbstractRuleTest}.
    """

    SCOPES = ('dataset', 'column', 'row', 'cell',)
    """Allowed values for the error scope"""

    def __init__(self, message, rule=None, value=None, row=None, column=None, suggested_value=None, scope='cell'):
        """Construct a new validation error report.
        @param message: the text message for the error
        @param rule: the rule associated with the error (it may have a more-general descriptive message)
        @param value: the value that triggered the error, if available
        @param row: the \L{hxl.model.Row} object associated with the error, if any
        @param column: the \L{hxl.model.Column} object associated with the error, if any
        @param suggested_value: the suggested replacement value, if known
        @param scope: the error scope (dataset, column, row, or cell)
        """
        super().__init__(message)
        
        self.rule = rule
        self.value = value
        self.row = row
        self.column = column
        self.suggested_value = suggested_value

        scope = hxl.datatypes.normalise_string(scope)
        if scope in HXLValidationException.SCOPES:
            self.scope = scope
        else:
            raise HXLException("Unrecognised validation-error scope: {}".format(scope))

    def __str__(self):
        """Get a string rendition of this error."""
        s = '<HXLValidationException '
        if self.message:
            s += self.message + ' '
        if self.rule:
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

    Workflow (triggered by \L{SchemaRule}):

    - needs_scan()
    - start()
    - scan_row() for each row (only if needs_scan() returned True)
    - scan_cell() for each non-empty matching cell (only if needs_scan() returned True)
    - end_scan() (only if needs_scan() returned True)
    - validate_dataset()
    - validate_row() for each row
    - validate_cell() for each matching non-empty cell in each row
    - end()
    """

    def __init__(self, callback=None):
        """Set up a schema test.
        @param callback: a callback function to receive error reports
        """
        self.callback = callback

    def needs_scan(self):
        """Report whether this test requires a cached dataset.

        A cached dataset is one that can be processed more than
        once. It requires more memory and processing time, so return
        True only if absolutely necessary.

        If this method returns True, then the validation engine will
        call scan_row() for each row in the dataset, scan_cell() for
        each non-empty matching cell, and end_scan() before invoking
        any of the validate_* methods.

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
        @returns: True if there are no new validation errors
        """
        return True

    def scan_row(self, row, indices=None, tag_pattern=None):
        """Scan a row of the dataset to collect information.

        Will be called for each row in the dataset, but only if
        needs_scan() returns True.

        This method does not report any errors; it simply collects
        information for the validate_*() methods to use later.

        @param row: the L{hxl.model.Row} object to pre-scan.
        @param indices: optional pre-compiled indices for the rule to
        look at (in lieu of tag_pattern)
        @param tag_pattern: optional tag pattern for the rule to use.

        """
        return

    def scan_cell(self, value, row, column):
        """Pre-scan a single cell to collect information.
        Will be called for each maching non-empty cell in each row,
        but only if needs_scan() returns True.

        This method does not report any errors; it simply collects
        information for the validate_*() methods to use later.

        @param value: the non-empty value to validate
        @param row: a hxl.model.Row object for location
        @param column: a hxl.model.Column object for location
        """
        return

    def end_scan(self):
        """Clean-up calculations after scanning and before validation.
        Will be called only if needs_scan() returned True
        Does not report any errors
        """
        return

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

    def report_error(self, message, row=None, column=None, value=None, suggested_value=None, scope='cell'):
        """Report an error from this test, if there is a callback function available."""
        if self.callback:
            self.callback(HXLValidationException(
                message,
                value=value,
                row=row,
                column=column,
                suggested_value=suggested_value,
                scope=scope
            ))
        return False # for convenience


class RequiredTest(AbstractRuleTest):
    """Test min/max occurrence
    HXL schema: #valid_required
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
        status = True
        indices = self.get_indices(indices, tag_pattern, dataset.columns)
        if self.min_occurs is not None and len(indices) < self.min_occurs:
            self.test_rows = False # no point testing individual rows
            status = self.report_error(
                "Expected at least {} matching column(s)".format(self.min_occurs),
                scope='dataset'
            )
        return status

    def validate_row(self, row, indices=None, tag_pattern=None):
        """Check the number of non-empty occurrences in a row."""
        if not self.test_rows: # skip if there aren't enough columns
            return
        status = True
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
            status = self.report_error(
                "Expected at least {} matching non-empty value(s)".format(self.min_occurs),
                row=row,
                column=first_empty_column,
                scope='row'
            )

        if self.max_occurs is not None and non_empty_count > self.max_occurs:
            status = self.report_error(
                "Expected at most {} matching non-empty value(s)".format(self.max_occurs),
                row=row,
                column=last_nonempty_column,
                scope='row'
            )

        return status

    
class DatatypeTest(AbstractRuleTest):
    """Test for a specified datatype
    HXL schema: #valid_datatype-consistent
    See also \L{ConsistentDatatypeTest}, which infers the most-common datatype.
    """

    # allowed datatypes
    DATATYPES = ('text', 'number', 'url', 'email', 'phone', 'date',)

    def __init__(self, datatype):
        """Constructor
        @param datatype: a string specifying the datatype (e.g. "number")
        """
        super().__init__()
        # make sure we recognise the datatype
        datatype = hxl.datatypes.normalise_string(datatype)
        if datatype in DatatypeTest.DATATYPES:
            self.datatype = datatype
        else:
            raise hxl.HXLException("Unsupported datatype: {}".format(datatype))

    def validate_cell(self, value, row, column):
        """Validate datatypes on the individual cell level"""
        status = True
        def report(message):
            return self.report_error(
                message,
                value=value,
                row=row,
                column=column
            )
        
        if self.datatype == 'number':
            if not hxl.datatypes.is_number(value):
                status = report("Expected a number")
        elif self.datatype == 'url':
            pieces = urllib.parse.urlparse(value)
            if not (pieces.scheme and pieces.netloc):
                status = report("Expected a URL")
        elif self.datatype == 'email':
            if not re.match(r'^[^@]+@[^@]+$', value):
                status = report("Expected an email address")
        elif self.datatype == 'phone':
            if not re.match(r'^\+?[0-9xX()\s-]{5,}$', value):
                status= report("Expected a phone number")
        elif self.datatype == 'date':
            if not hxl.datatypes.is_date(value):
                status = report("Expected a date")
        return status


class RangeTest(AbstractRuleTest):
    """Test for a range of numbers or strings
    HXL schema: #valid_value+min and #valid_value+max
    This class will try to determine whether to use date sorting, numeric sorting, or lexical sorting
    to determine what is within a range. Ranges are always case-insensitive
    """

    def __init__(self, min_value=None, max_value=None):
        """Constructor
        @param min_value: the minimum allowed value, or None for no minimum
        @param max_value: the maximum allowed value, or None for no maximum
        """
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
    """Test for irregular whitespace in a cell 
    HXL schema: #valid_value+whitespace
    Irregular whitespace is any leading or trailing space, 
    or anything but a single space character inside a string
    """

    PATTERN = r'^(\s+.*|.*(\s\s|[\t\r\n]).*|.*\s+)$'
    """Regular expression to detect irregular whitespace"""

    def validate_cell(self, value, row, column):
        """Is there irregular whitespace?"""
        if hxl.datatypes.is_string(value):
            if re.match(WhitespaceTest.PATTERN, value):
                return self.report_error(
                    'Found extra whitespace',
                    value=value,
                    row=row,
                    column=column,
                    suggested_value=hxl.datatypes.normalise_space(value)
                )
        return True
        

class RegexTest(AbstractRuleTest):
    """Test that non-empty values match a regular expression 
    HXL schema: #valid_value+regex
    The regex is unanchored, so use '^' or '$' to anchor if needed
    Whitespace is not normalised before matching
    """

    def __init__(self, regex, case_sensitive=True):
        """Constructor
        @param regex: the regular expression to test against
        @param case_sensitive: if True (default), matches are case-sensitive
        """
        super().__init__()
        self.regex_text = str(regex)
        if case_sensitive:
            self.regex = re.compile(regex)
        else:
            self.regex = re.compile(regex, flags=re.IGNORECASE)

    def validate_cell(self, value, row, column):
        """Match value (including whitespace) against the regular expression"""
        if self.regex.search(value):
            return True
        else:
            return self.report_error(
                'Should match regular expression /{}/'.format(self.regex_text),
                value=value,
                row=row,
                column=column
            )


class UniqueValueTest(AbstractRuleTest):
    """Test that individual values are unique
    HXL schema: #valid_unique-key
    Every value in any column matching tag_pattern must be unique.
    Normalises case and whitespace before testing, so "Aaa" and "  aaa" 
    would count as duplicates.
    """

    def start(self):
        self.values_seen = set() # create the empty value set

    def end(self):
        self.values_seen = None # free some memory
        return True

    def validate_cell(self, value, row, column):
        """Report an error if we see the same (normalised) value more than once"""
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
    """Test for duplicate rows, optionally using a list of tag patterns as a key
    HXL schema: #valid_unique+key
    If there are no tag patterns provided, uses the entire row to make the key
    Note that the target tag pattern (#valid_tag) is irrelevant for this test.
    """

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
                row=row,
                scope='row'
            )
        else:
            self.keys_seen.add(key)
            return True


class EnumerationTest(AbstractRuleTest):
    """Test against a list of enumerated values 
    HXL schema: #valid_value+list #valid_value+url #valid_value+case
    This test can also hold an extra error to report if there was a problem
    e.g. reading the values externally.
    """

    def __init__(self, allowed_values, case_sensitive=False):
        """Constructor
        @param allowed_values: sequence of allowed values
        @param case_sensitive: if True, make comparisons case-sensitive
        """
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
    #valid_correlation
    Supply a list of tag patterns, and report any outliers that don't
    correlate with those columns.
    TODO: this might be more efficient with pre-scanning
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
        status = True
        for tagspec, correlations in self.correlation_map.items():
            for key, value_maps in correlations.items():
                if len(value_maps) > 1:
                    status = False
                    value_maps = sorted(
                        value_maps.items(),
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
        return status

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
            tagspec = row.columns[i].get_display_tag(sort_attributes=True)
            if i < len(row.values) and not hxl.datatypes.is_empty(row.values[i]):
                value = row.values[i]
                column = row.columns[i]
                self.correlation_map.setdefault(tagspec, {}).setdefault(key, {}).setdefault(value, []).append((row, column,))

        # always succeed
        return True

    
class ConsistentDatatypesTest(AbstractRuleTest):
    """Check for consistent datatypes in a column.
    HXL: #valid_datatype+consistent
    Will report all but the most-common datatype as errors.
    Special knowledge of the #date hashtag
    """

    def needs_scan(self):
        """We want to prescan the dataset"""
        return True

    def start(self):
        self.datatype_map = dict()

    def scan_cell(self, value, row, column):
        datatype = self.guess_type(value, column)
        tagspec = column.get_display_tag(sort_attributes=True) # FIXME

        # keep track of how often the type appeared
        if not tagspec in self.datatype_map:
            self.datatype_map[tagspec] = {}
        if not datatype in self.datatype_map[tagspec]:
            self.datatype_map[tagspec][datatype] = 0
        self.datatype_map[tagspec][datatype] += 1

    def end_scan(self):
        """Reduce the datatype_map to the most-common type for each tagspec"""
        for tagspec, datatypes in self.datatype_map.items():
            max_type = None
            max_count = None
            for datatype, count in datatypes.items():
                if max_count is None or count > max_count:
                    max_count = count
                    max_type = datatype
            self.datatype_map[tagspec] = max_type

    def validate_cell(self, value, row, column):
        """Keep track of each different datatype
        Error reporting happens in the end() method, once we know which
        type is most common.
        @returns: always True
        """
        actual_datatype = self.guess_type(value, column)
        tagspec = column.get_display_tag(sort_attributes=True) # FIXME
        expected_datatype = self.datatype_map.get(tagspec)

        if actual_datatype == expected_datatype:
            return True
        else:
            message = "Inconsistent data types: expected {} but found {}".format(expected_datatype, actual_datatype)
            return self.report_error(
                message,
                value=value,
                row=row,
                column=column
            )

    def guess_type(self, value, column):
        """Guess the type of a value"""
        if column.tag == '#date' and hxl.datatypes.is_date(value):
            return 'date'
        elif hxl.datatypes.is_number(value):
            return 'number'
        else:
            return 'text'


class SpellingTest(AbstractRuleTest):
    """Detect spelling outliers in a column
    HXL schema: #valid_value+spelling
    Will treat numbers and dates as strings, so use this only in columns where
    you expect text, and frequently-repeated values (e.g. #status, #org+name, #sector+name).

    Will skip validation if the coefficient of variation > 1.0

    Collects all of the spelling variants first, then checks the rare ones in the end() method, and
    reports any ones that have near matches among the common ones.
    """

    ERROR_CUTOFF=0.05 # 5%
    """Cutoff for reporting a possible error, as percentage of mean frequency for each spelling"""

    def __init__(self, case_sensitive=False):
        """Constructor
        @param case_sensitive: if True, differences in case are considered errors (default False)
        """
        super().__init__()
        self.case_sensitive = case_sensitive

    def start(self):
        """Set up for a validation run"""
        # spelling -> locations

        self.spelling_map = dict()
        """Store spellings and locations by tagspec"""
        
        self.total_occurrences = dict()
        """Count the total spelling occurrences, for mean and standard deviation, by tagspec"""

    def end(self):
        """Report possible spelling errors.
        Collect all spellings that appear less than 1/3 of the mean frequency.

        For each of these, check whether there's a close match among
        the more-common spellings, and if so, then report (once for
        each location) with the suggested correction.
        """

        # start by assuming all is well
        status = True

        for tagspec, spellings in self.spelling_map.items():

            # cache corrections so that we don't keep looking up the same ones
            correction_cache = {}

            # if there aren't any spellings, then we're done
            if len(spellings) == 0:
                break

            # get the average (mean) occurrences for each spelling
            mean_frequency = self.total_occurrences[tagspec] / len(spellings)

            # calculate the coefficiant of variance (dimensionless)
            standard_deviation = math.sqrt(
                sum(map(lambda n: (len(n)-mean_frequency)**2, spellings.values())) / len(spellings)
            )
            variance_coefficient = standard_deviation / mean_frequency

            # there's no point spelling checking unless the variance coefficient is low enough to be meaningful
            if variance_coefficient > 1.0:
                break

            # first pass: collect and clear good spellings
            good_spellings = list()
            for spelling, locations in spellings.items():
                if len(locations) > mean_frequency * SpellingTest.ERROR_CUTOFF:
                    good_spellings.append(spelling)
                    spellings[spelling] = None # no potential errors

            # second pass: report any remaining dubious spellings that have close matches among good spellings
            for spelling, locations in spellings.items():
                if locations is None: # this spelling was OK
                    continue
                # is there a near match among good spellings?
                if spelling in correction_cache:
                    correction = correction_cache['spelling']
                else:
                    correction = find_closest_match(spelling, good_spellings)
                    correction_cache[spelling] = correction
                if correction is not None:
                    # if it's rare and there's a near match, report an error
                    status = False
                    for location in locations:
                        self.report_error(
                            'Possible spelling error',
                            value=location[2],
                            row=location[0],
                            column=location[1],
                            suggested_value=correction,
                            scope='cell'
                        )
                
        return status # false if we've found a possible correction

    def validate_cell(self, value, row, column):
        """Record all the spellings found, for later sorting"""

        tagspec = column.get_display_tag(sort_attributes=True) # FIXME

        if self.case_sensitive:
            cooked_value = hxl.datatypes.normalise_space(value)
        else:
            cooked_value = hxl.datatypes.normalise_string(value)
        self.total_occurrences[tagspec] = self.total_occurrences.setdefault(tagspec, 0) + 1
        self.spelling_map.setdefault(tagspec, {}).setdefault(cooked_value, []).append((row, column, value,))
        return True

    
class NumericOutlierTest(AbstractRuleTest):
    """Detect outliers among matching values
    Will skip any tagset with a coefficient of variation > 1.0
    """

    def needs_scan(self):
        return True

    def start(self):
        self.standard_deviations = dict()
        self.mean_values = dict()
        self.variation_coefficients = dict()
        self.values = dict()

    def scan_cell(self, value, row, column):
        tagspec = column.get_display_tag(sort_attributes=True) # FIXME
        try:
            num = hxl.datatypes.normalise_number(value)
            if not tagspec in self.values:
                self.values[tagspec] = list()
            self.values[tagspec].append(num)
        except:
            # not a number, so ignore
            pass

    def end_scan(self):
        for tagspec in self.values:

            values = self.values[tagspec]

            # if the list is long enough, remove the min and max values
            if len(values) >= 10: # 10 is our cutoff for removing lowest and highest values
                values = sorted(values)[1:-1]

            # now calculate the standard deviation of the remaining values
            self.mean_values[tagspec] = sum(values) / len(values)
            self.standard_deviations[tagspec] = math.sqrt(
                sum(map(lambda n: (n-self.mean_values[tagspec])*(n-self.mean_values[tagspec]), values)) / len(values)
            )

            # calculate the coefficient of variance, which we'll use as a cutoff
            self.variation_coefficients[tagspec] = self.standard_deviations[tagspec] / self.mean_values[tagspec]

        # free some memory
        del self.values

    def validate_cell(self, value, row, column):
        tagspec = column.get_display_tag(sort_attributes=True) # FIXME

        # don't bother if the data is highly variable
        if self.variation_coefficients.get(tagspec, 0.0) > 1.0:
            return True

        # try numeric validation
        try:
            num = hxl.datatypes.normalise_number(value)
            distance = abs(num - self.mean_values[tagspec])
            if distance > self.standard_deviations[tagspec] * 3:
                self.report_error(
                    "Possible numeric outlier",
                    value=value,
                    row=row,
                    column=column
                )
                return False
        except:
            # not a number, so ignore
            pass
        return True
    

#
# A single rule (containing one or more tests) within a schema
#

class SchemaRule(object):
    """A single rule within a schema.
    A rule contains one or more tests, together with some common metadata
    (a tag pattern, severity level, and description). If any test fails, then
    the whole rule fails.

    Workflow (triggered by \L{Schema.validate}:

    - needs_scan()
    - start()
    - scan_row() for each row (if needs_scan() returned True)
    - scan_cell() for each maching non-empty cell (if needs_scan() returned True)
    - end_scan() (if needs_scan() returned True)
    - validate_dataset()
    - validate_row() for each row
    - validate_cell() for each matching non-empty cell in each row
    - end()
    """

    SEVERITY = ('info', 'warning', 'error',)

    def __init__(self, tag_pattern, severity="error", description=None, callback=None):
        """Constructor
        @param tag_pattern: the tag pattern to match for the rule
        @param severity: one of 'info', 'warning', or 'error' (default)
        @param description: an optional error message to override the default from the tests
        @param callback: an optional callback function to handle error reports
        """
        self.tag_pattern = hxl.TagPattern.parse(tag_pattern)
        self.description = description
        self.callback = callback

        # make sure the severity level is valid
        severity = hxl.datatypes.normalise_string(severity)
        if severity in SchemaRule.SEVERITY:
            self.severity = severity
        else:
            raise HXLException("Unsupported rule severity level: {}".format(severity))

        # Additional internal variables
        self.tests = []
        """List of \L{AbstractRuleTest} objects to apply as part of this rule"""

        self.loading_errors = []
        """Errors setting up the rule, to be reported once during each run."""

        self.saved_indices = None
        """List of saved column indices matching tag_pattern"""

    def needs_scan(self):
        """Check if any test in this rule requires a pre-scan of the dataset.
        @returns: True if at least one test's needs_scan() method returns True
        """
        for test in self.tests:
            if test.needs_scan():
                return True
        return False

    def start(self):
        """Initialisation method
        Call after all values have been set, but before use
        """

        self.saved_indices = None

        def test_callback(e):
            """Relay error reports from tests, with rule-level context added"""
            e.rule = self
            if self.callback:
                self.callback(e)

        # (re)initialise all the tests
        for test in self.tests:
            test.callback = test_callback # call back to here
            test.start()

    def end(self):
        """Call at end of parse to get post-parse errors"""
        status = True

        # finish all the tests
        for test in self.tests:
            if not test.end():
                status = False

        self.saved_indices = None
        return status

    def scan_row(self, row):
        """Pre-scan a row and its individual cells.
        This method does not report errors or return a status.
        Will be invoked only if needs_scan() returned True
        Calls both scan_row() and scan_cell() for each test.
        @param row the Row to scan
        """
        if self.saved_indices is None:
            self.saved_indices = get_column_indices(self.tag_pattern, row.columns)

        # scan each row, then each matching cell in the row
        for test in self.tests:
            if not test.needs_scan(): # don't invoke unless the test asked for it
                continue
            test.scan_row(row, self.saved_indices)
            for i in self.saved_indices: # validate individual cells
                if i < len(row.values) and not hxl.datatypes.is_empty(row.values[i]):
                    test.scan_cell(row.values[i], row, row.columns[i])

    def end_scan(self):
        """Invoke end_scan() for all tests that need it"""
        for test in self.tests:
            if test.needs_scan():
                test.end_scan()
                    
    def validate_dataset(self, dataset, indices=None, tag_pattern=None):
        """Test whether the columns are present to satisfy this rule."""
        
        status = True
        if self.saved_indices is None:
            self.saved_indices = get_column_indices(self.tag_pattern, dataset.columns)

        # Report any loading errors
        for error in self.loading_errors:
            status = False
            if self.callback:
                self.callback(error)

        # run each of the tests
        for test in self.tests:
            if not test.validate_dataset(dataset, indices=self.saved_indices):
                status = False

        return status

    def validate_row(self, row):
        """
        Apply the rule to an entire Row
        @param row the Row to validate
        @return True if all matching values in the row are valid
        """

        # individual rules may change to False
        status = True
        if self.saved_indices is None:
            self.saved_indices = get_column_indices(self.tag_pattern, row.columns)

        # run each test on the complete row, then on individual cells
        for test in self.tests:
            if not test.validate_row(row, self.saved_indices):
                status = False
            for i in self.saved_indices: # validate individual cells
                if i < len(row.values) and not hxl.datatypes.is_empty(row.values[i]):
                    if not test.validate_cell(row.values[i], row, row.columns[i]):
                        status = False

        return status

    def __str__(self):
        """String representation of a rule (for debugging)"""
        return "<HXL schema rule: " + str(self.tag_pattern) + ">"
                

class Schema(object):
    """Schema against which to validate a HXL document.
    Consists of a sequence of L{SchemaRule} objects to apply to the data.

    The validate() method triggers the following:

    - start()
    - validate_dataset()
    - validate_row() for each row
    - validate_cell() for each matching non-empty cell in each row
    - end()

    Add new rules with

        schema.rules.append(rule)
    """

    def __init__(self, callback=None):
        """Constructor
        @param callback: a callback function to receive \L{HXLValidationException} objects as error reports
        """
        self.rules = []
        """Rules making up this schema"""
        
        self.callback = callback
        """Callback function to receive error reports"""

    def validate(self, source):
        """Execute the main validation workflow.
        @param source: the \L{hxl.model.Dataset} to validate
        """
        status = True # all is well at the beginning
        needs_scan = False # assume we don't need a pre-scan

        # do we need a cached, in-memory dataset?
        for rule in self.rules:
            if rule.needs_scan():
                needs_scan = True
                if not source.is_cached:
                    source = source.cache()
                break

        # initial setup
        self.start()

        # pre-scan if needed
        if needs_scan:
            for row in source:
                self.scan_row(row)
            self.end_scan()

        # dataset-level validations
        if not self.validate_dataset(source):
            status = False

        # row-level validations
        # (will also include cell-level validations)
        for row in source:
            if not self.validate_row(row):
                status = False

        # finalisation
        if not self.end():
            status = False
            
        return status

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
        status = True
        for rule in self.rules:
            if not rule.end():
                status = False
        return status

    def scan_row(self, row):
        """Pre-scan a row, only for rules that require it."""
        for rule in self.rules:
            if rule.needs_scan():
                rule.scan_row(row)

    def end_scan(self):
        """End pre-scan, for rules that require it."""
        for rule in self.rules:
            if rule.needs_scan():
                rule.end_scan()

    def validate_dataset(self, dataset):
        """Validate just at the dataset level
        @param dataset: the \L{hxl.model.Dataset} object to validate
        """
        status = True
        for rule in self.rules:
            if not rule.validate_dataset(dataset):
                status = False
        return status

    def validate_row(self, row):
        """Validate at the row and cell levels.
        Each rule will handle cell-level validation on its own,
        because it knows what columns to look at.
        @param row: the row to validate
        """
        status = True
        for rule in self.rules:
            if not rule.validate_row(row):
                status = False
        return status

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
            path = os.path.join(os.path.dirname(__file__), 'hxl-default-schema.json');
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
            tags = row.get('#valid_tag')
            if tags:
                tag_patterns = hxl.model.TagPattern.parse_list(tags)
                for tag_pattern in tag_patterns:
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
                        rule.tests.append(RegexTest(regex, case_sensitive))

                    if to_boolean(row.get('#valid_value+spelling')):
                        rule.tests.append(SpellingTest(case_sensitive=case_sensitive))

                    if to_boolean(row.get('#valid_unique-key')):
                        rule.tests.append(UniqueValueTest())

                    key = row.get('#valid_unique+key')
                    if hxl.datatypes.is_truthy(key):
                        # could be problematic if there's even a hashtag like #true or #yes
                        rule.tests.append(UniqueRowTest())
                    elif not hxl.datatypes.is_empty(key):
                        rule.tests.append(UniqueRowTest(key))

                    correlations = row.get('#valid_correlation')
                    if not hxl.datatypes.is_empty(correlations):
                        rule.tests.append(CorrelationTest(correlations))

                    if to_boolean(row.get('#valid_datatype+consistent')):
                        rule.tests.append(ConsistentDatatypesTest())

                    if to_boolean(row.get('#valid_value+outliers')):
                        rule.tests.append(NumericOutlierTest())

                    l = row.get('#valid_value+list')
                    if not hxl.datatypes.is_empty(l):
                        allowed_values = re.split(r'\s*\|\s*', l)
                        if len(allowed_values) > 0:
                            rule.tests.append(EnumerationTest(allowed_values, case_sensitive))

                    url = row.get('#valid_value+url')
                    if not hxl.datatypes.is_empty(url):
                        # default the target tag to the #valid_tag
                        target_tag = row.get('#valid_value+target_tag', default=tag_pattern)
                        try:
                            # read the values from an external dataset
                            source = hxl.data(url)
                            allowed_values = source.get_value_set(row.get('#valid_value+target_tag'))
                            if len(allowed_values) > 0:
                                rule.tests.append(EnumerationTest(allowed_values, case_sensitive))
                        except Exception as error:
                            rule.loading_errors.append(HXLValidationException(
                                'Error loading allowed values from {}: {}'.format(url, str(error)),
                                scope='dataset',
                                rule=rule
                            ))


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
    Uses a cutoff of len(s)/3 for candidate matches, and if two matches have the same 
    edit distance, prefers the one with the longer common prefix.
    @param s: the misspelled string to check
    @param allowed_values: a list of allowed values
    @return: the best match, or None if there was no candidate
    """
    best_match = None
    max_distance = len(s) / 3
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
    """Convenience function for making a schema
    Imported into __init__, and usually called as hxl.schema(source, callback).
    The callback, if provided, will receive a HXLValidationException object for each error
    @param source: something that can be used as a HXL data source
    @param callback: the validation callback function to use
    """
    return Schema.parse(source, callback)


def validate(data, schema=None):
    """Convenience function for validating HXL data.
    The is_valid parameter in the report will be a True/False value for the result.

    If you want to do anything tricky, you can pass pre-cooked hxl.data() parameters in, e.g.

    result = hxl.validate(hxl.data('foo.csv', allow_local=True), hxl.data('schema.csv', allow_local=True))

    @param data: the data to validate (a URL or anything else accepted by \L{hxl.data})
    @param schema: the schema to validate against (anything accepted by L{hxl.data}), or None (default) to use the built-in schema.
    @returns: a JSON validation report as documented at https://github.com/HXLStandard/hxl-proxy/wiki/Validation-reports
    """

    issue_map = dict()

    def add_issue(issue):
        hash = make_rule_hash(issue.rule)
        issue_map.setdefault(hash, []).append(issue)

    status = hxl.schema(schema, callback=add_issue).validate(hxl.data(data))

    schema_url = None
    data_url = None
    if hxl.datatypes.is_string(schema):
        schema_url = schema
    if hxl.datatypes.is_string(data):
        data_url = data

    return make_json_report(status, issue_map, schema_url=schema_url, data_url=data_url)


#
# Local functions
#

def make_json_report(status, issue_map, schema_url=None, data_url=None):
    """Generate a JSON error report from a dict of errors
    @param status: the validation status (boolean)
    @param issue_map: a dict of lists of \L{HXLValidationException} objects grouped by rule hash
    @param data_url: the original URL of the data, if available
    @param schema_url: the original URL of the schema, if available
    """

    json_report = {
        "validator": "libhxl-python",
        "timestamp": datetime.datetime.now().isoformat(),
        "is_valid": status,
        "stats": {
            "info": 0,
            "warning": 0,
            "error": 0,
            "total": 0
        },
        "issues": [],
    }

    if schema_url is not None:
        json_report['schema_url'] = schema_url
        
    if data_url is not None:
        json_report['data_url'] = data_url

    # add the issue objects
    for rule_id, locations in issue_map.items():
        json_issue = make_json_issue(rule_id, locations)
        json_report['stats']['total'] += len(json_issue['locations'])
        json_report['stats'][locations[0].rule.severity] += len(json_issue['locations'])
        json_report['issues'].append(json_issue)

    return json_report

def make_json_issue(rule_id, locations):
    """Create an issue (with list of locations) for a JSON validation report
    @param rule_id: the hash for the rule (used to group locations)
    @param locations: a list of \L{HXLValidation"""

    # grab first location as a model
    model = locations[0]

    # get a custom description first, then the generic message as a fallback
    description = model.rule.description
    if not description:
        description = model.message

    # get all unique locations
    location_keys = set()
    json_locations = []
    for location in locations:
        location_key = (location.row.row_number, location.column.column_number, location.value, location.suggested_value,)
        if not location_key in location_keys:
            json_locations.append(make_json_location(location))
            location_keys.add(location_key)

    # make the issue
    json_issue = {
        "rule_id": rule_id,
        "tag_pattern": str(model.rule.tag_pattern),
        "description": description,
        "severity": model.rule.severity,
        "location_count": len(locations),
        "scope": model.scope,
        "locations": json_locations
    }

    return json_issue

def make_json_location(location):
    """Create a single location for a JSON validation report"""
    json_location = {}

    # is there a row object?
    if location.row is not None:
        if location.row.row_number is not None:
            json_location['row'] = location.row.row_number
        if location.row.source_row_number is not None:
            json_location['source_row'] = location.row.source_row_number

    # is there a column object?
    if location.column is not None:
        if location.column.column_number is not None:
            json_location['col'] = location.column.column_number
        if location.column.display_tag is not None:
            json_location['hashtag'] = location.column.display_tag

    # is there an error value?
    if location.value is not None:
        json_location['location_value'] = location.value

    # is there a suggested value?
    if location.suggested_value is not None:
        json_location['suggested_value'] = location.suggested_value

    return json_location


def make_rule_hash(rule):
    """Make a good-enough hash for a rule."""
    s = "\r".join([str(rule.severity), str(rule.description), str(rule.tag_pattern)])
    return base64.urlsafe_b64encode(hashlib.md5(s.encode('utf-8')).digest())[:8].decode('ascii')

# end
