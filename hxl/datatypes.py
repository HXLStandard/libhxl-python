"""Utility functions for testing and normalising scalar-ish data types

Other modules in libhxl use these functions for consistent type
checking, conversion, and normalisation.

Examples:
    ```
    s = hxl.datatypes.normalise("   This IS a String  ") # => "this is a string"
    s = hxl.datatypes.normalise_whitespace("   a  b\\nc") # => "a b c"
    s = hxl.datatypes.normalise_date("1/13/2020") # => "2020-01-13"
    hxl.datatypes.is_empty("     ") # => True
    type = hxl.datatypes.typeof("     ") # => "empty"
    ```

Author:
    David Megginson

License:
    Public Domain

"""

import collections, datetime, dateutil.parser, json, logging, re, six, unidecode

__all__ = ["TOKEN_PATTERN", "normalise", "typeof", "flatten", "is_truthy", "is_empty", "is_string", "is_token", "normalise_space", "normalise_string", "is_number", "normalise_number", "is_date", "normalise_date", "is_dict", "is_list"]

logger = logging.getLogger(__name__)



########################################################################
# Constants
########################################################################

TOKEN_PATTERN = r'[A-Za-z][_0-9A-Za-z]*'
"""A regular expression matching a single string token.
"""

_WHITESPACE_PATTERN = re.compile(r'\s+', re.MULTILINE)

_ISO_DATE_PATTERN = re.compile(
    r'^(?P<year>[12]\d\d\d)(?:Q(?P<quarter>[1-4])|W(?P<week>\d\d?)|-(?P<month>\d\d?)(?:-(?P<day>\d\d?))?)?$',
    re.IGNORECASE
)

_SQL_DATETIME_PATTERN = re.compile(
    r'^(?P<year>[12]\d\d\d)-(?P<month>\d\d?)-(?P<day>\d\d?) \d\d?:\d\d?:\d\d?(?P<week>)?(?P<quarter>)?$'
)

_DEFAULT_DATE_1 = datetime.datetime(2015, 1, 1)

_DEFAULT_DATE_2 = datetime.datetime(2016, 3, 3)



########################################################################
# Functions
########################################################################

def normalise(value, col=None, dayfirst=True):
    """Intelligently normalise a value, optionally using the HXL hashtag and attributes for hints

    Attempt to guess the value's type using duck typing and
    (optionally) hints from the HXL hashtag, then product a string
    containing a standard representation of a date or number (if
    appropriate), or a string with whitespace normalised.

    Args:
        value: the value to convert to a normalised string
        col (hxl.model.Column): an optional Column object associated with the string (for hints)
        dayfirst (bool): hint for whether to default to DD-MM-YYYY or MM-DD-YYY when ambiguous.

    Returns:
        str: A normalised string version of the value provided.

    """
    # TODO add lat/lon

    if col and col.tag == '#date':
        try:
            return normalise_date(value, dayfirst=dayfirst)
        except ValueError:
            pass

    # fall through
    try:
        return normalise_number(value)
    except ValueError:
        return normalise_string(value)


def typeof(value, col=None):
    """Use duck typing and HXL hinting to guess of a value

    Args:
        value: the value to check
        col (hxl.model.Column): an optional Column object for hinting (via the hashtag and attributes)

    Returns:
        str: one of the strings "date", "number", "empty", or "string"

    """
    if col and col.tag == '#date' and is_date(value):
        return 'date'
    elif is_number(value):
        return 'number'
    elif is_empty(value):
        return 'empty'
    else:
        return 'string'


def flatten(value, use_json=True, separator=" | "):
    """Flatten potential lists and dictionaries

    If use_json is false, then remove hierarchies, and create a single list
    separated with " | ", and will use dict keys rather than values.

    Args:
        value: the value to flatten (may be a list)
        use_json (bool): if True (default), encode top-level lists as JSON
        separator (str): the string to use as a separator, if use_json is false

    Returns:
        str: a string version of the value

    """
    # keep it simple for now
    if value is None:
        return ''
    elif is_list(value) or is_dict(value):
        if use_json:
            return json.dumps(value)
        else:
            return " | ".join([flatten(item, False) for item in value])
    else:
        return str(value)

    
def is_truthy(value):
    """Loosely check for a boolean-type true value

    Accepts values such as "1", "yes", "t", "true", etc

    Args:
        value: the value to test

    Returns:
        bool: True if the value appears truthy

    """
    return normalise_string(value) in ['y', 'yes', 't', 'true', '1']


def is_empty(value):
    """Test for a functionally-empty value.

    None, empty string, or whitespace only counts as empty; anything else doesn't.

    Args:
        value: value to test

    Returns:
        bool: True if the value is functionally empty

    """
    return (value is None or value == '' or str(value).isspace())


def is_string(value):
    """Test if a value is already a string

    Looks for an actual string data type.

    Args:
        value: the value to test

    Returns:
        bool: True if the value is a string type.

    """
    return isinstance(value, six.string_types)


def is_token(value):
    """Test if a value is a valid HXL token

    A token is the string that may appear after "#" for a hashtag, or
    "+" for an attribute.  It must begin with a letter (A-Z, a-z),
    followed by letters, numbers, or underscore ("_"). Internal
    spaces, accented/non-Roman characters, and space or other
    punctuation are not allowed.

    Args:
        value: the value to test

    Returns:
        bool: True if the value is a token

    """
    return is_string(value) and re.fullmatch(TOKEN_PATTERN, value)


def normalise_space(value):
    """Normalise whitespace only in a string

    This method will convert the input value to a string first, then
    remove any leading or trailing whitespace, and replace all
    sequences of internal whitespace (including line breaks) with a
    single space character.

    Note: this does not perform other normalisations (date, etc), but
    simply calls the str() function on the value provided.

    Args:
        value: the value to normalise

    Returns:
        str: a string representation of the original value, with whitespace normalised.

    """
    if is_empty(value):
        return ''
    else:
        value = str(value).strip().replace("\n", " ")
        return re.sub(
            _WHITESPACE_PATTERN,
            ' ',
            value
        )


def normalise_string(value):
    """Normalise a string.

    Remove all leading and trailing whitespace. Convert to lower
    case. Replace all internal whitespace (including lineends) with a
    single space. Replace None with ''.

    The input value will be forced to a string using str()

    Args:
        value: the string to normalise

    Returns:
        str: the normalised string

    """
    if value is None:
        value = ''
    else:
        value = str(value)
    return normalise_space(unidecode.unidecode(value)).lower()


def is_number(value):
    """By duck typing, test if a value contains something recognisable as a number.

    Args:
        value: the value (string, int, float, etc) to test

    Returns:
        bool: True if usable as a number (via normalise_number())

    """
    try:
        float(value)
        return True
    except:
        return False


def normalise_number(value):
    """Attempt to convert a value to a number.

    Will convert to int type if it has no decimal places.

    Args:
        value: the value (string, int, float, etc) to convert.

    Returns:
        int: an integer value if there are no decimal places
        float: a floating point value if there were decimal places

    Raises:
        ValueError: if the value cannot be converted

    """
    try:
        n = float(value)
        if n == int(n):
            return int(n)
        else:
            return n
    except:
        raise ValueError("Cannot convert to number: {}".format(value))


def is_date(value):
    """Test if a value contains something recognisable as a date.

    Args:
        value: the value (string, etc) to test

    Returns:
        True if usable as a date

    """
    try:
        normalise_date(value)
        return True
    except ValueError:
        return False


def normalise_date(value, dayfirst=True):
    """Normalise a string as a date.

    This function will take a variety of different date formats and
    attempt to convert them to an ISO 8601 date, such as
    "2020-06-01". It also will use a non-ISO format for quarter years,
    such as "2020Q2".

    Args:
        value: the value to normalise as a date
        dayfirst (bool): if the date is ambiguous, assume the day comes before the month

    Returns:
        str: the date in ISO 8601 format or the extended quarters syntax
    
    Raises:
        ValueError: if the value cannot be parsed as a date

    """

    def make_date_string(year, quarter=None, month=None, week=None, day=None):
        if quarter:
            # *not* real ISO 8601
            quarter = int(quarter)
            if quarter < 1 or quarter > 4:
                raise ValueError("Illegal Quarter number: {}".format(quarter))
            return '{:04d}Q{:01d}'.format(int(year), int(quarter))
        elif week:
            week = int(week)
            if week < 1 or week > 53:
                raise ValueError("Illegal week number: {}".format(week))
            return '{:04d}W{:02d}'.format(int(year), int(week))
        elif month:
            month = int(month)
            if month < 1 or month > 12:
                raise ValueError("Illegal month number: {}".format(month))
            if day:
                day = int(day)
                if day < 1 or day > 31 or (month in [4, 6, 9, 11] and day > 30) or (month==2 and day>29):
                    raise ValueError("Illegal day {} for month {}".format(day, month))
                return '{:04d}-{:02d}-{:02d}'.format(int(year), int(month), int(day))
            else:
                return '{:04d}-{:02d}'.format(int(year), int(month))
        else:
            return '{:04d}'.format(int(year))

    # If it's a positive integer, try a quick conversion to days or seconds since epoch
    try:
        interval = int(value)
        if interval > 100000: # assume seconds for a big number
            d = datetime.datetime.fromtimestamp(interval)
            return d.strftime("%Y-%m-%d")
        elif interval >= 2200: # assume days (cut out for years)
            d = datetime.datetime(1970, 1, 1) + datetime.timedelta(days=interval-1)
            return d.strftime("%Y-%m-%d")
    except (ValueError, TypeError,):
        pass

    # First, try our quick ISO date pattern, extended to support quarter notation
    value = normalise_space(value)
    result = _ISO_DATE_PATTERN.match(value)
    if not result:
        result = _SQL_DATETIME_PATTERN.match(value)
    if result:
        return make_date_string(
            result.group('year'),
            quarter=result.group('quarter'),
            month=result.group('month'),
            week=result.group('week'),
            day=result.group('day')
        )

    # Next, check for a timestamp, which will crash the datetime module
    if value.isnumeric() and len(value) >= 10:
        if len(value) >= 16:
            timestamp = int(value) / 1000000 # nanoseconds
        if len(value) >= 13:
            timestamp = int(value) / 1000 # milliseconds
        else:
            timestamp = int(value) # seconds
        d = datetime.datetime.utcfromtimestamp(timestamp)
        return d.date().isoformat()

    # revert to full date parsing
    # we parse the date twice, to detect any default values Python might have filled in
    date1 = dateutil.parser.parse(value, default=_DEFAULT_DATE_1, dayfirst=dayfirst)
    date2 = dateutil.parser.parse(value, default=_DEFAULT_DATE_2, dayfirst=dayfirst)
    day = date1.day if date1.day==date2.day else None
    month = date1.month if date1.month==date2.month else None
    year = date1.year if date1.year==date2.year else None

    # do some quick validation
    if year is None:
        if month is not None:
            year = datetime.datetime.now().year
        else:
            raise ValueError("Will not provide default year unless month is present: {}".format(value))
    if month is None and day is not None:
        raise ValueError("Will not provide default month: {}".format(value))

    return make_date_string(year=year, month=month, day=day)


def is_dict(value):
    """Test if a value is a Python dict.

    Args:
        value: the value to test

    Returns:
        bool: True if the value is a Python dict or similar map.

    """
    return isinstance(value, collections.abc.Mapping)


def is_list(value):
    """Test if a value is a Python sequence (other than a string)

    Args:
        value: the value to test

    Returns:
        bool: True if the values is a non-string sequence.

    """
    return isinstance(value, collections.abc.Sequence) and not isinstance(value, six.string_types)

