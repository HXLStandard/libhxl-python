"""
Utility functions for testing and normalising scalar-ish data types

@author: David Megginson
@organization: UNOCHA
@license: Public Domain
@date: Started April 2018
@see: U{http://hxlstandard.org}
"""

import collections, datetime, dateutil.parser, json, logging, re, six, unidecode

logger = logging.getLogger(__name__)

TOKEN_PATTERN = r'[A-Za-z][_0-9A-Za-z]*'
"""Regular-expression pattern for a single token."""

WHITESPACE_PATTERN = re.compile('\s+', re.MULTILINE)
"""Regular-expression pattern for multi-line whitespace."""

ISO_DATE_PATTERN = re.compile(
    '^(?P<year>[12]\d\d\d)(?:Q(?P<quarter>[1-4])|W(?P<week>\d\d?)|-(?P<month>\d\d?)(?:-(?P<day>\d\d?))?)?$',
    re.IGNORECASE
)
"""Regular expression for basic ISO 8601 dates, plus extension to recognise quarters."""

SQL_DATETIME_PATTERN = re.compile(
    '^(?P<year>[12]\d\d\d)-(?P<month>\d\d?)-(?P<day>\d\d?) \d\d?:\d\d?:\d\d?(?P<week>)?(?P<quarter>)?$'
)
"""Regular expression for SQL datetime.
Added dummy week and quarter params for compatibility with ISO_DATE_PATTERN.
"""

DEFAULT_DATE_1 = datetime.datetime(2015, 1, 1)

DEFAULT_DATE_2 = datetime.datetime(2016, 3, 3)

def normalise(s, col=None, dayfirst=True):
    """Intelligently normalise a value, optionally using the HXL hashtag for hints"""
    # TODO add lat/lon

    if col and col.tag == '#date':
        try:
            return normalise_date(s, dayfirst=dayfirst)
        except ValueError:
            pass

    # fall through
    try:
        return normalise_number(s)
    except ValueError:
        return normalise_string(s)


def typeof(s, col=None):
    """Determine the type of a column value"""
    if col and col.tag == '#date' and is_date(s):
        return 'date'
    elif is_number(s):
        return 'number'
    elif is_empty(s):
        return 'empty'
    else:
        return 'string'
    
def flatten(value, is_subitem=False):
    """Flatten potential lists and dictionaries"""

    # keep it simple for now
    if value is None:
        return ''
    elif is_list(value) or is_dict(value):
        return json.dumps(value)
    else:
        return str(value)

    
def is_truthy(s):
    """Check for a boolean-type true value
    @param s: the value to test
    @returns: True if the value is truthy
    """
    return normalise_string(s) in ['y', 'yes', 't', 'true', '1']


def is_empty(s):
    """Is this an empty value?
    None or whitespace only counts as empty; anything else doesn't.
    @param s: value to test
    @return: True if the value is empty
    """
    return (s is None or s == '' or str(s).isspace())


def is_string(v):
    """Test if a value is currently a string
    @param v: the value to test
    @returns: True if the value is a string
    """
    return isinstance(v, six.string_types)

def normalise_space(s):
    """Normalise whitespace only
    @param v: value to normalise
    @returns: string value with whitespace normalised
    """
    if is_empty(s):
        return ''
    else:
        s = str(s).strip().replace("\n", " ")
        return re.sub(
            WHITESPACE_PATTERN,
            ' ',
            s
        )

def normalise_string(s):
    """Normalise a string.
    Remove all leading and trailing whitespace. Convert to lower
    case. Replace all internal whitespace (including lineends) with a single space. Replace None with ''.
    @param s: the string to normalise
    @returns: the normalised string
    """
    if s is None:
        s = ''
    else:
        s = str(s)
    return normalise_space(unidecode.unidecode(s)).lower()


def is_number(v):
    """Test if a value contains something recognisable as a number.
    @param v: the value (string, int, float, etc) to test
    @returns: True if usable as a number
    @see: L{normalise_number}
    """
    try:
        float(v)
        return True
    except:
        return False

def normalise_number(v):
    """Attempt to convert a value to a number.
    Will convert to int type if it has no decimal places.
    @param v: the value (string, int, float, etc) to convert.
    @returns: an int or float value
    @exception ValueError: if the value cannot be converted
    @see: L{is_number}
    """
    try:
        n = float(v)
        if n == int(n):
            return int(n)
        else:
            return n
    except:
        raise ValueError("Cannot convert to number: {}".format(v))

def is_date(v):
    """Test if a value contains something recognisable as a date.
    @param v: the value (string, etc) to test
    @returns: True if usable as a date
    @see: L{normalise_date}
    """
    try:
        normalise_date(v)
        return True
    except ValueError:
        return False

def normalise_date(v, dayfirst=True):
    """Normalise a string as a date.
    @param s: the string to normalise as a date
    @returns: the date in ISO 8601 format or quarters (extension)
    @exception ValueError: if value cannot be parsed as a date
    @see: L{is_date}
    """

    def make_date(year, quarter=None, month=None, week=None, day=None):
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

    # First, try our quick ISO date pattern, extended to support quarter notation
    v = normalise_space(v)
    result = ISO_DATE_PATTERN.match(v)
    if not result:
        result = SQL_DATETIME_PATTERN.match(v)
    if result:
        return make_date(
            result.group('year'),
            quarter=result.group('quarter'),
            month=result.group('month'),
            week=result.group('week'),
            day=result.group('day')
        )

    # revert to full date parsing
    # we parse the date twice, to detect any default values Python might have filled in
    date1 = dateutil.parser.parse(v, default=DEFAULT_DATE_1, dayfirst=dayfirst)
    date2 = dateutil.parser.parse(v, default=DEFAULT_DATE_2, dayfirst=dayfirst)
    day = date1.day if date1.day==date2.day else None
    month = date1.month if date1.month==date2.month else None
    year = date1.year if date1.year==date2.year else None

    # do some quick validation
    if year is None:
        if month is not None:
            year = datetime.datetime.now().year
        else:
            raise ValueError("Will not provide default year unless month is present: {}".format(v))
    if month is None and day is not None:
        raise ValueError("Will not provide default month: {}", v)

    return make_date(year=year, month=month, day=day)

def is_dict(e):
    """Test if a value is a Python dict.
    @param e: the value to test
    @return: True if the value is a dict; False otherwise
    """
    return isinstance(e, collections.Mapping)

def is_list(e):
    """Test if a value is a Python sequence (other than a string)
    @param e: the value to test
    @return: True if the value is a sequence; False otherwise
    """
    return isinstance(e, collections.Sequence) and not isinstance(e, six.string_types)

