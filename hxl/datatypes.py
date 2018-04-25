"""
Utility functions for testing and normalising scalar-ish data types

@author: David Megginson
@organization: UNOCHA
@license: Public Domain
@date: Started April 2018
@see: U{http://hxlstandard.org}
"""

import collections, datetime, dateutil, re, six, unidecode


TOKEN_PATTERN = r'[A-Za-z][_0-9A-Za-z]*'
"""Regular-expression pattern for a single token."""

WHITESPACE_PATTERN = re.compile('\s+', re.MULTILINE)
"""Regular-expression pattern for multi-line whitespace."""

ISO_DATE_PATTERN = re.compile('^(?P<year>[12]\d\d\d)(?:Q(?P<quarter>[1-4])|W(?P<week>\d\d?)|-(?P<month>\d\d?)(?:-(?P<day>\d\d?))?)?$', re.IGNORECASE)
"""Regular expression for basic ISO 8601 dates, plus extension to recognise quarters."""

DEFAULT_DATE_1 = datetime.date(2015, 1, 1)

DEFAULT_DATE_2 = datetime.date(2016, 2, 2)

def normalise(s, col=None):
    """Intelligently normalise a value, optionally using the HXL hashtag for hints"""
    # TODO add lat/lon
    if col and col.tag == '#date' and is_date(s):
        return normalise_date(s)
    elif is_number(s):
        return normalise_number(s)
    else:
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

    # handle scalar values, escaping special list characters
    if not hasattr(value, '__len__') or isinstance(value, six.string_types):
        if value is None:
            return ''
        else:
            return str(value).replace('\\', '\\\\').replace('{', '\\{').replace('=', '\\=').replace(',', '\\,') # already scalar

    # handle JSON arrays and objects
    elements = []
    if isinstance(value, dict):
        for key in value:
            elements.append(flatten(key, True) + '=' + flatten(value[key], True))
    else:
        for item in value:
            elements.append(flatten(item, True))
    if is_subitem:
        return '{' + ','.join(elements) + '}'
    else:
        return ','.join(elements)
    
def is_empty(s):
    """Is this an empty value?
    None or whitespace only counts as empty; anything else doesn't.
    @param s: value to test
    @return: True if the value is empty
    """
    return (s is None or s == '' or str(s).isspace())

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
    n = float(v)
    if n == int(n):
        return int(n)
    else:
        return n

def is_date(v):
    """Test if a value contains something recognisable as a date.
    @param v: the value (string, etc) to test
    @returns: True if usable as a date
    @see: L{normalise_date}
    """
    v = normalise_string(v)
    if ISO_DATE_PATTERN.match(v):
        return True
    try:
        dateutil.parser.parse(normalise_string(v))
        return True
    except:
        return False

def normalise_date(v):
    """Normalise a string as a date.
    @param s: the string to normalise as a date
    @returns: the date in ISO 8601 format or quarters (extension)
    @exception ValueError: if value cannot be parsed as a date
    @see: L{is_date}
    """

    def make_date(year, quarter=None, month=None, week=None, day=None):
        if quarter:
            # *not* real ISO 8601
            return '{:04d}Q{:01d}'.format(int(year), int(quarter))
        elif week:
            return '{:04d}W{:02d}'.format(int(year), int(week))
        elif month:
            if day:
                return '{:04d}-{:02d}-{:02d}'.format(int(year), int(month), int(day))
            else:
                return '{:04d}-{:02d}'.format(int(year), int(month))
        else:
            return '{:04d}'.format(int(year))
        

    # First, try our quick ISO date pattern, extended to support quarter notation
    result = ISO_DATE_PATTERN.match(v)
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
    date1 = dateutil.parser.parse(v, default=DEFAULT_DATE_1)
    date2 = dateutil.parser.parse(v, default=DEFAULT_DATE_2)
    return make_date(
        date1.year,
        month=(date1.month if date1.month==date2.month else None),
        day=(date1.day if date1.day==date2.day else None)
    )
    

def is_list(e):
    """Test if a value is a Python sequence (other than a string)
    @param e: the value to test
    @return: True if the value is a sequence; False otherwise"""
    if not isinstance(e, collections.Sequence) or isinstance(e, six.string_types):
        return False
    else:
        return True

