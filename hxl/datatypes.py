"""
Utility functions for scalar data types

@author: David Megginson
@organization: UNOCHA
@license: Public Domain
@date: Started April 2018
@see: U{http://hxlstandard.org}
"""

import collections, dateutil, re, six, unidecode


TOKEN_PATTERN = r'[A-Za-z][_0-9A-Za-z]*'
"""Regular-expression pattern for a single token."""


WHITESPACE_PATTERN = re.compile('\s+', re.MULTILINE)
"""Regular-expression pattern for multi-line whitespace."""

ISO_DATE_PATTERN = re.compile('^(?P<year>[12]\d\d\d)(?:Q(?P<quarter>[1-4])|W(?P<week>\d\d?)|-(?P<month>\d\d?)(?:-(?P<day>\d\d?))?)?$', re.IGNORECASE)
"""Regular expression for basic ISO 8601 dates, plus extension to recognise quarters."""


def is_empty(s):
    """Is this an empty value?
    None or whitespace only counts as empty; anything else doesn't.
    @param s: value to test
    @return: True if the value is empty
    """
    return (s is None or s == '' or str(s).isspace())


def normalise_string(s):
    """Normalise a string.
    Remove all leading and trailing whitespace. Convert to lower
    case. Replace all internal whitespace (including lineends) with a single space. Replace None with ''.
    @param s: the string to normalise
    @returns: the normalised string
    """
    if s is not None:
        # basic whitespace cleanups
        s = str(s).strip().lower().replace("\n", " ")

        # Normalise whitespace and return
        return re.sub(
            WHITESPACE_PATTERN,
            ' ',
            unidecode.unidecode(s)
        )
    else:
        return ''

    
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

def normalise_date(s):
    """Normalise a string as a date.
    @param s: the string to normalise as a date
    @returns: the date in ISO 8601 YYYY-mm-dd format, or False if we can't parse it as a date.
    """
    try:
        return dateutil.parser.parse(s).strftime('%Y-%m-%d')
    except:
        return False
    

def is_list(e):
    """Test if a value is a Python sequence (other than a string)
    @param e: the value to test
    @return: True if the value is a sequence; False otherwise"""
    if not isinstance(e, collections.Sequence) or isinstance(e, six.string_types):
        return False
    else:
        return True

