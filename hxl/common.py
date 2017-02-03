""" Common classes, variables, and functions for the HXL library.

@author: David Megginson
@organization: UNOCHA
@license: Public Domain
@date: Started April 2015
@see: U{hxlstandard.org}

"""

import re, unidecode, sys, dateutil

TOKEN_PATTERN = r'[A-Za-z][_0-9A-Za-z]*'
"""Regular-expression pattern for a single token."""

WHITESPACE_PATTERN = re.compile('\s+', re.MULTILINE)
"""Regular-expression pattern for multi-line whitespace."""

class HXLException(Exception):
    """Base class for all HXL-related exceptions."""

    def __init__(self, message, data={}):
        """Create a new HXL exception.

        @param message: error message for the exception
        @param data: dict of properties associated with the exception (default {})
        """
        super(Exception, self).__init__(message)
        self.message = message
        """The human-readable error message."""
        self.data = data
        """Additional properties related to the error."""

    def __str__(self):
        return "<HXLException: " + str(self.message) + ">"

def normalise_number(n):
    if n == '' or n is None:
        return ''
    if n == int(n):
        return int(n)
    else:
        return n

def is_empty(s):
    """Is this an empty value?
    None or whitespace only counts as empty; anything else doesn't.
    @param s: value to test.
    @returns: True if empty
    """
    return (s is None or s == '' or s.isspace())

def is_number(s):
    """Can we parse this as a number?"""
    try:
        float(s)
        return True
    except ValueError:
        return False

def normalise_date(s):
    """Normalise a date.

    Return the normalised version, or False if this can't be parsed as a date.
    """
    try:
        return dateutil.parser.parse(s).strftime('%Y-%m-%d')
    except:
        return False
    

def normalise_string(s):
    """Normalise a string.

    Remove all leading and trailing whitespace. Convert to lower
    case. Replace all internal whitespace (including lineends) with a single space.

    @param s: the string to normalise.
    """
    if s:
        # basic whitespace cleanups
        s = str(s).strip().lower().replace("\n", " ")

        # Python2 Unicode hack
        if sys.version_info[0] < 3:
            s = unicode(s, 'utf-8')

        # Normalise whitespace and return
        return re.sub(
            WHITESPACE_PATTERN,
            ' ',
            unidecode.unidecode(s)
        )
    else:
        return ''

