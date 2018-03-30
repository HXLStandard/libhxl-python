"""
Common classes, variables, and functions for the HXL library.

@author: David Megginson
@organization: UNOCHA
@license: Public Domain
@date: Started April 2015
@see: U{http://hxlstandard.org}
"""

import collections, dateutil, re, six, unidecode


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
    """Normalise a number to an integer type if it is defacto an integer.
    If it is not a number, leave it alone.
    @param n: the number to normalise
    @return: the number as an integer, if possible; also '' instead of None
    """
    if n == '' or n is None:
        return ''
    if n == int(n):
        return int(n)
    else:
        return n


def is_empty(s):
    """Is this an empty value?
    None or whitespace only counts as empty; anything else doesn't.
    @param s: value to test
    @return: True if the value is empty
    """
    return (s is None or s == '' or s.isspace())


def is_number(s):
    """Can we parse this as a number?
    @param s: the string to test as a number
    @return: True if we can force to a float, False otherwise
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def normalise_date(s):
    """Normalise a string as a date.
    @param s: the string to normalise as a date
    @return: the date in ISO 8601 YYYY-mm-dd format, or False if we can't parse it as a date.
    """
    try:
        return dateutil.parser.parse(s).strftime('%Y-%m-%d')
    except:
        return False
    

def normalise_string(s):
    """Normalise a string.
    Remove all leading and trailing whitespace. Convert to lower
    case. Replace all internal whitespace (including lineends) with a single space. Replace None with ''.
    @param s: the string to normalise
    @return: the normalised string
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

    
def is_list(e):
    """Test if a value is a Python sequence (other than a string)
    @param e: the value to test
    @return: True if the value is a sequence; False otherwise"""
    if not isinstance(e, collections.Sequence) or isinstance(e, six.string_types):
        return False
    else:
        return True


def list_product(lists, head=[]):
    """Generate the cartesian product of a list of lists 
    The elements of the result will be all possible combinations of the elements of
    the input lists.
    @param lists: a list of lists
    @return: the cross-product of the lists
    """
    if lists:
        result = []
        for item in lists[0]:
            tail = list_product(lists[1:], head + [item])
            result = result + tail
        return result
    else:
        return [head]
