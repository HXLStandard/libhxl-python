""" Common classes, variables, and functions for the HXL library.

@author: David Megginson
@organization: UNOCHA
@license: Public Domain
@date: Started April 2015

"""

import re

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

def normalise_string(s):
    """Normalise a string.  

    Remove all leading and trailing whitespace. Convert to lower
    case. Replace all internal whitespace (including lineends) with a single space.

    @param s: the string to normalise.
    """
    if s:
        return re.sub(WHITESPACE_PATTERN, ' ', str(s).strip().lower().replace("\n", " "))
    else:
        return ''

