"""
Common information for HXL.
"""

import six

TOKEN = r'[A-Za-z][_0-9A-Za-z]*'

class HXLException(Exception):
    """Base class for all HXL-related exceptions."""

    def __init__(self, message):
        super(Exception, self).__init__(message)
        self.message = message

    def __str__(self):
        return "<HXException: " + str(self.message) + ">"

def normalise_string(s):
    return s.strip().lower().replace(r'\s\s+', ' ')

def pattern_list(patterns):
    """
    Normalise a pattern list:
    - if falsy, make into an empty list
    - if scalar, make into a one-item list
    - make sure each item is compiled into a tag pattern
    @param patterns a string or list of strings/TagPattern objects
    """
    from hxl.model import TagPattern
    if not patterns:
        return []
    elif isinstance(patterns, six.string_types):
        patterns = [patterns]
    return [TagPattern.parse(pattern) for pattern in patterns]
