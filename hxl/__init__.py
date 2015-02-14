"""
Top-level package/module for libhxl.
David Megginson
Started February 2015

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

class HXLException(Exception):
    """Base class for all HXL-related exceptions."""

    def __init__(self, message):
        super(Exception, self).__init__(message)
        self.message = message

    def __str__(self):
        return "<HXException: " + self.message + ">"

# end


        
