"""
Filter submodule for libhxl.
David Megginson
Started February 2015

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import re
from hxl import HXLException
from hxl.io import StreamInput, URLInput
from hxl.model import HXLDataProvider, HXLColumn

class HXLFilterException(HXLException):
    pass

class TagPattern(object):
    """
    Pattern for matching a tag.

    #tag matches #tag with any attributes
    #tag+foo matches #tag with foo among its attributes
    #tag-foo matches #tag with foo *not* among its attributes
    #tag+foo-bar matches #tag with foo but not bar
    """

    def __init__(self, tag, include_attributes=None, exclude_attributes=None):
        """Like a column, but has a whitelist and a blacklist."""
        self.tag = tag
        self.include_attributes = include_attributes
        self.exclude_attributes = exclude_attributes

    def match(self, column):
        """Check whether a HXLColumn matches this pattern."""
        if self.tag == column.tag:
            # all include_attributes must be present
            if self.include_attributes:
                for attribute in self.include_attributes:
                    if attribute not in column.attributes:
                        return False
            # all exclude_attributes must be absent
            if self.exclude_attributes:
                for attribute in self.exclude_attributes:
                    if attribute in column.attributes:
                        return False
            return True
        else:
            return False

    def find_column_index(self, columns):
        """Get the index of the first matching column."""
        for i in range(len(columns)):
            if self.match(columns[i]):
                return i
        return None

    def find_column(self, columns):
        """Check whether there is a match in a list of columns."""
        for column in columns:
            if self.match(column):
                return column
        return None

    def get_value(self, row):
        """Return the first matching value for this pattern."""
        for i in range(min(len(row.columns), len(row.values))):
            if self.match(row.columns[i]):
                return row.values[i]
        return None

    def __repr__(self):
        s = self.tag
        if self.include_attributes:
            for attribute in self.include_attributes:
                s += '+' + attribute
        if self.exclude_attributes:
            for attribute in self.exclude_attributes:
                s += '-' + attribute
        return s

    __str__ = __repr__

    @staticmethod
    def parse(s):
        """Parse a single tagspec, like #tag+foo-bar."""
        result = re.match(r'^\s*#?([A-Za-z][_0-9A-Za-z]*)((?:[+-][A-Za-z][_0-9A-Za-z]*)*)\s*$', s)
        if result:
            tag = '#' + result.group(1)
            include_attributes = []
            exclude_attributes = []
            attribute_specs = re.split(r'([+-])', result.group(2))
            for i in range(1, len(attribute_specs), 2):
                if attribute_specs[i] == '+':
                    include_attributes.append(attribute_specs[i + 1])
                else:
                    exclude_attributes.append(attribute_specs[i + 1])
            return TagPattern(tag, include_attributes=include_attributes, exclude_attributes=exclude_attributes)
        else:
            raise HXLFilterException('Malformed tag: ' + s)

    @staticmethod
    def parse_list(s):
        """Parse a comma-separated list of tagspecs."""
        return [TagPattern.parse(spec) for spec in s.split(',')]


def run_script(func):
    """Try running a command-line script, with exception handling."""
    try:
        func(sys.argv[1:], sys.stdin, sys.stdout)
    except HXLException:
        print >>sys.stderr, "Fatal error (" + e.__class__.__name__ + "): " + str(e.message)
        print >>sys.stderr, "Exiting ..."
        sys.exit(2)
    except KeyboardInterrupt:
        print >>sys.stderr, "Interrupted"
        sys.exit(2)

def make_input(filename, stdin=sys.stdin):
    """Make an input from the specified file, if any."""
    if filename:
        return URLInput(filename)
    else:
        return StreamInput(stdin)

def make_output(filename, stdout=sys.stdout):
    if filename:
        return FileOutput(filename)
    else:
        return StreamOutput(stdout)

class FileOutput:

    def __init__(self, filename):
        self.output = open(filename, 'w')

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        close(self.output)

class StreamOutput:

    def __init__(self, output):
        self.output = output

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        pass
