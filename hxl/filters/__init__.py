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
from hxl.io import StreamInput, CSVInput, ExcelInput
from hxl.model import Dataset, Column

class HXLFilterException(HXLException):
    pass

def run_script(func):
    """Try running a command-line script, with exception handling."""
    try:
        func(sys.argv[1:], sys.stdin, sys.stdout)
    except HXLException as e:
        print >>sys.stderr, "Fatal error (" + e.__class__.__name__ + "): " + str(e.message)
        print >>sys.stderr, "Exiting ..."
        sys.exit(2)
    except KeyboardInterrupt:
        print >>sys.stderr, "Interrupted"
        sys.exit(2)

def make_input(filename, stdin=sys.stdin):
    """Make an input from the specified file, if any."""
    if filename:
        if re.match('^.*\.xlsx?$', filename):
            return ExcelInput(filename)
        else:
            return CSVInput(filename)
    else:
        return StreamInput(stdin)

def make_output(filename, stdout=sys.stdout):
    if filename:
        return FileOutput(filename)
    else:
        return StreamOutput(stdout)

class FileOutput(object):

    def __init__(self, filename):
        self.output = open(filename, 'w')

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        close(self.output)

class StreamOutput(object):

    def __init__(self, output):
        self.output = output

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        pass
