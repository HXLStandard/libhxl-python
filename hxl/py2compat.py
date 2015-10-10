"""
Compatibility classes for Python2
David megginson
Started October 2015

License: Public Domain
"""

from __future__ import absolute_import

import io

class InputStreamWrapper(io.BufferedIOBase):
    """Wrap a Python2 input file object in a Python3 io object."""

    def __init__(self, stream):
        self.stream = stream

    def close(self):
        self.stream.close()

    @property
    def closed(self):
        if hasattr(self.stream, 'closed'):
            return self.stream.closed
        else:
            return False

    def fileno(self):
        return self.stream.fileno()

    def flush(self):
        if hasattr(self.stream, 'flush'):
            self.stream.flush()

    def isatty(self):
        return self.stream.isatty()

    def readable(self):
        return True

    def readline(self, limit=-1):
        return self.stream.readline(limit)

    def readlines(self, hint=-1):
        return self.stream.readlines(hint)

    def seek(self, offset, whence=0):
        return self.stream.seek(offset, whence)

    def tell(self):
        return self.stream.tell()

    def writable(self):
        return False

    def read(self, n=-1):
        return self.stream.read(n)

# end
