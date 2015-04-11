"""
Command function to cache a dataset in memory.
David Megginson
April 2015

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

from copy import copy
from hxl.model import Dataset

class CacheFilter(Dataset):
    """Composable filter class to cache HXL data in memory."""

    def __init__(self, source, max_rows=None):
        """
        Constructor
        @param max_rows If >0, maximum number of rows to cache.
        """
        self.source = source
        self.max_rows = max_rows
        self.cached_columns = copy(source.columns)
        self.cached_rows = [copy(row) for row in source]
        self.overflow = False

    @property
    def columns(self):
        return self.cached_columns

    def __iter__(self):
        return iter(self.cached_rows)

    def _load(self):
        if self.cached_rows is None:
            self.cached_rows = []
            self.cached_columns = copy(self.source.columns)
            row_count = 0
            for row in self.source:
                row_count += 1
                if self.max_rows > 1 and row_count >= self.max_rows:
                    self.overflow = True
                    break
                else:
                    self.cached_rows.append(copy(row))
