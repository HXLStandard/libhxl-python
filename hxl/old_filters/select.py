"""
Select rows from a HXL dataset.
David Megginson
October 2014

Supply a list of simple <hashtag><operator><value> pairs, and return
the rows in the HXL dataset that contain matches for any of them.

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import re
import operator
from hxl.model import Dataset, TagPattern


def operator_re(s, pattern):
    """Regular-expression comparison operator."""
    return re.match(pattern, s)

def operator_nre(s, pattern):
    """Regular-expression negative comparison operator."""
    return not re.match(pattern, s)

def norm(s):
    return s.strip().lower().replace(r'\s\s+', ' ')


class Query(object):
    """Query to execute against a row of HXL data."""

    def __init__(self, pattern, op, value):
        self.pattern = pattern
        self.op = op
        self.value = value
        self._saved_indices = None
        try:
            float(value)
            self._is_numeric = True
        except:
            self._is_numeric = False

    def match_row(self, row):
        """Check if a key-value pair appears in a HXL row"""
        indices = self._get_saved_indices(row.columns)
        length = len(row.values)
        for i in indices:
            if i < length and row.values[i] and self.match_value(row.values[i]):
                    return True
        return False

    def match_value(self, value):
        """Try an operator as numeric first, then string"""
        # TODO add dates
        # TODO use knowledge about HXL tags
        if self._is_numeric:
            try:
                return self.op(float(value), float(self.value))
            except ValueError:
                pass
        return self.op(norm(value), norm(self.value))

    def _get_saved_indices(self, columns):
        """Cache the column tests, so that we run them only once."""
        # FIXME - assuming that the columns never change
        if self._saved_indices is None:
            self._saved_indices = []
            for i in range(len(columns)):
                if self.pattern.match(columns[i]):
                    self._saved_indices.append(i)
        return self._saved_indices

    @staticmethod
    def parse(s):
        """Parse a filter expression"""
        if isinstance(s, Query):
            # already parsed
            return s
        parts = re.split(r'([<>]=?|!?=|!?~)', s, maxsplit=1)
        pattern = TagPattern.parse(parts[0])
        op = Query.OPERATOR_MAP[parts[1]]
        value = parts[2]
        return Query(pattern, op, value)

    # Constant map of comparison operators
    OPERATOR_MAP = {
        '=': operator.eq,
        '!=': operator.ne,
        '<': operator.lt,
        '<=': operator.le,
        '>': operator.gt,
        '>=': operator.ge,
        '~': operator_re,
        '!~': operator_nre
    }
        

class RowFilter(Dataset):
    """
    Composable filter class to select rows from a HXL dataset.

    This is the class supporting the hxlselect command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = RowFilter(source, queries=[(TagPattern.parse('#org'), operator.eq, 'OXFAM')])
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, queries=[], reverse=False):
        """
        Constructor
        @param source the HXL data source
        @param queries a series for parsed queries
        @param reverse True to reverse the sense of the select
        """
        self.source = source
        if not hasattr(queries, '__len__') or isinstance(queries, str):
            # make a list if needed
            queries = [queries]
        self.queries = [Query.parse(query) for query in queries]
        self.reverse = reverse

    @property
    def columns(self):
        """Pass on the source columns unmodified."""
        return self.source.columns

    def __iter__(self):
        return RowFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __next__(self):
            """
            Return the next row that matches the select.
            """
            row = next(self.iterator)
            while not self.match_row(row):
                row = next(self.iterator)
            return row

        next = __next__

        def match_row(self, row):
            """Check if any of the queries matches the row (implied OR)."""
            for query in self.outer.queries:
                if query.match_row(row):
                    return not self.outer.reverse
            return self.outer.reverse


# end
