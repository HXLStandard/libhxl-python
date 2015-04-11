"""
Command function to rename columns in a HXL dataset.
David Megginson
October 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import copy
import re
import hxl
from hxl.model import Dataset, TagPattern, Column


class RenameFilter(Dataset):
    """
    Composable filter class to rename columns in a HXL dataset.

    This is the class supporting the hxlrename command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, single-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    filter = RenameFilter(source, rename=[[TagPattern.parse('#foo'), Column.parse('#bar')]])
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, rename=[]):
        """
        Constructor
        @param source the Dataset for the data.
        @param rename_map map of tags to rename
        """
        self.source = source
        self.rename = rename
        self._saved_columns = None

    @property
    def columns(self):
        """
        Return the renamed columns.
        """

        if self._saved_columns is None:
            def rename_column(column):
                for spec in self.rename:
                    if spec[0].match(column):
                        new_column = copy.copy(spec[1])
                        if new_column.header is None:
                            new_column.header = column.header
                        return new_column
                return column
            self._saved_columns = [rename_column(column) for column in self.source.columns]
        return self._saved_columns

    def __iter__(self):
        return iter(self.source)

    RENAME_PATTERN = r'^\s*#?({token}):(?:([^#]*)#)?({token})\s*$'.format(token=hxl.common.TOKEN)

    @staticmethod
    def parse_rename(s):
        result = re.match(RenameFilter.RENAME_PATTERN, s)
        if result:
            pattern = TagPattern.parse(result.group(1))
            column = Column.parse('#' + result.group(3), header=result.group(2), use_exception=True)
            return (pattern, column)
        else:
            raise HXLFilterException("Bad rename expression: " + s)

# end
