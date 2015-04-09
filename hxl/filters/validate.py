"""
Command function to schema-validate a HXL dataset.
David Megginson
November 2014

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import os
import argparse
from copy import copy
from hxl.model import Dataset, Column
from hxl.io import StreamInput, HXLReader, write_hxl
from hxl.schema import read_schema

class ValidateFilter(Dataset):
    """Composable filter class to validate a HXL dataset against a schema.

    This is the class supporting the hxlvalidate command-line utility.

    Because this class is a {@link hxl.model.Dataset}, you can use
    it as the source to an instance of another filter class to build a
    dynamic, singled-threaded processing pipeline.

    Usage:

    <pre>
    source = HXLReader(sys.stdin)
    schema = read_schema(read_hxl(open('my-schema.csv', 'r')))
    filter = ValidateFilter(source, schema)
    write_hxl(sys.stdout, filter)
    </pre>
    """

    def __init__(self, source, schema, show_all=False):
        """
        @param source a HXL data source
        @param schema a Schema object
        @param show_all boolean flag to report all lines (including those without errors).
        """
        self.source = source
        self.schema = schema
        self.show_all = show_all
        self._saved_columns = None

    @property
    def columns(self):
        """
        Add columns for the error reporting.
        """
        if self._saved_columns is None:
            # append error columns
            err_col = Column(tag='#x_errors', header='Error messages')
            tag_col = Column(tag='#x_tags', header='Error tag')
            row_col = Column(tag='#x_rows', header='Error row number (source)')
            col_col = Column(tag='#x_cols', header='Error column number (source)')
            self._saved_columns = self.source.columns + [err_col, tag_col, row_col, col_col]
        return self._saved_columns

    def __iter__(self):
        return ValidateFilter.Iterator(self)

    class Iterator:

        def __init__(self, outer):
            self.outer = outer
            self.iterator = iter(outer.source)

        def __next__(self):
            """
            Report rows with error information.
            """
            validation_errors = []
            def callback(error):
                """
                Collect validation errors
                """
                validation_errors.append(error)
            self.outer.schema.callback = callback

            """
            Read rows until we find an error (unless we're printing all rows)
            """
            row = next(self.iterator)
            while row:
                if not self.outer.schema.validate_row(row) or self.outer.show_all:
                    # append error data to row
                    error_row = copy(row)
                    messages = "\n".join(map(lambda e: e.message, validation_errors))
                    tags = "\n".join(map(lambda e: str(e.rule.tag_pattern) if e.rule else '', validation_errors))
                    rows = "\n".join(map(lambda e: str(e.row.source_row_number) if e.row else '', validation_errors))
                    error_row.columns = self.outer.columns
                    error_row.values = error_row.values + [messages, tags, rows]
                    return error_row
                else:
                    row = next(self.iterator)

        next = __next__


def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlvalidate with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Validate a HXL dataset.')
    parser.add_argument(
        'infile',
        help='HXL file to read (if omitted, use standard input).',
        nargs='?',
        type=argparse.FileType('r'),
        default=stdin
        )
    parser.add_argument(
        'outfile',
        help='HXL file to write (if omitted, use standard output).',
        nargs='?',
        type=argparse.FileType('w'),
        default=stdout
        )
    parser.add_argument(
        '-s',
        '--schema',
        help='Schema file for validating the HXL dataset (if omitted, use the default core schema).',
        metavar='schema',
        type=argparse.FileType('r'),
        default=None
        )
    parser.add_argument(
        '-a',
        '--all',
        help='Include all rows in the output, including those without errors',
        action='store_const',
        const=True,
        default=False
        )
    args = parser.parse_args(args)

    with args.infile, args.outfile:
        source = HXLReader(StreamInput(args.infile))
        if args.schema:
            with args.schema:
                schema = read_schema(HXLReader(StreamInput(args.schema)), base_dir=os.path.dirname(args.schema.name))
        else:
            schema = read_schema()
        filter = ValidateFilter(source, schema, args.all)
        write_hxl(args.outfile, filter)

# end
