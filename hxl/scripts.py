"""
Console scripts
David Megginson
April 2015

This is a big, ugly module to support the libhxl
console scripts, including (mainly) argument parsing.

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import sys
import re
import argparse
import json
from shapely.geometry import shape

import hxl

from hxl.filters import AddColumnsFilter, AppendFilter, CleanDataFilter, \
    ColumnFilter, CountFilter, DeduplicationFilter, MergeDataFilter, RenameFilter, \
    ReplaceDataFilter, RowFilter, SortFilter, ValidateFilter
from hxl.converters import Tagger

# In Python2, sys.stdin is a byte stream; in Python3, it's a text stream
if sys.version_info < (3,):
    STDIN = sys.stdin
else:
    STDIN = sys.stdin.buffer

    
#
# Console script entry points
#

def hxladd():
    """Console script for hxladd."""
    run_script(hxladd_main)

def hxlappend():
    """Console script for hxlappend."""
    run_script(hxlappend_main)

def hxlbounds():
    """Console script for hxlbounds."""
    run_script(hxlbounds_main)

def hxlclean():
    """Console script for hxlclean."""
    run_script(hxlclean_main)

def hxlcount():
    """Console script for hxlcount."""
    run_script(hxlcount_main)

def hxlcut():
    """Console script for hxlcut."""
    run_script(hxlcut_main)

def hxldedup():
    """Console script for hxldedup."""
    run_script(hxldedup_main)

def hxlmerge():
    """Console script for hxlmerge."""
    run_script(hxlmerge_main)

def hxlrename():
    """Console script for hxlrename."""
    run_script(hxlrename_main)

def hxlreplace():
    """Console script for hxlreplace."""
    run_script(hxlreplace_main)

def hxlselect():
    """Console script for hxlselect."""
    run_script(hxlselect_main)

def hxlsort():
    """Console script for hxlsort."""
    run_script(hxlsort_main)

def hxltag():
    """Console script for hxltag."""
    run_script(hxltag_main)

def hxlvalidate():
    """Console script for hxlvalidate."""
    run_script(hxlvalidate_main)


#
# Main scripts for command-line tools.
#

def hxladd_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxladd with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Add new columns with constant values to a HXL dataset.')
    parser.add_argument(
        '-s',
        '--spec',
        help='Constant value to add to each row (may repeat option)',
        metavar='header#<tag>=<value>',
        action='append',
        required=True
        )
    parser.add_argument(
        '-b',
        '--before',
        help='Add new columns before existing ones rather than after them.',
        action='store_const',
        const=True,
        default=False
    )
        
    args = parser.parse_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = AddColumnsFilter(source, specs=args.spec, before=args.before)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxlappend_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlappend with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Concatenate two HXL datasets')
    # repeatable argument
    parser.add_argument(
        '-a',
        '--append',
        help='HXL file to append (may repeat option).',
        metavar='file_or_url',
        action='append',
        required=True
        )
    parser.add_argument(
        '-x',
        '--exclude-extra-columns',
        help='Don not add extra columns not in the original dataset.',
        action='store_const',
        const=True,
        default=False
    )
        
    args = parser.parse_args(args)

    def append_data(source, output, urls):
        """Recursively process all append files."""
        if not urls:
            hxl.write_hxl(output.output, source, not args.strip_tags)
        else:
            with hxl.data(urls.pop(), True) as append_source:
                filter = AppendFilter(source, append_source, not args.exclude_extra_columns)
                append_data(filter, output, urls)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        # use a recursive function so that we can close all sources on the way out
        append_data(source, output, args.append)


def hxlbounds_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """Check that all lat/lon coordinates appear within bounds."""
    
    parser = make_args('Perform bounds checking on a HXL dataset.')
    parser.add_argument(
        '-b',
        '--bounds',
        help='GeoJSON file containing the boundary information.',
        required=True,
        type=argparse.FileType('r')
        )
    parser.add_argument(
        '-c',
        '--tags',
        help='Comma-separated list of column tags to include in error reports',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list,
        default='loc,org,sector,adm1,adm2,adm3'
        )
    args = parser.parse_args(args)

    data = json.load(args.bounds)
    shapes = []
    for d in data['features']:
        shapes.append(shape(d['geometry']))

    # Call the command function
    hxl.converters.hxlbounds(args.infile, args.outfile, shapes, tags=args.tags)


def hxlclean_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlclean with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Clean data in a HXL file.')
    parser.add_argument(
        '-W',
        '--whitespace-all',
        help='Normalise whitespace in all columns',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-w',
        '--whitespace',
        help='Comma-separated list of tags for normalised whitespace.',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-u',
        '--upper',
        help='Comma-separated list of tags to convert to uppercase.',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-l',
        '--lower',
        help='Comma-separated list of tags to convert to lowercase.',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-D',
        '--date-all',
        help='Normalise all dates.',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-d',
        '--date',
        help='Comma-separated list of tags for date normalisation.',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-N',
        '--number-all',
        help='Normalise all numbers.',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-n',
        '--number',
        help='Comma-separated list of tags for number normalisation.',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-r',
        '--remove-headers',
        help='Remove text header row above HXL hashtags',
        action='store_const',
        const=False,
        default=True
        )
    args = parser.parse_args(args)
    
    with make_source(args, stdin) as source, make_output(args, stdout) as output:

        if args.whitespace_all:
            whitespace_arg = True
        else:
            whitespace_arg = args.whitespace

        if args.date_all:
            date_arg = True
        else:
            date_arg = args.date

        if args.number_all:
            number_arg = True
        else:
            number_arg = args.number

        filter = CleanDataFilter(source, whitespace=whitespace_arg, upper=args.upper, lower=args.lower, date=date_arg, number=number_arg)
        hxl.write_hxl(output.output, filter, args.remove_headers, show_tags= not args.strip_tags)


def hxlcount_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcount with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = make_args('Generate aggregate counts for a HXL dataset')
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to count.',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list,
        default='loc,org,sector,adm1,adm2,adm3'
        )
    parser.add_argument(
        '-a',
        '--aggregate',
        help='Hashtag to aggregate.',
        metavar='tag',
        type=hxl.TagPattern.parse
        )

    args = parser.parse_args(args)
    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = CountFilter(source, args.tags, args.aggregate)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxlcut_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    parser = make_args('Cut columns from a HXL dataset.')
    parser.add_argument(
        '-i',
        '--include',
        help='Comma-separated list of column tags to include',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-x',
        '--exclude',
        help='Comma-separated list of column tags to exclude',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    args = parser.parse_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = ColumnFilter(source, args.include, args.exclude)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxldedup_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    parser = make_args('Remove duplicate rows from a HXL dataset.')
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to use for deduplication (by default, use all values).',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    args = parser.parse_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = DeduplicationFilter(source, args.tags)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxlmerge_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlmerge with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Merge part of one HXL dataset into another.')
    parser.add_argument(
        '-m',
        '--merge',
        help='HXL file to write (if omitted, use standard output).',
        metavar='filename',
        required=True
        )
    parser.add_argument(
        '-k',
        '--keys',
        help='HXL tag(s) to use as a shared key.',
        metavar='tag,tag...',
        required=True,
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to include from the merge dataset.',
        metavar='tag,tag...',
        required=True,
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-r',
        '--replace',
        help='Replace empty values in existing columns (when available) instead of adding new ones.',
        action='store_const',
        const=True,
        default=False
    )
    parser.add_argument(
        '-O',
        '--overwrite',
        help='Used with --replace, overwrite existing values.',
        action='store_const',
        const=True,
        default=False
    )
    args = parser.parse_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output, hxl.data(args.merge, True) if args.merge else None as merge_source:
        filter = MergeDataFilter(source, merge_source=merge_source,
                             keys=args.keys, tags=args.tags, replace=args.replace, overwrite=args.overwrite)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxlrename_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlrename with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Rename and retag columns in a HXL dataset')
    parser.add_argument(
        '-r',
        '--rename',
        help='Rename an old tag to a new one, with an optional new text header (may repeat option).',
        action='append',
        metavar='#?<original_tag>:<Text header>?#?<new_tag>',
        default=[],
        type=RenameFilter.parse_rename
        )
    args = parser.parse_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = RenameFilter(source, args.rename)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxlreplace_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlreplace with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Replace strings in a HXL dataset')

    inline_group = parser.add_argument_group('Inline replacement')
    map_group = parser.add_argument_group('External substitution map')

    inline_group.add_argument(
        '-p',
        '--pattern',
        help='String or regular expression to search for',
        nargs='?'
        )
    inline_group.add_argument(
        '-s',
        '--substitution',
        help='Replacement string',
        nargs='?'
        )
    inline_group.add_argument(
        '-t',
        '--tags',
        help='Tag patterns to match',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    inline_group.add_argument(
        '-r',
        '--regex',
        help='Use a regular expression instead of a string',
        action='store_const',
        const=True,
        default=False
        )
    map_group.add_argument(
        '-m',
        '--map',
        help='Filename or URL of a mapping table using the tags #x_pattern (required), #x_substitution (required), #x_tag (optional), and #x_regex (optional), corresponding to the inline options above, for multiple substitutions.',
        metavar='PATH',
        nargs='?'
        )
    args = parser.parse_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        if args.map:
            replacements = ReplaceDataFilter.Replacement.parse_map(hxl.data(args.map, True))
        else:
            replacements = []
        if args.pattern:
            for tag in args.tags:
                replacements.append(ReplaceDataFilter.Replacement(args.pattern, args.substitution, tag, args.regex))
        filter = ReplaceDataFilter(source, replacements)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxlselect_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlselect with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = make_args('Filter rows in a HXL dataset.')
    parser.add_argument(
        '-q',
        '--query',
        help='Query expression for selecting rows (may repeat option for logical OR). <op> may be =, !=, <, <=, >, >=, ~, or !~',
        action='append',
        metavar='<hashtag><op><value>',
        required=True
        )
    parser.add_argument(
        '-r',
        '--reverse',
        help='Show only lines *not* matching criteria',
        action='store_const',
        const=True,
        default=False
        )
    args = parser.parse_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = RowFilter(source, queries=args.query, reverse=args.reverse)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxlsort_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcut with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Sort a HXL dataset.')
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of tags to for columns to use as sort keys.',
        metavar='tag,tag...',
        type=hxl.TagPattern.parse_list
        )
    parser.add_argument(
        '-r',
        '--reverse',
        help='Flag to reverse sort order.',
        action='store_const',
        const=True,
        default=False
        )
    args = parser.parse_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = SortFilter(source, args.tags, args.reverse)
        hxl.write_hxl(output.output, filter, show_tags=not args.strip_tags)


def hxltag_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxltag with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Add HXL tags to a raw CSV file.')
    parser.add_argument(
        '-m',
        '--map',
        help='Mapping expression',
        required=True,
        action='append',
        metavar='Header Text#tag',
        type=Tagger.parse_spec
        )
    args = parser.parse_args(args)

    with hxl.make_input(args.infile or stdin) as input, make_output(args, stdout) as output:
        tagger = Tagger(input, args.map)
        hxl.write_hxl(output.output, hxl.data(tagger), show_tags=not args.strip_tags)


def hxlvalidate_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlvalidate with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Validate a HXL dataset.')
    parser.add_argument(
        '-s',
        '--schema',
        help='Schema file for validating the HXL dataset (if omitted, use the default core schema).',
        metavar='schema',
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
    parser.add_argument(
        '-e',
        '--error-level',
        help='Minimum error level to show (defaults to "info") ',
        choices=['info', 'warning', 'error'],
        metavar='info|warning|error',
        default='info'
    )
    args = parser.parse_args(args)

    with hxl.make_input(args.infile or stdin, True) as input, make_output(args, stdout) as output:

        class Counter:
            infos = 0
            warnings = 0
            errors = 0

        def callback(e):
            """Show a validation error message."""
            if e.rule.severity == 'info':
                if args.error_level != 'info':
                    return
                Counter.infos += 1
            elif e.rule.severity == 'warning':
                if args.error_level == 'error':
                    return
                Counter.warnings += 1
            else:
                Counter.errors += 1

            message = '[{}] '.format(e.rule.severity)
            if e.row:
                if e.rule:
                    message += "{},{}: ".format(e.row.row_number + 1, e.rule.tag_pattern)
                else:
                    message += "{}: ".format(e.row.row_number + 1)
            elif e.rule:
                message += "<dataset>,{}: ".format(e.rule.tag_pattern)
            else:
                message += "<dataset>: "
            if e.value:
                message += '"{}" '.format(e.value)
            if e.message:
                message += e.message
            message += "\n"
            output.write(message)

        output.write("Validating {} with schema {} ...\n".format(args.infile or "<standard input>", args.schema or "<default>"))
        source = hxl.data(input)
        if args.schema:
            with hxl.make_input(args.schema, True) as schema_input:
                schema = hxl.schema(schema_input, callback=callback)
        else:
            schema = hxl.schema(callback=callback)

        schema.validate(source)

        if args.error_level == 'info':
            output.write("{:,} error(s), {:,} warnings, {:,} suggestions\n".format(Counter.errors, Counter.warnings, Counter.infos))
        elif args.error_level == 'warning':
            output.write("{:,} error(s), {:,} warnings\n".format(Counter.errors, Counter.warnings))
        else:
            output.write("{:,} error(s)\n".format(Counter.errors))
            
        if Counter.errors > 0:
            output.write("Validation failed.\n")
            exit(2)
        else:
            output.write("Validation succeeded.\n")

#
# Utility functions
#

def run_script(func):
    """Try running a command-line script, with exception handling."""
    try:
        func(sys.argv[1:], STDIN, sys.stdout)
    # except hxl.HXLException as e:
    #     print >>sys.stderr, "Fatal error (" + e.__class__.__name__ + "): " + str(e.message)
    #     print >>sys.stderr, "Exiting ..."
    #     sys.exit(2)
    except KeyboardInterrupt:
        print >>sys.stderr, "Interrupted"
        sys.exit(2)

def make_args(description):
    """Set up parser with default arguments."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        'infile',
        help='HXL file to read (if omitted, use standard input).',
        nargs='?'
        )
    parser.add_argument(
        'outfile',
        help='HXL file to write (if omitted, use standard output).',
        nargs='?'
        )
    parser.add_argument(
        '--sheet',
        help='Select sheet from a workbook (1 is first sheet)',
        metavar='number',
        type=int,
        nargs='?'
        )
    parser.add_argument(
        '--strip-tags',
        help='Strip HXL tags from the CSV output',
        action='store_const',
        const=True,
        default=False
        )
    return parser

def make_source(args, stdin=STDIN):
    """Create a HXL input source."""
    sheet_index = args.sheet
    if sheet_index is not None:
        sheet_index -= 1
    input = hxl.make_input(args.infile or stdin, sheet_index=sheet_index, allow_local=True)
    return hxl.data(input)

def make_output(args, stdout=sys.stdout):
    """Create an output stream."""
    if args.outfile:
        return FileOutput(args.outfile)
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

    def write(self, s):
        self.output.write(s)
            
    
