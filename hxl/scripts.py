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

from hxl import hxl, TagPattern, HXLException
from hxl.io import write_hxl, make_input

from hxl.filters import AddColumnsFilter, AppendFilter, CleanDataFilter, ColumnFilter, CountFilter, MergeDataFilter, RenameFilter, ReplaceDataFilter, RowFilter, SortFilter, ValidateFilter
from hxl.converters import Tagger


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

def hxladd_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxladd with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Add new columns with constant values to a HXL dataset.')
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
        '-s',
        '--spec',
        help='Constant value to add to each row',
        metavar='[[Text header]#]<tag>=<value>',
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

    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output:
        filter = AddColumnsFilter(source, specs=args.spec, before=args.before)
        write_hxl(output.output, filter)


def hxlappend_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlappend with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Concatenate two HXL datasets')
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
        '-a',
        '--append',
        help='HXL file to append.',
        metavar='file_or_url',
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

    with hxl(args.infile or stdin, True) as source, hxl(args.append, True) as append_source, make_output(args.outfile, stdout) as output:
        filter = AppendFilter(source, append_source, not args.exclude_extra_columns)
        write_hxl(output.output, filter)


def hxlbounds_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Perform bounds checking on a HXL dataset.')
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
        type=TagPattern.parse_list,
        default='loc,org,sector,adm1,adm2,adm3'
        )
    args = parser.parse_args(args)

    data = json.load(args.bounds)
    shapes = []
    for d in data['features']:
        shapes.append(shape(d['geometry']))

    # Call the command function
    hxl.converters.hxlbounds(args.infile, args.outfile, shapes, tags=args.tags)


def hxlclean_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlclean with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Clean data in a HXL file.')
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
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-u',
        '--upper',
        help='Comma-separated list of tags to convert to uppercase.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-l',
        '--lower',
        help='Comma-separated list of tags to convert to lowercase.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
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
        type=TagPattern.parse_list
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
        type=TagPattern.parse_list
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
    
    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output:

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
        write_hxl(output.output, filter, args.remove_headers)


def hxlcount_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcount with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Generate aggregate counts for a HXL dataset')
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
        '-t',
        '--tags',
        help='Comma-separated list of column tags to count.',
        metavar='tag,tag...',
        type=TagPattern.parse_list,
        default='loc,org,sector,adm1,adm2,adm3'
        )
    parser.add_argument(
        '-a',
        '--aggregate',
        help='Hashtag to aggregate.',
        metavar='tag',
        type=TagPattern.parse
        )

    args = parser.parse_args(args)
    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output:
        filter = CountFilter(source, args.tags, args.aggregate)
        write_hxl(output.output, filter)


def hxlcut_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    parser = argparse.ArgumentParser(description = 'Cut columns from a HXL dataset.')
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
        '-i',
        '--include',
        help='Comma-separated list of column tags to include',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-x',
        '--exclude',
        help='Comma-separated list of column tags to exclude',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    args = parser.parse_args(args)

    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output:
        filter = ColumnFilter(source, args.include, args.exclude)
        write_hxl(output.output, filter)


def hxlmerge_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlmerge with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Merge part of one HXL dataset into another.')
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
        type=TagPattern.parse_list
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to include from the merge dataset.',
        metavar='tag,tag...',
        required=True,
        type=TagPattern.parse_list
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

    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output, hxl(args.merge, True) if args.merge else None as merge_source:
        filter = MergeDataFilter(source, merge_source=merge_source,
                             keys=args.keys, tags=args.tags, replace=args.replace, overwrite=args.overwrite)
        write_hxl(output.output, filter)


def hxlrename_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlrename with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Rename and retag columns in a HXL dataset')
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
        '-r',
        '--rename',
        help='Rename an old tag to a new one (with an optional new text header).',
        action='append',
        metavar='#?<original_tag>:<Text header>?#?<new_tag>',
        default=[],
        type=RenameFilter.parse_rename
        )
    args = parser.parse_args(args)

    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output:
        filter = RenameFilter(source, args.rename)
        write_hxl(output.output, filter)


def hxlreplace_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlreplace with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Replace strings in a HXL dataset')
    parser.add_argument(
        'original',
        help='String or regular expression to search for'
        )
    parser.add_argument(
        'replacement',
        help='Replacement string'
        )
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
        '-i',
        '--input',
        help='String or regular expression to replace',
        metavar='TEXT'
        )
    parser.add_argument(
        '-r',
        '--regex',
        help='Use a regular expression instead of a string',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of tag patterns to include in error reports',
        metavar='tag,tag...',
        type=TagPattern.parse_list
        )
    args = parser.parse_args(args)

    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output:
        filter = ReplaceDataFilter(source, args.original, args.replacement, args.tags, args.regex)
        write_hxl(output.output, filter)


def hxlselect_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlselect with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Filter rows in a HXL dataset.')
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
        '-q',
        '--query',
        help='query expression for selecting rows (use multiple times for logical OR): <hashtag><op><value>, where <op> is =, !=, <, <=, >, >=, ~, or !~',
        action='append',
        metavar='tag=value, etc.',
        default=[]
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

    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output:
        filter = RowFilter(source, queries=args.query, reverse=args.reverse)
        write_hxl(output.output, filter)


def hxlsort_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcut with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Sort a HXL dataset.')
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
        '-t',
        '--tags',
        help='Comma-separated list of tags to for columns to use as sort keys.',
        metavar='tag,tag...',
        type=TagPattern.parse_list
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

    with hxl(args.infile or stdin, True) as source, make_output(args.outfile, stdout) as output:
        filter = SortFilter(source, args.tags, args.reverse)
        write_hxl(output.output, filter)


def hxltag_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxltag with command-line arguments.
    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = argparse.ArgumentParser(description = 'Add HXL tags to a raw CSV file.')
    parser.add_argument(
        'infile',
        help='CSV file to read (if omitted, use standard input).',
        nargs='?'
        )
    parser.add_argument(
        'outfile',
        help='CSV file to write (if omitted, use standard output).',
        nargs='?'
        )
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

    with make_input(args.infile or stdin) as input, make_output(args.outfile, stdout) as output:
        tagger = Tagger(input, args.map)
        write_hxl(output.output, hxl(tagger))


def hxlvalidate_main(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
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
                schema = hxl_schema(args.schema)
        else:
            schema = hxl_schema()
        filter = ValidateFilter(source, schema, args.all)
        write_hxl(args.outfile, filter)



#
# Utility functions
#

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
            
    
