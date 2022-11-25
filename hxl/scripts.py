"""Console scripts
David Megginson
April 2015

This module implements the command-line scripts for HXL
processing. Most of them produce HXL output that another command can
use as input, so you can chain them together into a pipeline, e.g.

``` shell
$ cat dataset.csv | hxlselect -q "#org=UNICEF" \\
  | hxlsort -t "#value+committed" > output.csv
```

The ``-h`` option will provide more information about each script.

### About this module

**Author:** David Megginson

**Organisation:** UN OCHA

**License:** Public Domain

**Started:** April 2015

"""

from __future__ import print_function

import argparse, json, logging, os, re, requests, sys

# Do not import hxl, to avoid circular imports
import hxl.converters, hxl.filters, hxl.input


logger = logging.getLogger(__name__)


# Export only the script entry points
# (add any new scripts here)
__all__ = (
    'hxladd',
    'hxlappend',
    'hxlclean',
    'hxlcount',
    'hxlcut',
    'hxldedup',
    'hxlexpand',
    'hxlexplode',
    'hxlfill',
    'hxlimplode',
    'hxlhash',
    'hxlmerge',
    'hxlrename',
    'hxlreplace',
    'hxlselect',
    'hxlsort',
    'hxlspec',
    'hxltag',
    'hxlvalidate',
)


STDIN = sys.stdin.buffer
""" Constant: standard input (Python3) """

# Posix exit codes

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_SYNTAX = 2


#
# Console script entry points
#


def hxladd():
    """ Entry point for hxladd console script
``` none
usage: hxladd [-h] [--encoding [string]] [--sheet [number]]
              [--selector [path]] [--http-header header]
              [--remove-headers] [--strip-tags] [--ignore-certs]
              [--expand-merged] [--scan-ckan-resources]
              [--log debug|info|warning|error|critical|none] -s
              header#<tag>=<value> [-b]
              [infile] [outfile]

Add new columns with constant or computed values to a HXL dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -s header#<tag>=<value>, --spec header#<tag>=<value>
                        Constant value to add to each row (may repeat
                        option)
  -b, --before          Add new columns before existing ones rather than
                        after them.
```

"""
    run_script(hxladd_main)


def hxlappend():
    """ Entry point for hxlappend console script
``` none
usage: hxlappend [-h] [--encoding [string]] [--sheet [number]]
                 [--selector [path]] [--http-header header]
                 [--remove-headers] [--strip-tags] [--ignore-certs]
                 [--expand-merged] [--scan-ckan-resources]
                 [--log debug|info|warning|error|critical|none]
                 [-a file_or_url] [-l LIST] [-x]
                 [-q <tagspec><op><value>]
                 [infile] [outfile]

Concatenate two HXL datasets

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -a file_or_url, --append file_or_url
                        HXL file to append (may repeat option).
  -l LIST, --list LIST  URL or filename of list of URLs (may repeat
                        option). Will appear after sources in -a options.
  -x, --exclude-extra-columns
                        Don not add extra columns not in the original
                        dataset.
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        From --append datasets, include only rows
                        matching at least one query.
```

    """
    run_script(hxlappend_main)


def hxlclean():
    """ Entry point for hxlclean console script
``` none
usage: hxlclean [-h] [--encoding [string]] [--sheet [number]]
                [--selector [path]] [--http-header header]
                [--remove-headers] [--strip-tags] [--ignore-certs]
                [--expand-merged] [--scan-ckan-resources]
                [--log debug|info|warning|error|critical|none]
                [-w tag,tag...] [-u tag,tag...] [-l tag,tag...]
                [-d tag,tag...] [--date-format format] [-n tag,tag...]
                [--number-format format] [--latlon tag,tag...] [-p]
                [-q <tagspec><op><value>]
                [infile] [outfile]

Clean data in a HXL file by standardising formats.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -w tag,tag..., --whitespace tag,tag...
                        Comma-separated list of tag patterns for
                        whitespace normalisation.
  -u tag,tag..., --upper tag,tag...
                        Comma-separated list of tag patterns for
                        uppercase conversion.
  -l tag,tag..., --lower tag,tag...
                        Comma-separated list of tag patterns for
                        lowercase conversion.
  -d tag,tag..., --date tag,tag...
                        Comma-separated list of tag patterns for date
                        normalisation.
  --date-format format  Date formatting string in strftime format
                        (defaults to %Y-%m-%d).
  -n tag,tag..., --number tag,tag...
                        Comma-separated list of tag patternss for number
                        normalisation.
  --number-format format
                        Number formatting string in printf format
                        (without leading %).
  --latlon tag,tag...   Comma-separated list of tag patterns for lat/lon
                        normalisation.
  -p, --purge           Purge unparseable dates, numbers, and lat/lon
                        during cleaning.
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        Clean only rows matching at least one query.
```

"""
    run_script(hxlclean_main)


def hxlcount():
    """ Entry point for hxlcount console script
``` none
usage: hxlcount [-h] [--encoding [string]] [--sheet [number]]
                [--selector [path]] [--http-header header]
                [--remove-headers] [--strip-tags] [--ignore-certs]
                [--expand-merged] [--scan-ckan-resources]
                [--log debug|info|warning|error|critical|none]
                [-t tag,tag...] [-a statement] [-q <tagspec><op><value>]
                [infile] [outfile]

Generate aggregate counts for a HXL dataset, similar to a spreadsheet
pivot table

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -t tag,tag..., --tags tag,tag...
                        Comma-separated list of column tags to count.
  -a statement, --aggregator statement
                        Aggregator statement
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        Count only rows that match at least one query.
```

"""
    run_script(hxlcount_main)


def hxlcut():
    """ Entry point for hxlcut console script
``` none
usage: hxlcut [-h] [--encoding [string]] [--sheet [number]]
              [--selector [path]] [--http-header header]
              [--remove-headers] [--strip-tags] [--ignore-certs]
              [--expand-merged] [--scan-ckan-resources]
              [--log debug|info|warning|error|critical|none]
              [-i tag,tag...] [-x tag,tag...] [-s]
              [infile] [outfile]

Remove columns from a HXL dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -i tag,tag..., --include tag,tag...
                        Comma-separated list of column tags to include
  -x tag,tag..., --exclude tag,tag...
                        Comma-separated list of column tags to exclude
  -s, --skip-untagged   Skip columns without HXL hashtags
```

"""
    run_script(hxlcut_main)


def hxldedup():
    """ Entry point for hxldedup console script
``` none
usage: hxldedup [-h] [--encoding [string]] [--sheet [number]]
                [--selector [path]] [--http-header header]
                [--remove-headers] [--strip-tags] [--ignore-certs]
                [--expand-merged] [--scan-ckan-resources]
                [--log debug|info|warning|error|critical|none]
                [-t tag,tag...] [-q <tagspec><op><value>]
                [infile] [outfile]

Remove duplicate rows from a HXL dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -t tag,tag..., --tags tag,tag...
                        Comma-separated list of column tags to use for
                        deduplication (by default, use all values).
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        Leave rows alone if they don't match at least one
                        query.
```

"""
    run_script(hxldedup_main)


def hxlhash():
    """ Entry point for hxlhash console script
``` none
usage: hxlhash [-h] [--encoding [string]] [--sheet [number]]
               [--selector [path]] [--http-header header]
               [--ignore-certs] [--expand-merged] [--scan-ckan-resources]
               [--log debug|info|warning|error|critical|none] [-H]
               [infile]

Generate an MD5 hash for a HXL dataset (or just its header rows).

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -H, --headers-only    Hash only the header and hashtag rows.
```

"""
    run_script(hxlhash_main)


def hxlmerge():
    """ Entry point for hxlmerge console script
``` none
usage: hxlmerge [-h] [--encoding [string]] [--sheet [number]]
                [--selector [path]] [--http-header header]
                [--remove-headers] [--strip-tags] [--ignore-certs]
                [--expand-merged] [--scan-ckan-resources]
                [--log debug|info|warning|error|critical|none] -m
                filename -k tag,tag... -t tag,tag... [-r] [-O]
                [-q <tagspec><op><value>]
                [infile] [outfile]

Merge columns from one HXL dataset into another (similar to SQL join).

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -m filename, --merge filename
                        HXL file or URL to merge
  -k tag,tag..., --keys tag,tag...
                        HXL tag(s) to use as a shared key.
  -t tag,tag..., --tags tag,tag...
                        Comma-separated list of column tags to include
                        from the merge dataset.
  -r, --replace         Replace empty values in existing columns (when
                        available) instead of adding new ones.
  -O, --overwrite       Used with --replace, overwrite existing values.
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        Merged data only from rows that match at least
                        one query.
```

"""
    run_script(hxlmerge_main)


def hxlrename():
    """ Entry point for hxlrename console script
``` none
usage: hxlrename [-h] [--encoding [string]] [--sheet [number]]
                 [--selector [path]] [--http-header header]
                 [--remove-headers] [--strip-tags] [--ignore-certs]
                 [--expand-merged] [--scan-ckan-resources]
                 [--log debug|info|warning|error|critical|none]
                 [-r #?<original_tag>:<Text header>?#?<new_tag>]
                 [infile] [outfile]

Rename and retag columns in a HXL dataset

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -r #?<original_tag>:<Text header>?#?<new_tag>, --rename #?<original_tag>:<Text header>?#?<new_tag>
                        Rename an old tag to a new one, with an optional
                        new text header (may repeat option).
```

"""
    run_script(hxlrename_main)


def hxlreplace():
    """ Entry point for hxlreplace console script
``` none
usage: hxlreplace [-h] [--encoding [string]] [--sheet [number]]
                  [--selector [path]] [--http-header header]
                  [--remove-headers] [--strip-tags] [--ignore-certs]
                  [--expand-merged] [--scan-ckan-resources]
                  [--log debug|info|warning|error|critical|none]
                  [-p [PATTERN]] [-s [SUBSTITUTION]] [-t tag,tag...] [-r]
                  [-m [PATH]] [-q <tagspec><op><value>]
                  [infile] [outfile]

Replace strings in a HXL dataset

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        Replace only in rows that match at least one
                        query.

Inline replacement:
  -p [PATTERN], --pattern [PATTERN]
                        String or regular expression to search for
  -s [SUBSTITUTION], --substitution [SUBSTITUTION]
                        Replacement string
  -t tag,tag..., --tags tag,tag...
                        Tag patterns to match
  -r, --regex           Use a regular expression instead of a string

External substitution map:
  -m [PATH], --map [PATH]
                        Filename or URL of a mapping table using the tags
                        #x_pattern (required), #x_substitution
                        (required), #x_tag (optional), and #x_regex
                        (optional), corresponding to the inline options
                        above, for multiple substitutions.
```

"""
    run_script(hxlreplace_main)

def hxlfill():
    """ Entry point for hxlfill console script
``` none
usage: hxlfill [-h] [--encoding [string]] [--sheet [number]]
               [--selector [path]] [--http-header header]
               [--remove-headers] [--strip-tags] [--ignore-certs]
               [--expand-merged] [--scan-ckan-resources]
               [--log debug|info|warning|error|critical|none]
               [-t tagpattern,...] [-q <tagspec><op><value>]
               [infile] [outfile]

Fill empty cells in a HXL dataset

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -t tagpattern,..., --tag tagpattern,...
                        Fill empty cells only in matching columns
                        (default: fill in all); not allowed with --use-
                        merged
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        Fill only in rows that match at least one query.
```

"""
    run_script(hxlfill_main)


def hxlexpand():
    """ Entry point for hxlexpand console script
``` none
usage: hxlexpand [-h] [--encoding [string]] [--sheet [number]]
                 [--selector [path]] [--http-header header]
                 [--remove-headers] [--strip-tags] [--ignore-certs]
                 [--expand-merged] [--scan-ckan-resources]
                 [--log debug|info|warning|error|critical|none]
                 [-t [tag,tag...]] [-s string] [-c]
                 [-q <tagspec><op><value>]
                 [infile] [outfile]

Expand lists in cells by repeating rows

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -t [tag,tag...], --tags [tag,tag...]
                        Comma-separated list of tag patterns for columns
                        with lists to expand
  -s string, --separator string
                        string separating list items (defaults to "|")
  -c, --correlate       correlate list values instead of producing a
                        cartesian product
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        Limit list expansion to rows matching at least
                        one query.
```

"""
    run_script(hxlexpand_main)


def hxlexplode():
    """ Entry point for hxlexplode console script
``` none
usage: hxlexplode [-h] [--encoding [string]] [--sheet [number]]
                  [--selector [path]] [--http-header header]
                  [--remove-headers] [--strip-tags] [--ignore-certs]
                  [--expand-merged] [--scan-ckan-resources]
                  [--log debug|info|warning|error|critical|none] [-H att]
                  [-V tagpattern]
                  [infile] [outfile]

Explode a wide dataset into a long dataset

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -H att, --header-att att
                        attribute to add to the label column (defaults to
                        "label")
  -V tagpattern, --value-att tagpattern
                        attribute to add to the value column (defaults to
                        "value")
```

"""
    run_script(hxlexplode_main)


def hxlimplode():
    """ Entry point for hxlimplode console script
``` none
usage: hxlimplode [-h] [--encoding [string]] [--sheet [number]]
                  [--selector [path]] [--http-header header]
                  [--remove-headers] [--strip-tags] [--ignore-certs]
                  [--expand-merged] [--scan-ckan-resources]
                  [--log debug|info|warning|error|critical|none] -L
                  tagpattern -V tagpattern
                  [infile] [outfile]

Implode a long dataset into a wide dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -L tagpattern, --label tagpattern
                        HXL tag pattern for the label column
  -V tagpattern, --value tagpattern
                        HXL tag pattern for the value column
```

"""
    run_script(hxlimplode_main)


def hxlselect():
    """ Entry point for hxlselect console script
``` none
usage: hxlselect [-h] [--encoding [string]] [--sheet [number]]
                 [--selector [path]] [--http-header header]
                 [--remove-headers] [--strip-tags] [--ignore-certs]
                 [--expand-merged] [--scan-ckan-resources]
                 [--log debug|info|warning|error|critical|none] -q
                 <tagspec><op><value> [-r]
                 [infile] [outfile]

Filter rows in a HXL dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -q <tagspec><op><value>, --query <tagspec><op><value>
                        Query expression for selecting rows (may repeat
                        option for logical OR). <op> may be =, !=, <, <=,
                        >, >=, ~, or !~
  -r, --reverse         Show only lines *not* matching criteria
```

"""
    run_script(hxlselect_main)


def hxlsort():
    """ Entry point for hxlsort console script
``` none
usage: hxlsort [-h] [--encoding [string]] [--sheet [number]]
               [--selector [path]] [--http-header header]
               [--remove-headers] [--strip-tags] [--ignore-certs]
               [--expand-merged] [--scan-ckan-resources]
               [--log debug|info|warning|error|critical|none]
               [-t tag,tag...] [-r]
               [infile] [outfile]

Sort a HXL dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -t tag,tag..., --tags tag,tag...
                        Comma-separated list of tags to for columns to
                        use as sort keys.
  -r, --reverse         Flag to reverse sort order.
```

"""
    run_script(hxlsort_main)


def hxlspec():
    """ Entry point for hxlspec console script
``` none
usage: hxlspec [-h] [--encoding [string]] [--sheet [number]]
               [--selector [path]] [--http-header header]
               [--remove-headers] [--strip-tags] [--ignore-certs]
               [--expand-merged] [--scan-ckan-resources]
               [--log debug|info|warning|error|critical|none]
               [infile] [outfile]

Process a HXL JSON spec

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
```

"""
    run_script(hxlspec_main)


def hxltag():
    """ Entry point for hxltag console script
``` none
usage: hxltag [-h] [--encoding [string]] [--sheet [number]]
              [--selector [path]] [--http-header header]
              [--remove-headers] [--strip-tags] [--ignore-certs]
              [--expand-merged] [--scan-ckan-resources]
              [--log debug|info|warning|error|critical|none] [-a] -m
              Header Text#tag [-d #tag]
              [infile] [outfile]

Add HXL tags to a raw CSV file.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -a, --match-all       Match the entire header text (not just a
                        substring)
  -m Header Text#tag, --map Header Text#tag
                        Mapping expression
  -d #tag, --default-tag #tag
                        Default tag for non-matching columns
```

"""
    run_script(hxltag_main)


def hxlvalidate():
    """ Entry point for hxlvalidate console script
``` none
usage: hxlvalidate [-h] [--encoding [string]] [--sheet [number]]
                   [--selector [path]] [--http-header header]
                   [--remove-headers] [--strip-tags] [--ignore-certs]
                   [--expand-merged] [--scan-ckan-resources]
                   [--log debug|info|warning|error|critical|none]
                   [-s schema] [-a] [-e info|warning|error]
                   [infile] [outfile]

Validate a HXL dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard
                        input).
  outfile               HXL file to write (if omitted, use standard
                        output).

options:
  -h, --help            show this help message and exit
  --encoding [string]   Specify the character encoding of the input
  --sheet [number]      Select sheet from a workbook (1 is first sheet)
  --selector [path]     JSONPath expression for starting point in JSON
                        input
  --http-header header  Custom HTTP header to send with request
  --remove-headers      Strip text headers from the CSV output
  --strip-tags          Strip HXL tags from the CSV output
  --ignore-certs        Don't verify SSL connections (useful for self-
                        signed)
  --expand-merged       Expand merged areas by repeating the value (Excel
                        only)
  --scan-ckan-resources
                        For a CKAN dataset URL, scan all CKAN resources
                        for one that's HXLated
  --log debug|info|warning|error|critical|none
                        Set minimum logging level
  -s schema, --schema schema
                        Schema file for validating the HXL dataset (if
                        omitted, use the default core schema).
  -a, --all             Include all rows in the output, including those
                        without errors
  -e info|warning|error, --error-level info|warning|error
                        Minimum error level to show (defaults to "info")
```

"""
    run_script(hxlvalidate_main)


#
# Main scripts for command-line tools.
#

def hxladd_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxladd with command-line arguments.

    Add new columns with a constant or computed value.

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Add new columns with constant or computed values to a HXL dataset.')
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

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.AddColumnsFilter(source, specs=args.spec, before=args.before)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlappend_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlappend with command-line arguments.

    Concatenate two or more HXL datasets.

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
        default=[]
        )
    parser.add_argument(
        '-l',
        '--list',
        help='URL or filename of list of URLs (may repeat option). Will appear after sources in -a options.',
        action='append',
        default=[]
        )
    parser.add_argument(
        '-x',
        '--exclude-extra-columns',
        help='Don not add extra columns not in the original dataset.',
        action='store_const',
        const=True,
        default=False
    )
    add_queries_arg(parser, 'From --append datasets, include only rows matching at least one query.')

    args = parser.parse_args(args)

    do_common_args(args)

    append_sources = []
    for append_source in args.append:
        append_sources.append(hxl.data(append_source, make_input_options(args)))
    for list_source in args.list:
        for append_source in hxl.filters.AppendFilter.parse_external_source_list(hxl.data(list_source, make_input_options(args))):
            append_sources.append(hxl.data(append_source, make_input_options(args)))

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.AppendFilter(
            source,
            append_sources=append_sources,
            add_columns=(not args.exclude_extra_columns),
            queries=args.query
        )
        hxl.input.write_hxl(output.output, filter, show_headers=not args.remove_headers, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlclean_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlclean with command-line arguments.

    Clean data by standardising formats.

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Clean data in a HXL file by standardising formats.')
    parser.add_argument(
        '-w',
        '--whitespace',
        help='Comma-separated list of tag patterns for whitespace normalisation.',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '-u',
        '--upper',
        help='Comma-separated list of tag patterns for uppercase conversion.',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '-l',
        '--lower',
        help='Comma-separated list of tag patterns for lowercase conversion.',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '-d',
        '--date',
        help='Comma-separated list of tag patterns for date normalisation.',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '--date-format',
        help='Date formatting string in strftime format (defaults to %%Y-%%m-%%d).',
        default=None,
        metavar='format',
        )
    parser.add_argument(
        '-n',
        '--number',
        help='Comma-separated list of tag patternss for number normalisation.',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '--number-format',
        help='Number formatting string in printf format (without leading %%).',
        default=None,
        metavar='format',
        )
    parser.add_argument(
        '--latlon',
        help='Comma-separated list of tag patterns for lat/lon normalisation.',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '-p',
        '--purge',
        help='Purge unparseable dates, numbers, and lat/lon during cleaning.',
        action='store_const',
        const=True,
        default=False
        )
    add_queries_arg(parser, 'Clean only rows matching at least one query.')

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:

        filter = hxl.filters.CleanDataFilter(
            source, whitespace=args.whitespace, upper=args.upper, lower=args.lower,
            date=args.date, date_format=args.date_format, number=args.number, number_format=args.number_format,
            latlon=args.latlon, purge=args.purge, queries=args.query
        )
        hxl.input.write_hxl(output.output, filter, show_headers=not args.remove_headers, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlcount_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcount with command-line arguments.

    Generate aggregate counts, similar to a spreadsheet pivot table.

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    # Command-line arguments
    parser = make_args('Generate aggregate counts for a HXL dataset, similar to a spreadsheet pivot table')
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to count.',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list,
        default=None,
        )
    parser.add_argument(
        '-a',
        '--aggregator',
        help='Aggregator statement. Aggregators are count(), sum(), average(), min(), max(), and concat() (e.g. "sum(#affected+f) as Total Girls In Need#affected+f+total")',
        metavar='statement',
        action='append',
        type=hxl.filters.Aggregator.parse,
        default=[]
        )
    add_queries_arg(parser, 'Count only rows that match at least one query.')

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.CountFilter(source, patterns=args.tags, aggregators=args.aggregator, queries=args.query)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK

def hxlcut_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """ Run hxlcut with command-line arguments.

    Remove columns.

    """
    parser = make_args('Remove columns from a HXL dataset.')
    parser.add_argument(
        '-i',
        '--include',
        help='Comma-separated list of column tags to include',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '-x',
        '--exclude',
        help='Comma-separated list of column tags to exclude',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '-s',
        '--skip-untagged',
        help="Skip columns without HXL hashtags",
        action='store_const',
        const=True,
        default=False
        )
    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.ColumnFilter(source, args.include, args.exclude, args.skip_untagged)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxldedup_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """ Run hxldedup with command-line arguments.

    Remove duplicate rows from a HXL dataset.

    """
    parser = make_args('Remove duplicate rows from a HXL dataset.')
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to use for deduplication (by default, use all values).',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list
        )
    add_queries_arg(parser, 'Leave rows alone if they don\'t match at least one query.')

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.DeduplicationFilter(source, args.tags, args.query)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlhash_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """ Run hxlhash with command-line arguments.

    Generate an MD5 hash for a whole dataset or just its header rows.

    Does _not_ produce HXL output.

    """
    parser = make_args(
        'Generate an MD5 hash for a HXL dataset (or just its header rows).',
        hxl_output=False
    )
    parser.add_argument(
        '-H',
        '--headers-only',
        help='Hash only the header and hashtag rows.',
        action='store_const',
        const=True,
        default=False
        )

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source:
        if args.headers_only:
            print(source.columns_hash)
        else:
            print(source.data_hash)

    return EXIT_OK


def hxlmerge_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlmerge with command-line arguments.

    Merge columns from one HXL dataset into another (similar to SQL join).

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Merge columns from one HXL dataset into another (similar to SQL join).')
    parser.add_argument(
        '-m',
        '--merge',
        help='HXL file or URL to merge',
        metavar='filename',
        required=True
        )
    parser.add_argument(
        '-k',
        '--keys',
        help='HXL tag(s) to use as a shared key.',
        metavar='tag,tag...',
        required=True,
        type=hxl.model.TagPattern.parse_list
        )
    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of column tags to include from the merge dataset.',
        metavar='tag,tag...',
        required=True,
        type=hxl.model.TagPattern.parse_list
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
    add_queries_arg(parser, 'Merged data only from rows that match at least one query.')

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output, hxl.input.data(args.merge, hxl.InputOptions(allow_local=True)) if args.merge else None as merge_source:
        filter = hxl.filters.MergeDataFilter(
            source, merge_source=merge_source,
            keys=args.keys, tags=args.tags, replace=args.replace, overwrite=args.overwrite,
            queries=args.query
        )
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlrename_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlrename with command-line arguments.

    Rename and retag columns.

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
        type=hxl.filters.RenameFilter.parse_rename
        )
    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.RenameFilter(source, args.rename)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlreplace_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlreplace with command-line arguments.

    Replace values in the data.

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
        type=hxl.model.TagPattern.parse_list
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

    add_queries_arg(parser, 'Replace only in rows that match at least one query.')

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        if args.map:
            replacements = hxl.filters.ReplaceDataFilter.Replacement.parse_map(hxl.input.data(args.map, make_input_options(args)))
        else:
            replacements = [
                hxl.filters.ReplaceDataFilter.Replacement(args.pattern, args.substitution, args.tags, args.regex)
            ]
        filter = hxl.filters.ReplaceDataFilter(source, replacements, queries=args.query)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlfill_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlfill with command-line arguments.

    Fill empty cells.

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Fill empty cells in a HXL dataset')

    group = parser.add_mutually_exclusive_group(required=False)

    group.add_argument(
        '-t',
        '--tag',
        help='Fill empty cells only in matching columns (default: fill in all); not allowed with --use-merged',
        metavar='tagpattern,...',
        type=hxl.model.TagPattern.parse_list,
        )
    add_queries_arg(parser, 'Fill only in rows that match at least one query.')

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.FillDataFilter(source, patterns=args.tag, queries=args.query)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlexpand_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlexpand with command-line arguments.

    Expand lists in cells by repeating rows.

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Expand lists in cells by repeating rows')

    parser.add_argument(
        '-t',
        '--tags',
        help='Comma-separated list of tag patterns for columns with lists to expand',
        metavar='tag,tag...',
        type=hxl.model.TagPattern.parse_list,
        nargs="?"
        )

    parser.add_argument(
        "-s",
        '--separator',
        help='string separating list items (defaults to "|")',
        metavar='string',
        default="|"
        )

    parser.add_argument(
        "-c",
        '--correlate',
        help='correlate list values instead of producing a cartesian product',
        action='store_const',
        const=True,
        default=False
        )

    add_queries_arg(parser, 'Limit list expansion to rows matching at least one query.')

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.ExpandListsFilter(source, patterns=args.tags, separator=args.separator, correlate=args.correlate, queries=args.query)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlexplode_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlexplode with command-line arguments.

    Convert wide data into long data.

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Explode a wide dataset into a long dataset')

    parser.add_argument(
        '-H',
        '--header-att',
        help='attribute to add to the label column (defaults to "label")',
        metavar='att',
        default="label"
        )

    parser.add_argument(
        '-V',
        '--value-att',
        help='attribute to add to the value column (defaults to "value")',
        metavar='tagpattern',
        default="value"
        )

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.ExplodeFilter(source, header_attribute=args.header_att, value_attribute=args.value_att)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlimplode_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlexplode with command-line arguments.

    Convert long data into wide data.

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Implode a long dataset into a wide dataset.')

    parser.add_argument(
        '-L',
        '--label',
        help='HXL tag pattern for the label column',
        metavar='tagpattern',
        required=True,
        type=hxl.model.TagPattern.parse,
        )

    parser.add_argument(
        '-V',
        '--value',
        help='HXL tag pattern for the value column',
        metavar='tagpattern',
        required=True,
        type=hxl.model.TagPattern.parse,
        )

    args = parser.parse_args(args)

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.ImplodeFilter(source, label_pattern=args.label, value_pattern=args.value)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlselect_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlselect with command-line arguments.

    Filter rows.

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
        metavar='<tagspec><op><value>',
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

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.RowFilter(source, queries=args.query, reverse=args.reverse)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlsort_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlcut with command-line arguments.

    Sort rows.

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
        type=hxl.model.TagPattern.parse_list
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

    do_common_args(args)

    with make_source(args, stdin) as source, make_output(args, stdout) as output:
        filter = hxl.filters.SortFilter(source, args.tags, args.reverse)
        hxl.input.write_hxl(output.output, filter, show_tags=not args.strip_tags)

    return EXIT_OK


def hxlspec_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """ Run hxlspec with command-line arguments.

    Process a HXL JSON spec.

    Args:
        args (list): a list of command-line arguments
        stdin (io.IOBase): alternative standard input (mainly for testing)
        stdout (io.IOBase): alternative standard output (mainly for testing)
        stderr (io.IOBase): alternative standard error (mainly for testing)

    """

    def get_json (url_or_filename):

        if not url_or_filename:
            return json.load(stdin)

        if re.match(r'^(?:https?|s?ftp)://', url_or_filename.lower()):
            headers = make_headers(args)
            response = requests.get(url_or_filename, verify=(not args.ignore_certs), headers=headers)
            response.raise_for_status()
            return response.json()
        else:
            with open(url_or_filename, "r") as input:
                return json.load(input)

    parser = make_args('Process a HXL JSON spec')
    args = parser.parse_args(args)

    do_common_args(args)

    spec = get_json(args.infile)
    source = hxl.input.from_spec(spec, allow_local_ok=True)

    with make_output(args, stdout) as output:
        hxl.input.write_hxl(output.output, source, show_tags=not args.strip_tags)


def hxltag_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxltag with command-line arguments.

    Add tags to a non-HXLated file (accepts non-HXL input).

    @param args A list of arguments, excluding the script name
    @param stdin Standard input for the script
    @param stdout Standard output for the script
    @param stderr Standard error for the script
    """

    parser = make_args('Add HXL tags to a raw CSV file.')
    parser.add_argument(
        '-a',
        '--match-all',
        help='Match the entire header text (not just a substring)',
        action='store_const',
        const=True,
        default=False
        )
    parser.add_argument(
        '-m',
        '--map',
        help='Mapping expression',
        required=True,
        action='append',
        metavar='Header Text#tag',
        type=hxl.converters.Tagger.parse_spec
        )
    parser.add_argument(
        '-d',
        '--default-tag',
        help='Default tag for non-matching columns',
        metavar='#tag',
        type=hxl.model.Column.parse
    )
    args = parser.parse_args(args)

    do_common_args(args)

    with make_input(args, stdin) as input, make_output(args, stdout) as output:
        tagger = hxl.converters.Tagger(input, args.map, default_tag=args.default_tag, match_all=args.match_all)
        hxl.input.write_hxl(output.output, hxl.input.data(tagger), show_tags=not args.strip_tags)

    return EXIT_OK


def hxlvalidate_main(args, stdin=STDIN, stdout=sys.stdout, stderr=sys.stderr):
    """
    Run hxlvalidate with command-line arguments.

    Validate a dataset against a schema (produces non-HXL output).

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

    do_common_args(args)

    with make_input(args, stdin) as input, make_output(args, stdout) as output:

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
        source = hxl.input.data(input)
        if args.schema:
            with make_input(args, None, args.schema) as schema_input:
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
            return EXIT_ERROR
        else:
            output.write("Validation succeeded.\n")
            return EXIT_OK

#
# Utility functions
#

def run_script(func):
    """Try running a command-line script, with exception handling."""
    try:
        sys.exit(func(sys.argv[1:], STDIN, sys.stdout))
    except KeyboardInterrupt:
        logger.error("Interrupted")
        sys.exit(EXIT_ERROR)

def make_args(description, hxl_output=True):
    """Set up parser with default arguments.
    @param description: usage description to show
    @param hxl_output: if True (default), include options for HXL output.
    @returns: an argument parser, partly set up.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        'infile',
        help='HXL file to read (if omitted, use standard input).',
        nargs='?'
        )
    if hxl_output:
        parser.add_argument(
            'outfile',
            help='HXL file to write (if omitted, use standard output).',
            nargs='?'
        )
    parser.add_argument(
        '--encoding',
        help='Specify the character encoding of the input',
        metavar='string',
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
        '--selector',
        help='JSONPath expression for starting point in JSON input',
        metavar='path',
        nargs='?'
        )
    parser.add_argument(
        '--http-header',
        help='Custom HTTP header to send with request',
        metavar='header',
        action='append'
    )
    if hxl_output:
        parser.add_argument(
            '--remove-headers',
            help='Strip text headers from the CSV output',
            action='store_const',
            const=True,
            default=False
        )
        parser.add_argument(
            '--strip-tags',
            help='Strip HXL tags from the CSV output',
            action='store_const',
            const=True,
            default=False
        )
    parser.add_argument(
        "--ignore-certs",
        help="Don't verify SSL connections (useful for self-signed)",
        action='store_const',
        const=True,
        default=False
    )
    parser.add_argument(
        "--expand-merged",
        help="Expand merged areas by repeating the value (Excel only)",
        action='store_const',
        const=True,
        default=False
    )
    parser.add_argument(
        "--scan-ckan-resources",
        help="For a CKAN dataset URL, scan all CKAN resources for one that's HXLated",
        action='store_const',
        const=True,
        default=False
    )
    parser.add_argument(
        '--log',
        help='Set minimum logging level',
        metavar='debug|info|warning|error|critical|none',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='error'
    )
    return parser

def add_queries_arg(parser, help='Apply only to rows matching at least one query.'):
    parser.add_argument(
        '-q',
        '--query',
        help=help,
        metavar='<tagspec><op><value>',
        action='append'
    )
    return parser


def do_common_args(args):
    """Process standard args"""
    logging.basicConfig(format='%(levelname)s (%(name)s): %(message)s', level=args.log.upper())


def make_source(args, stdin=STDIN):
    """Create a HXL input source."""

    infile = args.infile
    if infile is None:
        infile = stdin

    return hxl.input.data(infile, make_input_options(args))


def make_input(args, stdin=sys.stdin, url_or_filename=None):
    """Create an input object"""

    if url_or_filename is None:
        url_or_filename = args.infile

    # JSONPath selector
    selector = args.selector

    return hxl.input.make_input(
        url_or_filename or stdin,
        make_input_options(args),
    )

def make_input_options(args):

    sheet_index = args.sheet
    if sheet_index is not None:
        sheet_index -= 1

    http_headers = make_headers(args)

    return hxl.input.InputOptions(
        sheet_index=sheet_index,
        selector=args.selector,
        allow_local=True,
        http_headers=http_headers,
        verify_ssl=(not args.ignore_certs),
        encoding=args.encoding,
        expand_merged=args.expand_merged,
        scan_ckan_resources=args.scan_ckan_resources,
    )


def make_output(args, stdout=sys.stdout):
    """Create an output stream."""
    if args.outfile:
        return FileOutput(args.outfile)
    else:
        return StreamOutput(stdout)


def make_headers (args):
    # get custom headers
    header_strings = []
    header = os.environ.get("HXL_HTTP_HEADER")
    if header is not None:
        header_strings.append(header)
    if args.http_header is not None:
        header_strings += args.http_header
    http_headers = {}
    for header in header_strings:
        parts = header.partition(':')
        http_headers[parts[0].strip()] = parts[2].strip()
    return http_headers



class FileOutput(object):

    def __init__(self, filename):
        self.output = open(filename, 'w')

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        self.output.close()

class StreamOutput(object):

    def __init__(self, output):
        self.output = output

    def __enter__(self):
        return self

    def __exit__(self, value, type, traceback):
        pass

    def write(self, s):
        self.output.write(s)


