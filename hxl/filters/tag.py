import sys
import re
import argparse
import csv
import hxl
from hxl.model import Column
from hxl.io import AbstractInput, HXLReader, write_hxl
from hxl.filters import make_input, make_output, HXLFilterException

class Tagger(AbstractInput):
    """Add HXL hashtags to a CSV-like input stream.

    The input spec is a list of tuples, where the first item is a
    substring to match (case-/space-/punctuation-insensitive), and the
    second item is the HXL tag spec to use. Example:

    [('Cluster', '#sector'), ('Organi', '#org'), ('province', '#adm1+es')]

    The tag specs are not parsed for correctness.
    """

    def __init__(self, input, specs=[]):
        self.specs = [(_norm(spec[0]), spec[1]) for spec in specs]
        self.input = iter(input)
        self._cache = []
        self._found_tags = False

    def __next__(self):
        """Get the next row, if we can tag the raw data."""
        if not self._found_tags:
            # Search the first 25 rows for a match.
            if self.add_tags():
                self._found_tags = True
            else:
                # if no match, through an exception
                raise hxl.HXLException("Tagging failed")
        if len(self._cache) > 0:
            # read from the cache, first
            return self._cache.pop(0)
        else:
            return next(self.input)

    next = __next__

    def add_tags(self):
        """Look for headers in the first 25 rows."""
        for n in range(0, 25):
            raw_row = next(self.input)
            if not raw_row:
                break
            self._cache.append(raw_row)
            tag_row = self.tryTagRow(raw_row)
            if tag_row:
                self._cache.append(tag_row)
                return True
        return False

    def tryTagRow(self, raw_row):
        """See if we can match a header row."""
        tags = []
        tag_count = 0
        for index, value in enumerate(raw_row):
            value = _norm(value)
            for spec in self.specs:
                if spec[0] in value:
                    tags.append(spec[1])
                    tag_count += 1
                    break
            else:
                # run only if nothing found
                tags.append('')
        if tag_count > 0 and tag_count/float(len(self.specs)) >= 0.5:
            return tags
        else:
            return None

    def __iter__(self):
        """Make iterable."""
        return self

def _norm(s):
    """Normalise a string to lower case, alphanum only, single spaces."""
    if not s:
        s = ''
    s = str(s).strip()
    s = re.sub(r'\W+', ' ', s)
    s = s.lower()
    return s

#
# Command-line support
#

SPEC_PATTERN = r'^(.+)(#{token}([+]{token})*)$'.format(token=hxl.TOKEN)

def parse_spec(s):
    result = re.match(SPEC_PATTERN, s)
    if result:
        return (result.group(1), Column.parse(result.group(2), use_exception=True).display_tag)
    else:
        raise HXLFilterException("Bad tagging spec: " + s)

def run(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
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
        type=parse_spec
        )
    args = parser.parse_args(args)

    with make_input(args.infile, stdin) as input, make_output(args.outfile, stdout) as output:
        tagger = Tagger(input, args.map)
        source = HXLReader(tagger)
        write_hxl(output.output, source)

# end
