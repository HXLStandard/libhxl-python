"""
HXL converters (to/from other formats)
"""

import re
import hxl
from hxl.common import normalise_string
from hxl.model import Column


class Tagger(hxl.io.AbstractInput):
    """Add HXL hashtags to a CSV-like input stream.

    The input spec is a list of tuples, where the first item is a
    substring to match (case-/space-/punctuation-insensitive), and the
    second item is the HXL tag spec to use. Example:

    [('Cluster', '#sector'), ('Organi', '#org'), ('province', '#adm1+es')]

    The tag specs are not parsed for correctness.
    """

    def __init__(self, input, specs=[]):
        self.specs = [(normalise_string(spec[0]), spec[1]) for spec in specs]
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
            value = normalise_string(value)
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

    SPEC_PATTERN = r'^(.+)(#{token}([+]{token})*)$'.format(token=hxl.common.TOKEN)

    @staticmethod
    def parse_spec(s):
        result = re.match(Tagger.SPEC_PATTERN, s)
        if result:
            return (result.group(1), Column.parse(result.group(2), use_exception=True).display_tag)
        else:
            raise HXLFilterException("Bad tagging spec: " + s)


# FIXME untested and badly out of date
def hxlbounds(input, output, bounds, tags=[]):
    """
    Check that all points in a HXL dataset fall without a set of bounds.
    """

    def error(row, message):
        """Report a bounds error."""
        lat = row.get('#lat_deg')
        lon = row.get('#lon_deg')
        context = [
            '#lat_deg' + '=' + lat,
            '#lon_deg' + '=' + lon
            ]
        if (tags):
            for tag in tags:
                value = row.get(tag)
                if value:
                    context.append(tag + '=' + value)
        report = str(message) + ' (row ' + str(row.source_row_number) + ') ' + str(context)
        print >>output, report

    reader = HXLReader(input)
    for row in reader:
        lat = row.get('#lat_deg')
        lon = row.get('#lon_deg')
        if lat and lon:
            try:
                seen_shape = False
                for s in bounds:
                    if s.contains(Point(float(lon), float(lat))):
                        seen_shape = True
                        break;
                if not seen_shape:
                    error(row, 'out of bounds')
            except ValueError:
                error(row, 'malformed lat/lon')
        elif lat or lon:
            error(row, '#lat_deg or #lon_deg missing')
        # TODO user option to report no lat *and* no lon
                    
