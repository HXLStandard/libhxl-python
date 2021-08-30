"""Data-conversion classes

This module holds classes for converting to HXL from other formats, or
from HXL to other formats. Current, the only class is ``Tagger`` (for
adding tags to non-HXL tabular data on the fly), but we may add more
converters, especially for formats like GeoJSON.

Author:
    David Megginson

License:
    Public Domain

"""

import hxl
import logging, re


logger = logging.getLogger(__name__)


class Tagger(hxl.input.AbstractInput):
    """Add HXL hashtags to a non-HXL datasource on the fly.

    Example:
    ```
    input = hxl.input.make_input(url_or_filename)
    specs = [('Cluster', '#sector'), ('Organi', '#org'), ('province', '#adm1+es')]
    dataset = hxl.converters.Tagger(input, specs)
    ```

    The more-common way to invoke the tagger is through the
    ``hxl.input.tagger()`` function:

    ```
    dataset = hxl.input.tagger(url_or_filename, specs)
    ```

    """

    def __init__(self, input, specs=[], default_tag=None, match_all=False):
        """Construct a new Tagger object.

        The input spec is a list of tuples, where the first item is a
        substring to match (case-/space-/punctuation-insensitive), and
        the second item is the HXL tag spec to use.

        Example:
        ```
        spec = [
            ['Cluster', '#sector'],
            ['Organisation', '#org'],
            ['Province', '#adm1+es']
        ]
        ```

        Args:
            input (hxl.input.AbstractInput): an input source that can yield rows of values (see ``hxl.input.make_input``).
            specs (dict): the input specs, as described above (default: [])
             match_all (bool): if True, require that the full header string match; otherwise, match substrings (default: False)
            default_tag (str): default tagspec to use for any column without a match
        """
        if isinstance(specs, dict):
            # convert to list of tuples if needed
            specs = [(key, specs[key]) for key in specs]
        self.specs = [(hxl.datatypes.normalise_string(spec[0]), spec[1]) for spec in specs]
        self.default_tag = default_tag
        self.match_all = match_all
        self.input = iter(input)
        self._cache = []
        self._found_tags = False

    def __next__(self):
        """Return the next line of input (including the new tags)."""
        if not self._found_tags:
            # Search the first 25 rows for a match.
            if self._add_tags():
                self._found_tags = True
            else:
                # if no match, through an exception
                raise hxl.HXLException("Tagging failed")
        if len(self._cache) > 0:
            # read from the cache, first
            return self._cache.pop(0)
        else:
            return next(self.input)

    def _add_tags(self):
        """Look for headers in the first 25 rows of data.
        @return: True if headers were found matching the tagging specs; False otherwise.
        """
        for n in range(0, 25):
            raw_row = next(self.input)
            if raw_row is None:
                break
            self._cache.append(raw_row)
            tag_row = self._try_tag_row(raw_row)
            if tag_row:
                self._cache.append(tag_row)
                return True
        return False

    def _try_tag_row(self, raw_row):
        """See if we can match a potential header row with the spec headers.
        @param raw_row: the row to check
        @return: the row of hashtag specs if successful, or None otherwise.
        """
        tags = []
        tag_count = 0
        for index, value in enumerate(raw_row):
            value = hxl.datatypes.normalise_string(value)
            for spec in self.specs:
                if self._check_header(spec[0], value):
                    tags.append(spec[1])
                    tag_count += 1
                    break
            else:
                # run only if nothing found
                tags.append('')
        if tag_count > 0 and tag_count/float(len(self.specs)) >= 0.5:
            if self.default_tag:
                tags = [tag or self.default_tag for tag in tags]
            return tags
        else:
            return None

    def _check_header(self, spec, header):
        """Check if an individual header matches a spec for tagging.
        Assumes that both the spec and the header have already been
        case- and whitespace-normalised. If self.match_all is True,
        then the spec must match the header completely; otherwise, it
        needs to match only a substring.
        @param spec: the spec to match
        @param header: the header to test
        @return True if there's a match; False otherwise
        """
        if self.match_all:
            return (spec == header)
        else:
            return (spec in header)

    # this class is its own iterator
    def __iter__(self):
        return self

    _SPEC_PATTERN = r'^(.+)(#{token}([+]{token})*)$'.format(token=hxl.datatypes.TOKEN_PATTERN)
    """Regular-expression pattern for matching a tagging specification as a string"""

    @staticmethod
    def parse_spec(s):
        """Parse a JSON-like tagger spec

        The string is in the format "HEADER TEXT#hashtag+attributes"

        Example:
        ```
        spec = hxl.converters.Tagger.parse_spec("Organisation name#org+name")
        ```

        Used only by the command-line tools.

        Args:
            s (str): the string representing a tagging specification

        Returns:
            hxl.model.Column: the parsed specification as a column object (header, hashtags, and attributes)

        Raises:
            hxl.filters.HXLFilterException: if there is an error parsing the spec

        """
        result = re.match(Tagger._SPEC_PATTERN, s)
        if result:
            return (result.group(1), hxl.model.Column.parse(result.group(2), use_exception=True).display_tag)
        else:
            raise HXLFilterException("Bad tagging spec: " + s)

    @staticmethod
    def _load(input, spec):
        """Create a tagger from a dict spec.

        Example:
        ```
        {
          "match_all": false,
          "default_tag": "#affected+label",
          "specs": [
            ["district", "#adm1+name"],
            ["p-code", "#adm1+code+v_pcode"],
            ["organi", "#org+name"]
          ]
        }
        ```

        """
        return Tagger(
            input=input,
            specs=spec.get('specs', []),
            default_tag=spec.get('default_tag', None),
            match_all=spec.get('match_all', False)
        )
