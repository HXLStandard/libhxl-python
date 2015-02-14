import re
from hxl import HXLException

class Tagger(object):
    """Add HXL hashtags to a CSV-like input stream.

    The input spec is a list of tuples, where the first item is a
    substring to match (case-/space-/punctuation-insensitive), and the
    second item is the HXL tag spec to use. Example:

    [('Cluster', '#sector'), ('Organi', '#org'), ('province', '#adm1/es')]

    The tag specs are not parsed for correctness.
    """

    def __init__(self, specs, rawData):
        self.specs = []
        for spec in specs:
            self.specs.append([_norm(spec[0]), spec[1]])
        self.rawData = iter(rawData)
        self._cache = []
        self._found_tags = False

    def __next__(self):
        """Get the next row, if we can tag the raw data."""
        if not self._found_tags:
            # Search the first 25 rows for a match.
            if self.addTags():
                self._found_tags = True
            else:
                # if no match, through an exception
                raise HXLException("Tagging failed")
        if len(self._cache) > 0:
            # read from the cache, first
            return self._cache.pop(0)
        else:
            return next(self.rawData)

    next = __next__

    def addTags(self):
        """Look for headers in the first 25 rows."""
        for n in range(0, 25):
            rawRow = next(self.rawData)
            if not rawRow:
                break
            self._cache.append(rawRow)
            tagRow = self.tryTagRow(rawRow)
            if tagRow:
                self._cache.append(tagRow)
                return True
        return False

    def tryTagRow(self, rawRow):
        """See if we can match a header row."""
        tags = []
        tagCount = 0
        for index, value in enumerate(rawRow):
            value = _norm(value)
            for spec in self.specs:
                if spec[0] in value:
                    tags.append(spec[1])
                    tagCount += 1
                    break
            else:
                # run only if nothing found
                tags.append(None)
        if tagCount > 0 and tagCount/float(len(self.specs)) >= 0.5:
            return tags
        else:
            return None

    def __iter__(self):
        """Make iterable."""
        return self

                
def _norm(s):
    """Normalise a string to lower case, alphanum only, single spaces."""
    s = s.strip()
    s = re.sub(r'\W+', ' ', s)
    return s.lower()

