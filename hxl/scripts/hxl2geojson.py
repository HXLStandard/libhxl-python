"""
Script to produce GeoJSON from HXL
David Megginson
October 2014

Command-line usage:

  python -m hxl.scripts.hxl2geojson < DATA_IN.csv > DATA_OUT.json

Program usage:

  import sys
  from hxl.scripts.hxl2geojson import hxl2geojson

  hxl2geojson(sys.stdin, sys.stdout)

Creates point data only. The script can handle extremely long
datasets, because it does not try to hold the entire dataset in memory
at once.

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import re
import json
from hxl.parser import HXLReader

def hxl2geojson(input, output):
    """
    Convert HXL to GeoJSON
    """

    output.write('{"type": "FeatureCollection",\n')
    output.write(' "features": [\n')

    parser = HXLReader(input)

    pattern = re.compile('^', re.MULTILINE)
    isFirst = True
    for row in parser:
        # Create a feature only when lat/lon are present
        lat = row.get('#lat_deg')
        lon = row.get('#lon_deg')
        if lat and lon:

            # Construct the basic feature
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lat, lon]
                    },
                }

            # Add selected properties if present
            properties = {}
            for tag in ['#loctype', '#loc', '#country', '#adm1', '#adm2', '#adm3']:
                if row.get(tag):
                    properties[tag] = row.get(tag)
            feature['properties'] = properties

            # Render the feature
            if isFirst:
                isFirst = False
            else:
                print ','
            output.write(re.sub(pattern, '   ', json.dumps(feature, indent=1)))

    output.write('\n ]\n}\n')

# If run as script
if __name__ == '__main__':
    hxl2geojson(sys.stdin, sys.stdout)

# end
