"""
Script to perform bounds checking on a HXL dataset.
David Megginson
October 2014

Check that all #lat_deg and #lon_deg values are contained within the
#GeoJSON featureset provided.

Prerequisites:
- Python Shapely library must be installed: https://pypi.python.org/pypi/Shapely
- libgeos_c must be installed: 

In Ubuntu Linux, the following commands will do the trick:

  sudo apt-get install libgeos-c1
  sudo pip install Shapely

Command-line usage:

  python -m hxl.scripts.hxlbounds -b BOUNDS.json DATA.csv > report.txt

(Use -h option to get full usage.)

Program usage:

  import sys
  from hxl.scripts.hxlbonds import hxlbounds

  data = json.load(open('LBR.json', 'r'))
  bounds = []
  for d in data['features']:
      bounds.append(shape(d['geometry']))
  hxlbounds(sys.stdin, sys.stdout, bounds)

License: Public Domain
Documentation: http://hxlstandard.org
"""

import sys
import argparse
import json
from shapely.geometry import Point, shape
from hxl.parser import HXLReader
from . import parse_tags

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
        report = str(message) + ' (row ' + str(row.sourceRowNumber) + ') ' + str(context)
        print >>output, report

    reader = HXLReader(input)
    for row in reader:
        lat = row.get('#lat_deg')
        lon = row.get('#lon_deg')
        if lat and lon:
            try:
                seen_shape = False
                for s in shapes:
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
                    

# If run as script
if __name__ == '__main__':

    # Command-line arguments
    parser = argparse.ArgumentParser(description = 'Normalize a HXL file.')
    parser.add_argument('infile', help='HXL file to read (if omitted, use standard input).', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', help='HXL file to write (if omitted, use standard output).', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-b', '--bounds', help='Preserve text header row above HXL hashtags', required=True, type=argparse.FileType('r'))
    parser.add_argument('-c', '--tags', help='Comma-separated list of column tags to include in error reports', metavar='tag,tag...', type=parse_tags, default='loc,org,sector,adm1,adm2,adm3')
    args = parser.parse_args()

    data = json.load(args.bounds)
    shapes = []
    for d in data['features']:
        shapes.append(shape(d['geometry']))

    # Call the command function
    hxlbounds(args.infile, args.outfile, shapes, tags=args.tags)

# end
