libhxl-python
=============

Python support library for the Humanitarian Exchange Language (HXL) data standard.

http://hxlstandard.org

# Usage

Identity transformation in a pipeline (read from standard input, write to standard output):

```
import sys
import csv
from hxl.parser import HXLReader


parser = HXLReader(sys.stdin)
writer = csv.writer(sys.stdout)

writer.writerow(parser.headers)
writer.writerow(parser.tags)
for row in parser:
    writer.writerow(row.values)
```

# Scripts

There are several scripts that you can call from the command line:

## _normalize_ script

```
usage: normalize.py [-h] [-H] [-i tag,tag...] [-e tag,tag...]
                    [infile] [outfile]

Normalize a HXL file.

positional arguments:
  infile                HXL file to read (if omitted, use standard input).
  outfile               HXL file to write (if omitted, use standard output).

optional arguments:
  -h, --help            show this help message and exit
  -H, --headers         Preserve text header row above HXL hashtags
  -i tag,tag..., --include-tags tag,tag...
                        Comma-separated list of column tags to include
  -e tag,tag..., --exclude-tags tag,tag...
                        Comma-separated list of column tags to exclude
```

Create a normalised version of a HXL dataset, removing columns with no
tags, stripping leading and trailing whitespace from values, and
expanding compact-disaggregated notation. Unless the user provides the
-H / --headers option, the script will also strip headers and preserve
only the HXL tags, so that the file is suitable for processing by
regular CSV tools like
[csvkit](http://csvkit.readthedocs.org/en/0.9.0/).

```
python -m hxl.scripts.normalize < DATASET_IN.csv > DATASET_OUT.csv
```

You can include columns by tag using the -i option, or exclude them
using the -e option (it's OK to leave the leading '#' off tag
names). The following example strips any columns with the tags `#name`,
`#email`, or `#phone` from the input file:

```
python -m hxl.scripts.normalize -e name,email,phone < DATASET_IN.csv > DATASET_OUT.csv
```

## _counter_ script

Count unique combinations of values for one or more HXL tags (you may
omit the leading '#' from hashtags to avoid having to quote them on
the command line):

```
python -m hxl.scripts.counter org sector < DATASET_IN.csv > DATASET_OUT.csv
```

Sample output:

```
#org,#sector,#x_total_num
ACNUR,WASH,2
OMS,Salud,2
OMS,WASH,2
UNICEF,Educación,2
```

## _hxl2geojson_ script

Generate a GeoJSON file from a HXL dataset. There will be one GeoJSON
"point" feature for each input row that contains values for the
'#lat_deg' and '#lon_deg' HXL hashtags:

```
python -m hxl.scripts.hxl2geojson < DATASET_IN.csv > DATASET_OUT.json
```

