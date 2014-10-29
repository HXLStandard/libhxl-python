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

## _hxlnorm_ script

```
usage: hxlnorm.py [-h] [-H] [infile] [outfile]

Normalize a HXL file.

positional arguments:
  infile         HXL file to read (if omitted, use standard input).
  outfile        HXL file to write (if omitted, use standard output).

optional arguments:
  -h, --help     show this help message and exit
  -H, --headers  Preserve text header row above HXL hashtags
```

Create a normalised version of a HXL dataset, removing columns with no
tags, stripping leading and trailing whitespace from values, and
expanding compact-disaggregated notation. Unless the user provides the
-H / --headers option, the script will also strip headers and preserve
only the HXL tags, so that the file is suitable for processing by
regular CSV tools like
[csvkit](http://csvkit.readthedocs.org/en/0.9.0/).

## _hxlcut_ script

```
usage: hxlcut.py [-h] [-c tag,tag...] [-C tag,tag...] [infile] [outfile]

Cut columns from a HXL dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard input).
  outfile               HXL file to write (if omitted, use standard output).

optional arguments:
  -h, --help            show this help message and exit
  -c tag,tag..., --include-tags tag,tag...
                        Comma-separated list of column tags to include
  -C tag,tag..., --exclude-tags tag,tag...
                        Comma-separated list of column tags to exclude
```

Selectively cuts columns from a HXL dataset, using a whitelist, a
blacklist, or both.  Example removing personal information from a HXL
dataset:

```
python -m hxl.scripts.hxlcut -C name,email,phone < DATASET_IN.csv > DATASET_OUT.csv
```

## _hxlfilter_ script

Filter lines in a HXL dataset, preserving or removing matches.

**Usage:**

```
usage: hxlfilter.py [-h] [-f tag=value] [-v] [infile] [outfile]

Cut columns from a HXL dataset.

positional arguments:
  infile                HXL file to read (if omitted, use standard input).
  outfile               HXL file to write (if omitted, use standard output).

optional arguments:
  -h, --help            show this help message and exit
  -f tag=value, --filter tag=value
                        hashtag=value pair for filtering
  -v, --invert          Show only lines *not* matching criteria
```

**Examples:**

Show only lines where the country is Colombia and the sector is WASH:

```
python -m hxl.scripts.hxlfilter -f country=Colombia -f sector=WASH < DATASET_IN.csv > DATASET_OUT.csv
```

Show only lines where the org is not UNICEF:

```
python -m hxl.scripts.hxlfilter -f org=UNICEF -v < DATASET_IN.csv > DATASET_OUT.csv
```

## _hxlcount_ script

Count unique combinations of values for one or more HXL tags (you may
omit the leading '#' from hashtags to avoid having to quote them on
the command line):

```
python -m hxl.scripts.hxlcount org sector < DATASET_IN.csv > DATASET_OUT.csv
```

Sample output:

```
#org,#sector,#x_total_num
ACNUR,WASH,2
OMS,Salud,2
OMS,WASH,2
UNICEF,Educación,2
```

## _hxlbounds_ script

Check whether all of the points in a HXL dataset are contained
somewhere within a GeoJSON feature set.

**Prerequisites:** the Python
[Shapely](https://pypi.python.org/pypi/Shapely) library and the C
[libgeos](http://trac.osgeo.org/geos/) library must be available on
your system. In Ubuntu Linux, the follow commands may be sufficient:

```
sudo apt-get install libgeos_c1
sudo pip install Shapely
```

**Usage:**

```
usage: hxlbounds.py [-h] -b BOUNDS [-c tag,tag...] [infile] [outfile]

Normalize a HXL file.

positional arguments:
  infile                HXL file to read (if omitted, use standard input).
  outfile               HXL file to write (if omitted, use standard output).

optional arguments:
  -h, --help            show this help message and exit
  -b BOUNDS, --bounds BOUNDS
                        Preserve text header row above HXL hashtags
  -c tag,tag..., --tags tag,tag...
                        Comma-separated list of column tags to include in
                        error reports
```

**Example:**

Test whether all lat/lon data falls within the bounds of the GeoJSON
feature collection Colombia.json, showing also the #activity and #org
values in any error reports:

```
python -m hxl.scripts.hxlbounds -c activity,org -b Colombia.json hxl-data.csv > hxl-data-errors.txt
```

## _hxl2geojson_ script

Generate a GeoJSON file from a HXL dataset. There will be one GeoJSON
"point" feature for each input row that contains values for the
'#lat_deg' and '#lon_deg' HXL hashtags:

```
python -m hxl.scripts.hxl2geojson < DATASET_IN.csv > DATASET_OUT.json
```

