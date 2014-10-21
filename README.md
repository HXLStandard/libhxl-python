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

## _Normalize_ script

Create a normalised version of a HXL dataset, removing columns with no
tags, stripping leading and trailing whitespace from values, and
expanding compact-disaggregated notation.

```
python -m hxl.scripts.normalize < DATASET_IN.csv > DATASET_OUT.csv
```

## _Counter_ script

Count unique combinations of values for one or more HXL tags (you may
omit the leading '#' from hashtags to avoid having to quote them on
the command line):

```
python -m hxl.scripts.counter org sector < DATASET_IN.csv > DATASET_out.csv
```

Sample output:

```
#org,#sector,#x_total_num
ACNUR,WASH,2
OMS,Salud,2
OMS,WASH,2
UNICEF,Educación,2
```
