libhxl-python
=============

Python support library for the Humanitarian Exchange Language (HXL) data standard.

# Usage

Read a HXL file from standard input, row by row:

```
from hxl.parser import HXLReader
import sys

reader = HXLReader(sys.stdin)

for row in reader:
    print "Row " + str(row.rowNumber)
    for value in row:
        print '  ' + str(value.column.hxlTag) + '=' + str(value.content)
```

Identity transformation in a pipeline (read from standard input, write to standard output):

```
import sys
from hxl.parser import HXLReader
from hxl.writer import HXLWriter

parser = HXLReader(sys.stdin)
writer = HXLWriter(sys.stdout)

is_first = True
for row in parser:
    if is_first:
        writer.writeHeaders(row)
        writer.writeTags(row)
        is_first = False
    writer.writeData(row)
```

# Scripts

There are several scripts that you can call from the command line:

## _Normalize_ script

Create a normalised version of a HXL dataset, removing columns with no
tags, stripping leading and trailing whitespace from values, and
expanding compact-disaggregated notation.

``
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
