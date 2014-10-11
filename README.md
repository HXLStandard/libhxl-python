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
