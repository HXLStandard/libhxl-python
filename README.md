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