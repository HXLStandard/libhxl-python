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
    for value in row:
        print(value)
```
