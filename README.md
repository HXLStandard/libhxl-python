libhxl-python
=============

Python support library for the Humanitarian Exchange Language (HXL) data standard.

http://hxlstandard.org

# Usage

Streaming identity transformation in a pipeline (read from standard input, write to standard output):

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

Same transformation, but loading the entire dataset into memory:

```
import sys
from hxl.parser import readHXL, writeHXL

dataset = readHXL(sys.stdin)
writeHXL(sys.stdout, dataset)
```

# Installation

This repository includes a standard Python `setup.py` script for
installing the library and scripts (applications) on your system. In a
Unix-like operating system, you can install using the following
command:

```
python setup.py install
```

Once you've installed, you will be able to include the HXL libraries
from any Python application, and will be able to call scripts like
_hxlvalidate_ from the command line.

# Scripts

There are several scripts that you can call from the command line. For
details, see https://github.com/HXLStandard/libhxl-python/wiki