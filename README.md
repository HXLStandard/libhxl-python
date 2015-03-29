libhxl-python
=============

Python support library for the Humanitarian Exchange Language (HXL) data standard.

http://hxlstandard.org

# Usage

Streaming identity transformation in a pipeline (read from standard input, write to standard output):

```
import sys
from hxl.io import StreamInput, HXLReader, write_hxl

source = HXLReader(StreamInput(sys.stdin))
write_hxl(sys.stdout, source)
```

Same transformation, but loading the entire dataset into memory:

```
import sys
from hxl.io import read_hxl, write_hxl

dataset = read_hxl(StreamInput(sys.stdin))
write_hxl(sys.stdout, dataset)
```

Finding the #org for every row that has a #sector of "WASH":

```
import sys
from hxl.io import read_hxl

source = HXLReader(StreamInput(sys.stdin))
for row in source:
    if row.get('#sector') == 'WASH':
       print row.get('#org')
```

# Installation

This repository includes a standard Python `setup.py` script for
installing the library and scripts (applications) on your system. In a
Unix-like operating system, you can install using the following
command:

```
python setup.py install
```

If you don't need to install from source, try simply

```
pip install libhxl
```

Once you've installed, you will be able to include the HXL libraries
from any Python application, and will be able to call scripts like
_hxlvalidate_ from the command line.

# Scripts

There are several scripts that you can call from the command line. For
details, see https://github.com/HXLStandard/libhxl-python/wiki
