libhxl-python
=============

Python support library for the Humanitarian Exchange Language (HXL)
data standard.  It supports both Python 2.7+ and Python 3.

About HXL: http://hxlstandard.org

# Usage

Streaming identity transformation in a pipeline (read from standard
input, write to standard output):

```
for line in hxl(sys.stdin).gen_csv():
    print(line)
```

Same transformation, but loading the entire dataset into memory as an
intermediate step:

```
for line in hxl(sys.stdin).cache().gen_csv():
    print(line)
```

## Filters

There are a number of filters that you can apply in a stream after a
HXL dataset.  This example uses the _with_rows()_ filter to find every
row that has a #sector of "WASH" and print the organisation mentioned
in the row:

```
for row in hxl(sys.stdin).with_rows('#sector=WASH'):
    print('The organisation is {}'.format(row.get('#org')))
```

This example removes the WASH sector from the results, then counts the
number of times each organisation appears in the remaining rows:

```
url = 'http://example.org/data.csv'
result = hxl(url).with_rows('#sector!=WASH').count('#org')
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
