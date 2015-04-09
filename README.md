libhxl-python
=============

Python support library for the Humanitarian Exchange Language (HXL)
data standard.  It supports both Python 2.7+ and Python 3.

About HXL: http://hxlstandard.org


# Usage

The _hxl()_ function (in the package ``hxl``) reads HXL from a file
object, filename, URL, or list of arrays and makes it available for
processing, much like ``$()`` in JQuery:

```
import sys
from hxl import hxl

dataset = hxl(sys.stdin)
```

You can add additional methods to process the data.  This example
shows an identity transformation in a pipeline (See "Generators",
below):

```
for line in hxl(sys.stdin).gen_csv():
    print(line)
```

This is the Same transformation, but loading the entire dataset into
memory as an intermediate step (see "Filters", below):

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

The following filters are available:

* .cache() - cache an in-memory version of the dataset (for processing multiple times)
* .with_columns(patterns) - include only columns that match the tag pattern(s), e.g. "#org+impl"
* .without_columns(patterns) - include all columns _except_ those that match the tag patterns
* .with_rows(queries) - include only rows that match at least one of the queries, e.g. "#sector=WASH"
* .without_rows(queries) - exclude rows that match at least one of the queries, e.g. "#sector=WASH"
* .sort(patterns, reverse=False) - sort the rows, optionally using the pattern(s) provided as sort keys. Set _reverse_ to True for a descending sort
* .count(patterns, aggregate_pattern=None) - count the number of value combinations that appear for the pattern(s), e.g. ['#sector', '#org']


## Generators

Generators allow the re-serialising of HXL data, returning something that works like an iterator.  Example:

```
for line in hxl(url).gen_csv():
    print(line)
```

The following generators are available (you can use the parameters to turn the text headers and HXL tags on or off):

* .gen_raw(show_headers=True, show_tags=True) - generate arrays of strings, one row at a time
* .gen_csv(show_headers=True, show_tags=True) - generate encoded CSV rows, one row at a time
* .gen_json(show_headers=True, show_tags=True) - generate encoded JSON rows, one row at a time


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

