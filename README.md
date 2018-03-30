libhxl-python
=============

Python support library for the Humanitarian Exchange Language (HXL)
data standard.  The library requires Python 3 (versions prior to 4.6
also supported Python 2.7).

About HXL: http://hxlstandard.org


# Usage

The _hxl.data()_ function reads HXL from a file object, filename, URL,
or list of arrays and makes it available for processing, much like
``$()`` in JQuery:

```
import sys
import hxl

dataset = hxl.data(sys.stdin)
```

You can add additional methods to process the data.  This example
shows an identity transformation in a pipeline (See "Generators",
below):

```
for line in hxl.data(sys.stdin).gen_csv():
    print(line)
```

This is the Same transformation, but loading the entire dataset into
memory as an intermediate step (see "Filters", below):

```
for line in hxl.data(sys.stdin).cache().gen_csv():
    print(line)
```


## Filters

There are a number of filters that you can apply in a stream after a
HXL dataset.  This example uses the _with_rows()_ filter to find every
row that has a #sector of "WASH" and print the organisation mentioned
in the row:

```
for row in hxl.data(sys.stdin).with_rows('#sector=WASH'):
    print('The organisation is {}'.format(row.get('#org')))
```

This example removes the WASH sector from the results, then counts the
number of times each organisation appears in the remaining rows:

```
url = 'http://example.org/data.csv'
result = hxl.data(url).with_rows('#sector!=WASH').count('#org')
```

The following filters are available:

<table>
  <thead>
    <th>Filter method</th>
    <th>Description</th>
  </thead>
  <tbody>
    <tr>
      <td><code>.append(append_sources, add_columns=True, queries=[])</code></td>
      <td>Append a second HXL dataset to the current one, lining up columns.</td>
    </tr>
    <tr>
      <td><code>.cache()</code></td>
      <td>Cache an in-memory version of the dataset (for processing multiple times).</td>
    </tr>
    <tr>
      <td><code>.dedup(patterns=[], queries=[])</code></td>
      <td>Deduplicate the rows in a dataset, optionally looking only at specific columns.</td>
    </tr>
    <tr>
      <td><code>.with_columns(whitelist)</code></td>
      <td>Include only columns that match the whitelisted tag pattern(s), e.g. "#org+impl".</td>
    </tr>
    <tr>
      <td><code>.without_columns(blacklist)</code></td>
      <td>Include all columns <em>except</em> those that match the blacklisted tag patterns.</td>
    </tr>
    <tr>
      <td><code>.with_rows(queries, mask=[])</code></td>
      <td>Include only rows that match at least one of the queries, e.g. "#sector=WASH". Optionally ignore rows that don't match a mask pattern.</td>
    </tr>
    <tr>
      <td><code>.without_rows(queries, mask=[])</code></td>
      <td>Exclude rows that match at least one of the queries, e.g. "#sector=WASH". Optionally ignore rows that don't match a mask pattern.</td>
    </tr>
    <tr>
      <td><code>.sort(keys=None, reverse=False)</code></td>
      <td>Sort the rows, optionally using the pattern(s) provided as sort keys. Set _reverse_ to True for a descending sort.</td>
    </tr>
    <tr>
      <td><code>.count(patterns=[], aggregators=None, queries=[])</code></td>
      <td>Count the number of value combinations that appear for the pattern(s), e.g. ['#sector', '#org']. Optionally perform other aggregations, such as sums or averages.</td>
    </tr>
    <tr>
      <td><code>.replace_data(original, replacement, pattern=None, use_regex=False, queries=[])</code></td>
      <td>Replace values in a HXL dataset.</td>
    </tr>
    <tr>
      <td><code>.replace_data_map(map_source, queries=[])</code></td>
      <td>Replace values in a HXL dataset using a replacement map in another HXL dataset.</td>
    </tr>
    <tr>
      <td><code>.add_columns(specs, before=False)</code></td>
      <td>Add columns with fixed values to the dataset, e.g. "Country#country=Kenya" to add a new column #country with the text header "Country" and the value "Kenya" in every row.</td>
    </tr>
    <tr>
      <td><code>.rename_columns(specs)</code></td>
      <td>Change the header text and HXL hashtags for one or more columns.</td>
    </tr>
    <tr>
      <td><code>.clean_data(whitespace=[], upper=[], lower=[], date=[], number=[], queries=[])</code></td>
      <td>Clean whitespace, normalise dates and numbers, etc., optionally limited to specific columns.</td>
    </tr>
    <tr>
      <td><code>.merge_data(merge_source, keys, tags, replace=False, overwrite=False, queries=[])</code></td>
      <td>Merge values horizontally from a second dataset, based on shared keys (like a SQL join).</td>
    </tr>
    <tr>
      <td><code>.explode(header_attribute='header', value_attribute='value')</code></td>
      <td>Explode a "wide" dataset into a "long" dataset, using the HXL +label attribute.</td>
    </tr>
  </tbody>
</table>

## Sinks

Sinks take a HXL stream and convert it into something that's not HXL.

### Validation

To validate a HXL dataset against a schema (also in HXL), use the ``validate`` sink:

```
is_valid = hxl.data(url).validate('my-schema.csv')
```

If you don't specify a schema, the library will use a simple, built-in schema:

```
is_valid = hxl.data(url).validate()
```

If you include a callback, you can collect details about the errors and warnings:

```
def my_callback(error_info):
    # error_info is a HXLValidationException
    sys.stderr.write(error_info)

is_valid = hxl.data(url).validate(schema='my-schema.csv', callback=my_callback)
```

### Generators

Generators allow the re-serialising of HXL data, returning something that works like an iterator.  Example:

```
for line in hxl.data(url).gen_csv():
    print(line)
```

The following generators are available (you can use the parameters to turn the text headers and HXL tags on or off):

<table>
  <thead>
    <th>Generator method</th>
    <th>Description</th>
  </thead>
  <tbody>
    <tr>
      <td><code>Dataset.gen_raw(show_headers=True, show_tags=True)</code></td>
      <td>Generate arrays of strings, one row at a time.</td>
    </tr>
    <tr>
      <td><code>Dataset.gen_csv(show_headers=True, show_tags=True)</code></td>
      <td>Generate encoded CSV rows, one row at a time.</td>
    </tr>
    <tr>
      <td><code>Dataset.gen_json(show_headers=True, show_tags=True)</code></td>
      <td>Generate encoded JSON rows, one row at a time.</td>
    </tr>
  </tbody>
</table>


## Caching

libhxl uses the Python
[requests](http://docs.python-requests.org/en/master/) library for
opening URLs. If you want to enable caching (for example, to avoid
beating up on your source with repeated requests), your code can use
the [requests_cache](https://pypi.python.org/pypi/requests-cache)
plugin, like this:

    import requests_cache
    requests_cache.install_cache('demo_cache', expire_after=3600)

The default caching backend is a sqlite database at the location specied.


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

