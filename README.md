libhxl-python
=============

Python support library for the Humanitarian Exchange Language (HXL)
data standard.  The library requires Python 3 (versions prior to 4.6
also supported Python 2.7).

**API docs:** https://hxlstandard.github.io/libhxl-python/ (and in the ``docs/`` folder)

**HXL standard:** http://hxlstandard.org

## Quick start

From the command line (or inside a Python3 virtual environment):

```
$ pip3 install libhxl
```

In your code:

```
import hxl

url = "https://github.com/HXLStandard/libhxl-python/blob/main/tests/files/test_io/input-valid.csv"

data = hxl.data(url).with_rows("#sector=WASH").sort("#country")

for line in data.gen_csv():
    print(line)
```

## Usage

### Reading from a data source

The _hxl.data()_ function reads HXL from a file object, filename, URL,
or list of arrays and makes it available for processing, much like
``$()`` in JQuery. The following will read HXLated data from standard input:

```
import sys
import hxl

dataset = hxl.data(sys.stdin)
```

Most commonly, you will open a dataset via a URL:

```
dataset = hxl.data("https://example.org/dataset.url"
```

To open a local file rather than a URL, use the _allow\_local_ property
of the
[InputOptions](https://hxlstandard.github.io/libhxl-python/input.html#hxl.input.InputOptions)
class:

```
dataset = hxl.data("dataset.xlsx", hxl.InputOptions(allow_local=True))
```

#### Input caching

libhxl uses the Python
[requests](http://docs.python-requests.org/en/master/) library for
opening URLs. If you want to enable caching (for example, to avoid
beating up on your source with repeated requests), your code can use
the [requests_cache](https://pypi.python.org/pypi/requests-cache)
plugin, like this:

    import requests_cache
    requests_cache.install_cache('demo_cache', expire_after=3600)

The default caching backend is a sqlite database at the location specied.


### Filter chains

You can filters to transform the output, and chain them as
needed. Transformation is lazy, and uses the minimum memory
possible. For example, this command selects only data rows where the
country is "Somalia", sorted by the organisation:

```
transformed = hxl.data(url).with_rows("#country=Somalia").sort("#org")
```

For more on filters see the API documentation for the
[hxl.model.Dataset](https://hxlstandard.github.io/libhxl-python/model.html#hxl.model.Dataset)
class and the
[hxl.filters](https://hxlstandard.github.io/libhxl-python/filters.html)
module.


### Generators

Generators allow the re-serialising of HXL data, returning something that works like an iterator.  Example:

```
for line in hxl.data(url).gen_csv():
    print(line)
```

The following generators are available (you can use the parameters to turn the text headers and HXL tags on or off):

Generator method | Description
-- | --
[gen_raw()](https://hxlstandard.github.io/libhxl-python/model.html#hxl.model.Dataset.gen_raw) | Generate arrays of strings, one row at a time.
[gen_csv()](https://hxlstandard.github.io/libhxl-python/model.html#hxl.model.Dataset.gen_csv) | Generate encoded CSV rows, one row at a time.
[gen_json()](https://hxlstandard.github.io/libhxl-python/model.html#hxl.model.Dataset.gen_json) | Generate JSON output, either as rows or as JSON objects with the HXL hashtags as property names.

### Validation

To validate a HXL dataset against a schema (also in HXL), use the [validate()](https://hxlstandard.github.io/libhxl-python/model.html#hxl.model.Dataset.validate) method at the end of the filter chain:

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
    ## error_info is a HXLValidationException
    sys.stderr.write(error_info)

is_valid = hxl.data(url).validate(schema='my-schema.csv', callback=my_callback)
```

For more information on validation, see the API documentation for the
[hxl.validation](https://hxlstandard.github.io/libhxl-python/validation.html)
module and the format documentation for [HXL
schemas](https://github.com/HXLStandard/hxl-proxy/wiki/HXL-schemas).


## Command-line scripts

The filters are also available as command-line scripts, installed with
the library. For example,

```
$ hxlcount -t country dataset.csv
```

Will perform the same action as

```
import hxl

hxl.data("dataset.csv", hxl.InputOptions(allow_local=True)).count("country").gen_csv()
```

See the API documentation for the
[hxl.scripts](https://hxlstandard.github.io/libhxl-python/scripts.html)
module for more information about the command-line scripts
available. All scripts have an ``-h`` option that gives usage
information.


## Installation

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


## Makefile

There is also a generic Makefile that automates many tasks, including
setting up a Python virtual environment for testing. The Python3 venv
module is required for most of the targets.


```
make build-venv
```

Set up a local Python virtual environment for testing, if it doesn't
already exist. Will recreate the virtual environment if setup.py has
changed.

```
make test
```

Set up a virtual environment (if missing) and run all the unit tests

```
make test-install
```

Test a clean installation to verify there are no missing dependencies,
etc.

## License

libhxl-python is released into the Public Domain, and comes with NO
WARRANTY. See [LICENSE.md](./LICENSE.md) for details.
