"""Support library for the Humanitarian Exchange Language (HXL), version 1.1.

This library provides support for parsing, validating, cleaning, and
transforming humanitarian datasets that follow the [HXL
standard](https://hxlstandard.org). Its use will be familiar to
developers who have worked with libraries like
[JQuery](https://jquery.com).

### Example

```
import hxl
data = hxl.data('data.xlsx', True).with_rows('org=UNICEF').without_columns('contact').count('country')
```

This two-line script performs the following actions:

  1. Load and parse the spreadsheet ``data.xlsx`` (the library can
     also load from any URL, and understands how to read Google
     spreadsheets or [CKAN](http://ckan.org) resources).

  2. Filter out all rows where the value "UNICEF" doesn't appear under
     the ``#org`` (organisation) hashtag.

  3. Strip out personally-identifiable information by removing all
     columns with the ``#contact`` hashtag (e.g. ``#contact+name`` or
     ``#contact+phone`` or ``#contact+email``).

  4. Produce a report showing the number of times each unique
     ``#country`` appears in the resulting sheet (e.g. to count the
     number of activities being conducted by UNICEF in each country).

### Command-line scripts

The various filters are also available
as command-line scripts, so you could perform the same actions as
above in a shell script like this:

```
$ cat data.xlsx | hxlselect -q 'org=UNICEF' | hxlcut -x contact | hxlcount -t country
```

For more information about scripts, see the documentation for
`hxl.scripts`, or invoke any script with the ``-h`` option.

### Imports

Several identifiers are imported into this top-level package for
typing convenience, including `hxl.model.TagPattern`,
`hxl.model.Dataset`, `hxl.model.Column`, `hxl.model.Row`,
`hxl.model.RowQuery`, `hxl.input.data`, `hxl.input.tagger`,
`hxl.input.HXLParseException`, `hxl.input.write_hxl`,
`hxl.input.make_input`, `hxl.input.InputOptions`,
`hxl.input.from_spec`, `hxl.validation.schema`,
`hxl.validation.validate`, and
`hxl.validation.HXLValidationException`.

### Next steps

To get started, read the documentation for the `hxl.input.data` function and
the `hxl.model.Dataset` class. 

### About this module

**Author:** David Megginson

**Organisation:** UN OCHA

**License:** Public Domain

**Started:** Started August 2014

**GitHub:** https://github.com/HXLStandard/libhxl-python

**PyPi:** https://pypi.org/project/libhxl/

"""

import sys

if sys.version_info < (3,):
    raise RuntimeError("libhxl requires Python 3 or higher")

__version__="4.27.3"
"""Module version number
see https://www.python.org/dev/peps/pep-0396/
"""

# Flatten out common items for easier access

class HXLException(Exception):
    """Base class for all HXL-related exceptions."""

    def __init__(self, message, data={}):
        """Create a new HXL exception.

        Args:
            message (str): error message for the exception
            data (dict): properties associated with the exception (default {})
        """
        super(Exception, self).__init__(message)

        self.message = message
        """The human-readable error message."""

        self.data = data
        """Additional properties related to the error."""

    def __str__(self):
        return "<{}: {}>".format(type(self).__name__, str(self.message))

import hxl.geo
import hxl.datatypes
from hxl.model import TagPattern, Dataset, Column, Row, RowQuery
from hxl.input import data, tagger, HXLParseException, write_hxl, make_input, InputOptions, from_spec
from hxl.validation import schema, validate, HXLValidationException

# end


        
