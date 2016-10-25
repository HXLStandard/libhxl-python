"""Support library for the Humanitarian Exchange Language (HXL), version 1.0.

This library provides support for parsing, validating, cleaning, and
transforming humanitarian datasets that follow the HXL standard. Its
use will be familiar to developers who have worked with libraries like
U{JQuery<https://jquery.com>}.  Here's an example::

  import hxl
  data = hxl.data('data.xlsx', True).with_rows('org=UNICEF').without_columns('contact').count('country')

This two-line script performs the following actions:

  1. Load and parse the spreadsheet C{data.xlsx} (the library can also
     load from any URL, and understands how to read Google
     spreadsheets or U{CKAN<http://ckan.org>} resources).

  2. Filter out all rows where the value "UNICEF" doesn't appear under
     the C{#org} (organisation) hashtag.

  3. Strip out personally-identifiable information by removing all
     columns with the C{#contact} hashtag (e.g. C{#contact+name},
     C{#contact+phone}, C{#contact+email}).

  4. Produce a report showing the number of times each unique
     C{#country} appears in the resulting sheet (e.g. to count the number
     of activities being conducted by UNICEF in each country).

To get started, read the documentation for the L{hxl.data} function
and the L{hxl.model.Dataset} class. The various filters are also
available as command-line scripts, so you could perform the same
actions as above in a shell script like this::

  $ cat data.xlsx | hxlselect -q 'org=UNICEF' | hxlcut -x contact | hxlcount -t country

@author: David Megginson
@organization: UNOCHA
@license: Public Domain
@date: Started August 2014
@see: U{hxlstandard.org} for the HXL data standard
@see: U{proxy.hxlstandard.org} for web-based deployment of this library

"""

# Flatten out common items for easier access

from hxl.common import HXLException
from hxl.model import TagPattern, Dataset, Column, Row, RowQuery
from hxl.io import data, tagger, HXLParseException, write_hxl, make_input, from_spec
from hxl.validation import schema, HXLValidationException

# end


        
