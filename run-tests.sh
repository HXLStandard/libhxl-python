#!/bin/sh
########################################################################
# Automatically discover and run libhxl_python unit tests.
#
# Usage:
#   sh run-tests.sh
########################################################################

python -m unittest discover tests

# end
