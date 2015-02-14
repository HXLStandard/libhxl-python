#!/bin/sh
########################################################################
# Automatically discover and run libhxl_python unit tests.
#
# Usage:
#   sh run-tests.sh
########################################################################

echo "**"
echo "** Tests for Python 2"
echo "**"
python -m unittest discover tests

echo
echo "**"
echo "** Tests for Python 3"
echo "**"
python3 -m unittest discover tests

# end
