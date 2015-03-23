#!/bin/sh
########################################################################
# Full installation and unit tests in clean virtual environments.
#
# This is a more-expensive test script to run, since it sets up virtual
# environments from scratch, but it can detect installation errors,
# or errors masked by stale installed files.
#
# The script will leave virtual environments installed in the following
# directories:
#
# testenv-python2/
# testenv-python3/
#
# Started by David Megginson, 2015-03-12
########################################################################

# Where do we live?
DIR=$(cd `dirname "$0"` && pwd)
cd $DIR

#
# Create a new virtualenv for Python2 and install libhxl
#
echo
echo "Setting up Python2 test environment ..."
echo
rm -rf testenv-python2
virtualenv testenv-python2
. testenv-python2/bin/activate
python setup.py install

#
# Create a new virtualenv for Python3 and install libhxl
#
echo
echo "Setting up Python3 test environment ..."
echo
rm -rf testenv-python3
virtualenv -p /usr/bin/python3 testenv-python3
. testenv-python3/bin/activate
python setup.py install

#
# Run tests in clean Python2 environment
#
. testenv-python2/bin/activate
echo
echo -n "Testing in "
python --version
python setup.py test

#
# Run tests in clean Python3 environment
#
. testenv-python3/bin/activate
echo
echo -n "Testing in "
python --version
python setup.py test

# rm -rf testenv-python2
# rm -rf testenv-python3

# end
