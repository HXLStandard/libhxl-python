#!/usr/bin/python

from setuptools import setup
import sys

if sys.version_info < (3,):
    raise RuntimeError("libhxl requires Python 3 or higher")

setup(name='libhxl',
      version="4.15.1",
      description='Python support library for the Humanitarian Exchange Language (HXL). See http://hxlstandard.org and https://github.com/HXLStandard/libhxl-python',
      author='David Megginson',
      author_email='megginson@un.org',
      url='http://hxlproject.org',
      install_requires=['python-dateutil', 'xlrd', 'requests', 'unidecode', 'python-io-wrapper', 'jsonpath_rw', 'ply'],
      packages=['hxl', 'hxl.formulas'],
      package_data={'hxl': ['*.json']},
      include_package_data=True,
      test_suite='tests',
      tests_require = ['mock'],
      entry_points={
          'console_scripts': [
              'hxladd = hxl.scripts:hxladd',
              'hxlappend = hxl.scripts:hxlappend',
              'hxlclean = hxl.scripts:hxlclean',
              'hxlcount = hxl.scripts:hxlcount',
              'hxlcut = hxl.scripts:hxlcut',
              'hxldedup = hxl.scripts:hxldedup',
              'hxlfill = hxl.scripts:hxlfill',
              'hxlhash = hxl.scripts:hxlhash',
              'hxlmerge = hxl.scripts:hxlmerge',
              'hxlrename = hxl.scripts:hxlrename',
              'hxlreplace = hxl.scripts:hxlreplace',
              'hxlselect = hxl.scripts:hxlselect',
              'hxlsort = hxl.scripts:hxlsort',
              'hxltag = hxl.scripts:hxltag',
              'hxlvalidate = hxl.scripts:hxlvalidate'
          ]
      }
)
