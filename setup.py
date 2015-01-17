#!/usr/bin/python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='libhxl',
      version='0.9',
      description='Python support for the Humanitarian Exchange Language (HXL).',
      author='David Megginson',
      url='http://hxlproject.org',
      install_requires=['shapely', 'python-dateutil'],
      packages=['hxl', 'hxl.filters'],
      scripts=[
        'scripts/hxl2geojson',
        'scripts/hxlbounds',
        'scripts/hxlcount',
        'scripts/hxlcut',
        'scripts/hxlfilter',
        'scripts/hxlmerge',
        'scripts/hxlnorm',
        'scripts/hxlrename',
        'scripts/hxlsort',
        'scripts/hxlvalidate'
        ]
      )
