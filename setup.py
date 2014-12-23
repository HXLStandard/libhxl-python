#!/usr/bin/python

from distutils.core import setup
setup(name='libhxl',
      version='0.6',
      description='Python support for the Humanitarian Exchange Language (HXL).',
      author='David Megginson',
      url='http://hxlproject.org',
      requires=['shapely'],
      packages=['hxl', 'hxl.filters'],
      scripts=[
        'scripts/hxl2geojson',
        'scripts/hxlbounds',
        'scripts/hxlcount',
        'scripts/hxlcut',
        'scripts/hxlfilter',
        'scripts/hxlmerge',
        'scripts/hxlnorm',
        'scripts/hxlvalidate'
        ]
      )
