#!/usr/bin/python

from setuptools import setup, find_packages
setup(name='libhxl',
      version='0.6',
      description='Python support for the Humanitarian Exchange Language (HXL).',
      author='David Megginson',
      url='http://hxlproject.org',
      requires=['shapely'],
      packages=['hxl', 'hxl.commands'],
      package_data={
        '': ['*.csv']
        },
      include_package_data=True,
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
