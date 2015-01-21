#!/usr/bin/python

from setuptools import setup

setup(name='libhxl',
      version='0.11',
      description='Python support for the Humanitarian Exchange Language (HXL).',
      author='David Megginson',
      url='http://hxlproject.org',
      install_requires=['shapely', 'python-dateutil'],
      packages=['hxl', 'hxl.filters'],
      package_data={'': ['*.csv']},
      include_package_data=True,
      scripts=[
        'scripts/hxl2geojson',
        'scripts/hxladd',
        'scripts/hxlbounds',
        'scripts/hxlcount',
        'scripts/hxlcut',
        'scripts/hxlmerge',
        'scripts/hxlnorm',
        'scripts/hxlrename',
        'scripts/hxlselect',
        'scripts/hxlsort',
        'scripts/hxlvalidate'
        ]
      )
