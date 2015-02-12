#!/usr/bin/python

from setuptools import setup
dependency_links=[
    'git+https://github.com/Toblerity/Shapely.git@maint#egg=Shapely',
]

setup(name='libhxl',
      version='0.12',
      description='Python support for the Humanitarian Exchange Language (HXL).',
      author='David Megginson',
      author_email='contact@megginson.com',
      url='http://hxlproject.org',
      install_requires=['shapely', 'python-dateutil'],
      dependency_links=dependency_links,
      packages=['hxl', 'hxl.filters'],
      package_data={'': ['*.csv']},
      include_package_data=True,
      test_suite='tests',
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
