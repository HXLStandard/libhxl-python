#!/usr/bin/python

from setuptools import setup

dependency_links=[
    'git+https://github.com/Toblerity/Shapely.git@maint#egg=Shapely',
]

setup(name='libhxl',
      version='1.11',
      description='Python support for the Humanitarian Exchange Language (HXL).',
      author='David Megginson',
      author_email='contact@megginson.com',
      url='http://hxlproject.org',
      install_requires=['shapely', 'python-dateutil', 'xlrd'],
      dependency_links=dependency_links,
      packages=['hxl', 'hxl.filters'],
      package_data={'': ['*.csv']},
      include_package_data=True,
      test_suite='tests',
      entry_points={
          'console_scripts': [
              'hxlcut = hxl.scripts:hxlcut'
          ]
      },
      scripts=[
        'scripts/hxl2geojson',
        'scripts/hxladd',
        'scripts/hxlbounds',
        'scripts/hxlclean',
        'scripts/hxlcount',
        'scripts/hxlmerge',
        'scripts/hxlrename',
        'scripts/hxlselect',
        'scripts/hxlsort',
        'scripts/hxltag',
        'scripts/hxlvalidate'
        ]
      )
