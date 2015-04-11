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
              'hxladd = hxl.scripts:hxladd',
              'hxlclean = hxl.scripts:hxlclean',
              'hxlcount = hxl.scripts:hxlcount',
              'hxlcut = hxl.scripts:hxlcut',
              'hxlmerge = hxl.scripts:hxlmerge',
              'hxlrename = hxl.scripts:hxlrename',
              'hxlselect = hxl.scripts:hxlselect'
          ]
      },
      scripts=[
        'scripts/hxl2geojson',
        'scripts/hxlbounds',
        'scripts/hxlsort',
        'scripts/hxltag',
        'scripts/hxlvalidate'
        ]
      )
