#!/usr/bin/python

from setuptools import setup

dependency_links=[
    'git+https://github.com/Toblerity/Shapely.git@maint#egg=Shapely',
]

setup(name='libhxl',
      version='1.12',
      description='Python support for the Humanitarian Exchange Language (HXL).',
      author='David Megginson',
      author_email='contact@megginson.com',
      url='http://hxlproject.org',
      install_requires=['shapely', 'python-dateutil', 'xlrd'],
      dependency_links=dependency_links,
      packages=['hxl', 'hxl.old_filters'],
      package_data={'': ['*.csv']},
      include_package_data=True,
      test_suite='tests',
      entry_points={
          'console_scripts': [
              'hxladd = hxl.scripts:hxladd',
              'hxlbounds = hxl.scripts:hxlbounds',
              'hxlclean = hxl.scripts:hxlclean',
              'hxlcount = hxl.scripts:hxlcount',
              'hxlcut = hxl.scripts:hxlcut',
              'hxlmerge = hxl.scripts:hxlmerge',
              'hxlrename = hxl.scripts:hxlrename',
              'hxlselect = hxl.scripts:hxlselect',
              'hxlsort = hxl.scripts:hxlsort',
              'hxltag = hxl.scripts:hxltag',
              'hxlvalidate = hxl.scripts:hxlvalidate'
          ]
      }

)
