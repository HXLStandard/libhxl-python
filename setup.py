#!/usr/bin/python

from setuptools import setup
import sys

if sys.version_info < (3,):
    raise RuntimeError("libhxl requires Python 3 or higher")

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='libhxl',
    version="5.2",
    description='Python support library for the Humanitarian Exchange Language (HXL). See http://hxlstandard.org and https://github.com/HXLStandard/libhxl-python',
    long_description=long_description,
    long_description_content_type="text/markdown",
    project_urls={
        'Documentation': 'https://hxlstandard.github.io/libhxl-python/index.html',
        'GitHub': 'https://github.com/HXLStandard/libhxl-python/',
        'Changelog': 'https://github.com/HXLStandard/libhxl-python/blob/prod/CHANGELOG',
    },
    author='David Megginson',
    author_email='megginson@un.org',
    url='http://hxlproject.org',
    install_requires=[
        'jsonpath_ng',
        'ply',
        'python-dateutil',
        'python-io-wrapper>=0.2',
        'requests',
        'structlog',
        'unidecode',
        'urllib3',
        'wheel',
        'xlrd3>=1.1.0',
    ],
    packages=['hxl', 'hxl.formulas'],
    package_data={'hxl': ['*.json']},
    include_package_data=True,
    test_suite='tests',
    tests_require = [
        'pytest',
        'mock'
    ],
    entry_points={
        'console_scripts': [
            'hxladd = hxl.scripts:hxladd',
            'hxlappend = hxl.scripts:hxlappend',
            'hxlclean = hxl.scripts:hxlclean',
            'hxlcount = hxl.scripts:hxlcount',
            'hxlcut = hxl.scripts:hxlcut',
            'hxldedup = hxl.scripts:hxldedup',
            'hxlexpand = hxl.scripts:hxlexpand',
            'hxlexplode = hxl.scripts:hxlexplode',
            'hxlfill = hxl.scripts:hxlfill',
            'hxlimplode = hxl.scripts:hxlimplode',
            'hxlhash = hxl.scripts:hxlhash',
            'hxlinfo = hxl.scripts:hxlinfo',
            'hxlmerge = hxl.scripts:hxlmerge',
            'hxlrename = hxl.scripts:hxlrename',
            'hxlreplace = hxl.scripts:hxlreplace',
            'hxlselect = hxl.scripts:hxlselect',
            'hxlsort = hxl.scripts:hxlsort',
            'hxlspec = hxl.scripts:hxlspec',
            'hxltag = hxl.scripts:hxltag',
            'hxlvalidate = hxl.scripts:hxlvalidate'
        ]
    }
)
