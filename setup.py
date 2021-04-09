#! /usr/bin/env python

from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

long_description = '''
DJArchive-Client: datajoint.io data publication/archive client library.
see README for further information.
'''

with open(path.join(here, 'requirements.txt')) as f:
    requirements = [r.rstrip().split('#')[0].rstrip() for r in f]


setup(
    name='djarchive-client',
    version='0.0.1',
    description='DataJoint data archive client',
    long_description=long_description,
    author='Vathes, Inc.',
    author_email='chris@vathes.com',
    license='All Rights Reserved',
    url='https://github.com/datajoint/djarchive-client',
    keywords='DataJoint MySQL',
    install_requires=requirements,
    packages=find_packages(exclude=['doc', 'test']),
    scripts=['scripts/djarchive.py']
)
