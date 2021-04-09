#! /usr/bin/env python

from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

long_description = '''
DataJoint-DataPub: datajoint.io data publication library.
see README for further information.
'''

with open(path.join(here, 'requirements.txt')) as f:
    requirements = [r.rstrip().split('#')[0].rstrip() for r in f]


setup(
    name='datajoint-datapub',
    version='0.0.1',
    description='DataJoint DataPub',
    long_description=long_description,
    author='Vathes, Inc.',
    author_email='chris@vathes.com',
    license='All Rights Reserved',
    url='https://github.com/vathes/datajoint-datapub',
    keywords='DataJoint MySQL',
    install_requires=requirements,
    packages=find_packages(exclude=['doc', 'test']),
    scripts=['scripts/djpub.py']
)
