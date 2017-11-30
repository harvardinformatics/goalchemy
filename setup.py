# -*- coding: utf-8 -*-

'''


Created on  2017-04-12 16:13:05

@author: Aaron Kitzmiller <aaron_kitzmiller@harvard.edu>
@copyright: 2017 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''

from setuptools import setup, find_packages
import re

def getVersion():
    version = '0.0.0'
    with open('goa/__init__.py','r') as f:
        contents = f.read().strip()

    m = re.search(r"__version__ = '([\d\.]+)'", contents)
    if m:
        version = m.group(1)
    return version

setup(
    name="goalchemy",
    version=getVersion(),
    author='Aaron Kitzmiller <aaron_kitzmiller@harvard.edu>',
    author_email='aaron_kitzmiller@harvard.edu',
    description='A loader for Uniprot GOA',
    license='LICENSE.txt',
    url='http://pypi.python.org/pypi/goalchemy/',
    packages=find_packages(),
    long_description='Loader for Uniprot GOA',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
    ],
    entry_points={
        'console_scripts': [
            'goa = goa.cli:main'
        ]
    },
    install_requires=[
        'MySQL-python>=1.2.5',
    ],
)
