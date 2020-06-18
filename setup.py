#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import re
from setuptools import setup, find_packages

version_filename = 'src/spooq2/_version.py'
version_file     = open(version_filename, "rt").read()
version_regex    = r"^__version__ = ['\"]([^'\"]*)['\"]"
regex_result     = re.search(version_regex, version_file, re.M)

if regex_result:
    version_string = regex_result.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (version_filename,))


with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'pandas',
    'future'
]

setup(
    name='Spooq2',
    version=version_string,
    description="""
        Spooq helps to run basic ETL processes in Data Lakes based on Apache Spark.
        All extractors, transformers, and loaders are single components which can be mixed to one's liking.
    """,
    long_description=readme + '\n\n' + history,
    author="David Eigenstuhler",
    author_email=['david.eigenstuhler@runtastic.com', 'breaka@gmx.at'],
    url='https://github.com/Breaka84/Spooq',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords=[
        'spooq', 'spark', 'hive', 'cloudera', 'hadoop', 'etl',
        'data ingestion', 'data wrangling', 'databricks', 'big data'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ]
)
