#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import re
from setuptools import setup, find_packages

version_filename = "spooq/_version.py"
with open(version_filename, "rt") as version_filename:
    version_file = version_filename.read()
    version_regex    = r"""^__version__ = ["\"]([^"\"]*)["\"]"""
    regex_result     = re.search(version_regex, version_file, re.M)
    if regex_result:
        version_string = regex_result.group(1)
    else:
        raise RuntimeError("Unable to find version string in %s." % (version_filename,))

with open("README.md") as readme_file:
    readme = readme_file.read()

with open("CHANGELOG.rst") as history_file:
    history = history_file.read()

with open("docs/source/index.rst") as sphinx_index_file:
    sphinx_index = sphinx_index_file.read()

requirements = [
    "pandas",
    "future"
]

setup(
    name="Spooq",
    version=version_string,
    description="""
        Spooq is a PySpark based helper library for ETL data ingestion pipeline in Data Lakes.
    """,
    long_description=readme + "\n\n" + history,
    # long_description=sphinx_index,
    long_description_content_type="text/markdown",
    author="David Hohensinn",
    author_email="breaka@gmx.at",
    url="https://spooq.readthedocs.io",
    include_package_data=False,
    packages=find_packages(exclude=['*tests*']),
    install_requires=requirements,
    zip_safe=False,
    keywords=[
        "spooq", "spark", "hive", "cloudera", "hadoop", "etl",
        "data ingestion", "data wrangling", "databricks", "big data",
        "batch", "streaming"
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.7",
    ]
)
