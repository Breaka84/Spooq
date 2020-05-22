Loader Base Class
==============================

.. automodule:: spooq2.loader.loader

.. _custom_loader:

Create your own Loader
----------------------------

Let your loader class inherit from the loader base class.
This includes the name, string representation and logger attributes from the superclass.

| The only mandatory thing is to provide a `load()` method which 
| **takes** a
| => *PySpark DataFrame!*
| and **returns**
| *nothing* (or at least the API does not expect anything)

All configuration and parameterization should be done while initializing the class instance.

Here would be a simple example for a loader which save a DataFrame to parquet files:

Exemplary Sample Code
^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: create_loader/parquet.py
    :caption: src/spooq2/loader/parquet.py:
    :language: python

References to include
^^^^^^^^^^^^^^^^^^^^^^^^

This makes it possible to import the new loader class directly 
from `spooq2.loader` instead of `spooq2.loader.parquet`. 
It will also be imported if you use `from spooq2.loader import *`.

.. literalinclude:: create_loader/init.diff
    :caption: src/spooq2/loader/__init__.py:
    :language: udiff

Tests
^^^^^^^^^^^^^^^^^^^^^^^^

One of Spooq2's features is to provide tested code for multiple data pipelines. 
Please take your time to write sufficient unit tests!
You can reuse test data from `tests/data` or create a new schema / data set if needed.
A SparkSession is provided as a global fixture called `spark_session`.

.. literalinclude:: create_loader/test_parquet.py
    :caption: tests/unit/loader/test_parquet.py:
    :language: python 

Documentation
^^^^^^^^^^^^^^^^^^^^^^^^

You need to create a `rst` for your loader
which needs to contain at minimum the `automodule` or the `autoclass` directive.

.. literalinclude:: create_loader/parquet.rst.code
    :caption: docs/source/loader/parquet.rst:
    :language: RST

To automatically include your new loader in the HTML / PDF documentation 
you need to add it to a `toctree` directive. Just refer to your newly created 
`parquet.rst` file within the loader overview page.

.. literalinclude:: create_loader/overview.diff
    :caption: docs/source/loader/overview.rst:
    :language: udiff

That should be it!
