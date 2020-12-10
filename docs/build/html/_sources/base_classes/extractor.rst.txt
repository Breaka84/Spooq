Extractor Base Class
==============================

.. automodule:: spooq2.extractor.extractor
    :no-members:
    :noindex:

.. _custom_extractor:

Create your own Extractor
----------------------------

Let your extractor class inherit from the extractor base class.
This includes the name, string representation and logger attributes from the superclass.

| The only mandatory thing is to provide an `extract()` method which
| **takes**
| => *no input parameters*
| and **returns** a
| => *PySpark DataFrame!*

All configuration and parameterization should be done while initializing the class instance.

Here would be a simple example for a CSV Extractor:

Exemplary Sample Code
^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: create_extractor/csv_extractor.py
    :caption: src/spooq2/extractor/csv_extractor.py:
    :language: python


References to include
^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: create_extractor/init.diff
    :caption: src/spooq2/extractor/__init__.py:
    :language: udiff

Tests
^^^^^^^^^^^^^^^^^^^^^^^^

One of Spooq2's features is to provide tested code for multiple data pipelines.
Please take your time to write sufficient unit tests!
You can reuse test data from `tests/data` or create a new schema / data set if needed.
A SparkSession is provided as a global fixture called `spark_session`.

.. literalinclude:: create_extractor/test_csv.py
    :caption: tests/unit/extractor/test_csv.py:
    :language: python

Documentation
^^^^^^^^^^^^^^^^^^^^^^^^

You need to create a `rst` for your extractor
which needs to contain at minimum the `automodule` or the `autoclass` directive.

.. literalinclude:: create_extractor/csv.rst.code
    :caption: docs/source/extractor/csv.rst:
    :language: RST

To automatically include your new extractor in the HTML documentation you need to add it to a `toctree` directive. Just refer to your newly created
`csv.rst` file within the extractor overview page.

.. literalinclude:: create_extractor/overview.diff
    :caption: docs/source/extractor/overview.rst:
    :language: udiff

That should be all!
