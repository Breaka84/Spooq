Transformer Base Class
==============================

.. automodule:: spooq.transformer.transformer
    :no-members:
    :noindex:

.. _custom_transformer:

Create your own Transformer
----------------------------

Let your transformer class inherit from the transformer base class.
This includes the name, string representation and logger attributes from the superclass.

| The only mandatory thing is to provide a `transform()` method which
| **takes** a
| => *PySpark DataFrame!*
| and **returns** a
| => *PySpark DataFrame!*

All configuration and parameterization should be done while initializing the class instance.

Here would be a simple example for a transformer which drops records without an Id:

Exemplary Sample Code
^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: create_transformer/no_id_dropper.py
    :caption: spooq/transformer/no_id_dropper.py:
    :language: python

References to include
^^^^^^^^^^^^^^^^^^^^^^^^

This makes it possible to import the new transformer class directly
from `spooq.transformer` instead of `spooq.transformer.no_id_dropper`.
It will also be imported if you use `from spooq.transformer import *`.

.. literalinclude:: create_transformer/init.diff
    :caption: spooq/transformer/__init__.py:
    :language: udiff

Tests
^^^^^^^^^^^^^^^^^^^^^^^^

One of Spooq's features is to provide tested code for multiple data pipelines.
Please take your time to write sufficient unit tests!
You can reuse test data from `tests/data` or create a new schema / data set if needed.
A SparkSession is provided as a global fixture called `spark_session`.

.. literalinclude:: create_transformer/test_no_id_dropper.py
    :caption: tests/unit/transformer/test_no_id_dropper.py:
    :language: python

Documentation
^^^^^^^^^^^^^^^^^^^^^^^^
You need to create a `rst` for your transformer
which needs to contain at minimum the `automodule` or the `autoclass` directive.

.. literalinclude:: create_transformer/no_id_dropper.rst.code
    :caption: docs/source/transformer/no_id_dropper.rst:
    :language: RST

To automatically include your new transformer in the HTML / PDF documentation
you need to add it to a `toctree` directive. Just refer to your newly created
`no_id_dropper.rst` file within the transformer overview page.

.. literalinclude:: create_transformer/overview.diff
    :caption: docs/source/transformer/overview.rst:
    :language: udiff

That should be it!
