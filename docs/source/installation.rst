Installation
=========================

Via Pip
-------

.. code-block:: bash

    $ pip install spooq

Build wheel file
----------------
.. code-block:: bash

    $ cd spooq
    $ python setup.py sdist bdist_wheel

The output is stored as ``dist/Spooq-<VERSION_NUMBER>-py3-none-any.whl`` and Spooq-<VERSION_NUMBER>.tar.gz.

Build egg file
--------------

.. code-block:: bash

    $ cd spooq
    $ python setup.py bdist_egg

The output is stored as ``dist/Spooq-<VERSION_NUMBER>-py3.7.egg``

Build zip file
--------------

.. code-block:: bash

    $ zip_file_name="Spooq-$(grep "__version__" spooq/_version.py | cut -d " " -f 3 | tr -d \").zip"
    $ zip -r $zip_file_name spooq

The output is stored as ``Spooq-<VERSION_NUMBER>.zip``.

Include pre-build package (egg or zip) with Spark
---------------------------------------------------------

For Submitting or Launching Spark:

.. code-block:: bash

    $ pyspark --py-files Spooq-<VERSION_NUMBER>.egg

The library has still to be imported in the pyspark application!

Within Running Spark Session:

.. code-block:: python

    >>> sc.addFile("Spooq-<VERSION_NUMBER>.egg")
    >>> import spooq

Install local repository as package
-----------------------------------

.. code-block:: bash

    $ cd spooq
    $ python setup.py install

Install Spooq directly from git
--------------------------------
.. code-block:: bash

    $ pip install git+https://github.com/Breaka84/Spooq@master


Development, Testing, and Documenting
------------------------------------------------------
Please refer to :ref:`dev_setup`.
