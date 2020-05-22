Installation / Deployment
=========================


Build egg file
--------------

.. code-block:: bash

    $ cd spooq2
    $ python setup.py bdist_egg

The output is stored as `dist/Spooq2-<VERSION_NUMBER>-py2.7.egg`

Build zip file
--------------

.. code-block:: bash

    $ cd spooq2
    $ rm temp.zip
    $ zip -r temp.zip src/spooq2
    $ mv temp.zip Spooq2_$(grep "__version__" src/spooq2/_version.py | \
        cut -d " " -f 3 | tr -d \").zip

The output is stored as `Spooq2-<VERSION_NUMBER>.zip`.

Include pre-build package (egg or zip) with Spark
---------------------------------------------------------

For Submitting or Launching Spark:

.. code-block:: bash

    $ pyspark --py-files Spooq2-<VERSION_NUMBER>.egg

The library still has to be imported in the pyspark application!

Within Running Spark Session::

    >>> sc.addFile("Spooq2-<VERSION_NUMBER>.egg")
    >>> import spooq2

Install local repository as package
-----------------------------------

.. code-block:: bash

    $ cd spooq2
    $ python setup.py install

Install Spooq2 directly from git
--------------------------------
.. code-block:: bash

    $ pip install git+https://github.com/breaka84/spooq@master


Development, Testing, and Documenting
------------------------------------------------------
Please refer to :ref:`dev_setup`.
