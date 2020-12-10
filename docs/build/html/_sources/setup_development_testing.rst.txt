.. _dev_setup:

Setup for Development, Testing, Documenting
===========================================

**Attention: The current version of Spooq is designed (and tested) for Python 2.7/3.7/3.8 on ubuntu, manjaro linux and WSL2 (Windows Subsystem Linux).**

Prerequisites
-------------

* python 2.7 or python 3.7/3.8
* Java 8 (jdk8-openjdk)
* pipenv
* Latex (for PDF documentation)


Setting up the Environment
--------------------------
The requirements are stored in the file `Pipfile` separated for production and development packages.

To install the packages needed for development and testing run the following command:

.. code-block:: bash

    $ pipenv install --dev

This will create a virtual environment in `~/.local/share/virtualenvs`.

If you want to have your virtual environment installed as a sub-folder (.venv) you have to set the
environment variable `PIPENV_VENV_IN_PROJECT` to 1.

To remove a virtual environment created with pipenv just change in the folder where you created it
and execute `pipenv --rm`.

Activate the Virtual Environment
--------------------------------

.. code-block:: bash
    :caption: To activate the virtual environment enter:

    $ pipenv shell

.. code-block:: bash
    :caption: To deactivate the virtual environment simply enter:

    $ exit
    # or close the shell

For more commands of pipenv call `pipenv -h`.

Creating Your Own Components
----------------------------------

Implementing new extractors, transformers, or loaders is fairly straightforward.
Please refer to following descriptions and examples to get an idea:

* :ref:`custom_extractor`
* :ref:`custom_transformer`
* :ref:`custom_loader`

Configure Spark
----------------
The tests use per default the Spark 3 package in the ``bin/spark3`` folder.
To use a different Spark installation for the tests, you have to either:

* set ``spark_home`` in the ``pytest.ini`` (tests/pytest.ini) to the new location **or**
* set the environment variable ``SPARK_HOME`` and comment out ``spark_home`` in pytest.ini

Running Tests
-------------
The tests are implemented with the `pytest <https://docs.pytest.org/en/3.10.1/>`_ framework.

.. code-block:: bash
    :caption: Start all tests:

    $ pipenv shell
    $ cd tests
    $ pytest

Test Plugins
^^^^^^^^^^^^

Those are the most useful plugins automatically used:

`html <https://github.com/pytest-dev/pytest-html>`_
***************************************************

.. code-block:: bash
    :caption: Generate an HTML report for the test results:

    $ pytest --html=report.html


`random-order <https://pythonhosted.org/pytest-random-order/>`_
***************************************************************

Shuffles the order of execution for the tests to avoid / discover dependencies of the tests.

Randomization is set by a seed number. To re-test the same order of execution where you found
an error, just set the seed value to the same as for the failing test.
To temporarily disable this feature run with `pytest -p no:random-order -v`

`cov <https://pytest-cov.readthedocs.io/en/v2.6.0/>`_
*******************************************************

Generates an HTML for the test coverage

.. code-block:: bash
    :caption: Get a test coverage report in the terminal:

    $ pytest --cov-report term --cov=spooq2

.. code-block:: bash
    :caption: Get the test coverage report as HTML

    $ pytest --cov-report html:cov_html --cov=spooq2


`ipdb <https://github.com/gotcha/ipdb>`_
***************************************************

To use ipdb (IPython Debugger) add following code at your breakpoint::
    >>> import ipdb
    >>> ipdb.set_trace()

You have to start pytest with `-s` if you want to use interactive debugger.

.. code-block:: bash

    $ pytest -s

Generate Documentation
--------------------------
This project uses `Sphinx <https://www.sphinx-doc.org/en/1.8/>`_ for creating its documentation.
Graphs and diagrams are produced with PlantUML.

The main documentation content is defined as docstrings within the source code.
To view the current documentation open `docs/build/html/index.html`
or `docs/build/latex/spooq2.pdf` in your application of choice.
There are symlinks in the root folder for symplicity:

* Documentation.html
* Documentation.pdf

Although, if you are reading this, you have probably already found the documentation...

Diagrams
^^^^^^^^^^^^^^^^
For generating the graphs and diagrams, you need a working plantuml installation
on your computer! Please refer to `sphinxcontrib-plantuml <https://pypi.org/project/sphinxcontrib-plantuml/>`_.

HTML
^^^^^^^^^^^^

.. code-block:: bash

    $ cd docs
    $ make html
    $ chromium build/html/index.html

PDF
^^^^^^^^^^^^
For generating documentation in the PDF format you need to have a working (pdf)latex installation
on your computer! Please refer to `TexLive <https://www.tug.org/texlive/>`_ on how to install
TeX Live - a compatible latex distribution. But beware, the download size is huge!

.. code-block:: bash

    $ cd docs
    $ make latexpdf
    $ evince build/latex/Spooq2.pdf


Configuration
^^^^^^^^^^^^^
Themes, plugins, settings, ... are defined in `docs/source/conf.py`.

`napoleon <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/>`_
******************************************************************************************************
Enables support for parsing docstrings in NumPy / Google Style

`intersphinx <http://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html>`_
******************************************************************************************************
Allows linking to other projects’ documentation. E.g., PySpark, Python2
To add an external project, at the documentation link to `intersphinx_mapping` in `conf.py`

`recommonmark <https://recommonmark.readthedocs.io/en/latest/>`_
******************************************************************************************************
This allows you to write CommonMark (Markdown) inside of Docutils & Sphinx projects instead
of rst.

`plantuml <https://github.com/sphinx-contrib/plantuml/>`_
******************************************************************************************************
Allows for inline Plant UML code (uml directive) which is automatically rendered into an
svg image and placed in the document. Allows also to source puml-files. See :ref:`architecture`
for an example.
