.. release:

Releasing a new Version on PyPi
================================

Things to consider
------------------

Version Bump
____________

For any update on PyPi we need a new version number.
You can manually edit the file `spooq/_version.py` to change the version number.
This is reflected in the `setup.py` and consequently in the release version number.

Documentation
_____________

Please don't forget to also update the documentation accordingly.
This is either done directly in the source code as docstrings or for more overview-centered topics
in the rst file under `docs/source`.

Changelog
_________

Please add your changes to the CHANGELOG.rst

Automatic Publishing via Github Action
--------------------------------------

The current Spooq version is automatically published on PyPi after a release on github is created.


Manual Publishing from Command Line
-----------------------------------

Create the Distribution Files
_____________________________

.. code-block:: bash

    $ python setup.py sdist bdist_wheel

Upload to Test-PyPi
___________________

.. code-block:: bash

    $ pipenv shell
    $ twine upload --repository-url https://test.pypi.org/legacy/ dist/

Your new version is available at https://test.pypi.org/project/Spooq/.
Beware, that the test PyPi uses different credentials than the real PyPi.
You can get the credentials from your favourite collaborator.

Upload to Real PyPi
___________________

.. code-block:: bash

    $ pipenv shell
    $ twine upload dist/

Your new version is available at https://pypi.org/project/Spooq/.
You can get the credentials from your favourite collaborator.
