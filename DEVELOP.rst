===============
Developer Guide
===============

Building
========

This project uses Gradle_ as build tool.

Gradle can be invoked like so::

    $ ./gradlew

The first time this command is executed, Gradle is downloaded and bootstrapped
for you automatically.

Testing
=======

Run the unit tests like so::

    $ ./gradlew test

Preparing a Release
===================

To create a new release, you must:

- Add a new version to the ``io.crate.client.jdbc.CrateDriverVersion`` class

- Point the ``CURRENT`` version in that class to the newly added version

- Add a note for the new version at the ``CHANGES.txt`` file

- Commit your changes with a message like "prepare release x.x.x"

- Push to origin

- Create a tag by running ``./devtools/create_tag.sh``

At this point, Jenkins will take care of building and uploading the release to
the Maven repository.

However, if you'd like to do this manually, you can run::

    $ ./gradlew uploadArchives

Writing Documentation
=====================

The docs live under the docs directory.

The docs are written written with ReStructuredText_ and processed with Sphinx_.

Prepare the docs setup by running::

  $ python3 bootstrap.py
  $ ./bin/buildout -N

Build the docs by running::

  $ bin/sphinx

The output can then be found in the `out/html` directory.

The docs are automatically built from Git by `Read the Docs`_ and there is
nothing special you need to do to get the live docs to update.

.. _Gradle: https://gradle.org/
.. _ReStructuredText: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx-doc.org/
.. _Read the Docs: http://readthedocs.org/
