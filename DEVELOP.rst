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

Integration tests use a randomized CrateDB version. If you want to run the
tests against a specific version you can either use the ``CRATE_VERSION`` or
``CRATE_URL`` environment variable, e.g.::

    $ CRATE_VERSION=2.3.4 ./gradlew test

or::

    $ CRATE_URL=https://cdn.crate.io/downloads/releases/nightly/crate-0.58.0-201611210301-7d469f8.tar.gz ./gradlew test

Preparing a Release
===================

To create a new release, you must:

- Add a new version to the ``io.crate.client.jdbc.CrateDriverVersion`` class

- Point the ``CURRENT`` version in that class to the newly added version

- Add a note for the new version at the ``CHANGES.txt`` file

- Commit your changes with a message like "prepare release x.x.x"

- Push to origin

- Create a tag by running ``./devtools/create_tag.sh``

- Archive docs for old releases (see section below)

At this point, Jenkins will take care of building and uploading the release to
the Maven repository.

However, if you'd like to do this manually, you can run::

    $ ./gradlew uploadArchives

Archiving Docs Versions
-----------------------

Check the `versions hosted on ReadTheDocs`_.

We should only be hosting the docs for `latest`, the last three minor release
branches of the last major release, and the last minor release branch
corresponding to the last two major releases.

For example:

- ``latest``
- ``2.2``
- ``2.1``
- ``2.0``
- ``1.14``

Because this project has not any releases with major version of ``0``, we stop
at ``1.14``.

Sometimes you might find that there are multiple older releases that need to be
archived.

You can archive releases by selecting *Edit*, unselecting the *Active*
checkbox, and then saving.

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
.. _versions hosted on ReadTheDocs: https://readthedocs.org/projects/crate-jdbc/versions/
