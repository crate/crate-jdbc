===============
Developer guide
===============

These instructions show you how to build the CrateDB JDBC driver from the
source code, and how to invoke the test suite. For a conventional install
(using pre-built JAR files), follow the `installation documentation`_.


Acquire source
==============

Clone the repository::

    $ git clone --recursive https://github.com/crate/crate-jdbc

Change directory into the repository::

    $ cd crate-jdbc

Building
========

This project uses Gradle_ as build tool.

Gradle can be invoked like so::

    $ ./gradlew

The first time this command is executed, Gradle is downloaded and bootstrapped
for you automatically.

Build a regular JAR file::

    $ ./gradlew jar

Or, build a JAR file that includes dependencies::

    $ ./gradlew standaloneJar

Afterwards you can find the JAR file in the ``build/lib`` directory.

Note that building the JAR files requires your environment locale set to
``UTF-8``.


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

For debugging purposes, integration tests can be run against any CrateDB build.
Build tar.gz file by running ./gradlew distTar from crate repository and set
path to the generated file to the ``CRATE_PATH`` environment variable, e.g.::

    $ CRATE_PATH=../crate/app/build/distributions/crate-4.7.0-SNAPSHOT-3edf1b4f2f2.tar.gz ./gradlew test

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

To make changes to the RTD configuration (e.g., to activate or deactivate a
release version), please contact the `@crate/docs`_ team.

Writing Documentation
=====================

The docs live under the docs directory.

The docs are written written with ReStructuredText_ and processed with Sphinx_.

Build the docs by running::

    cd docs
    make html
    open .crate-docs/.build/index.html

The docs are automatically built from Git by `Read the Docs`_ and there is
nothing special you need to do to get the live docs to update.

.. _@crate/docs: https://github.com/orgs/crate/teams/docs
.. _Gradle: https://gradle.org/
.. _installation documentation: https://crate.io/docs/jdbc/en/latest/getting-started.html
.. _ReStructuredText: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx-doc.org/
.. _Read the Docs: http://readthedocs.org/
.. _versions hosted on ReadTheDocs: https://readthedocs.org/projects/crate-jdbc/versions/
