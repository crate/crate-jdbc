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

.. _Gradle: https://gradle.org/