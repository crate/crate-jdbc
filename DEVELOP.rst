===========
DEVELOPMENT
===========

Build
=====

This project uses Gradle_ as build tool. It can be invoked by
executing ``./gradlew``. The first time this command is executed it is
bootstrapped automatically, therefore there is no need to install
gradle on the system.

Testing
=======

Unit tests are run like this::

  ./gradlew test

Build and Upload
================

Before creating a new distribution, a new version and tag must be created:

 - Add a new version to the ``io.crate.client.jdbc.CrateDriverVersion`` class.

 - Point the CURRENT version in that class to the newly added version.

 - Add a note for the new version at the ``CHANGES.txt`` file.

 - Commit e.g. using message 'prepare release x.x.x'.

 - Tag commit using the ``./devtools/create_tag.sh`` script.

 - Push to origin

Now everything is ready for building a new distribution, either
manually or let jenkins do the job as usual :-)

Building a signed jar and uploading to maven repository
is done by gradle with the command::

    ./gradlew uploadArchives

