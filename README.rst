===================
CrateDB JDBC Driver
===================

.. image:: https://travis-ci.org/crate/crate-jdbc.svg?branch=master
        :target: https://travis-ci.org/crate/crate-jdbc
        :alt: Test

|

A `JDBC`_ driver for `CrateDB`_.

JDBC is a core API for Java 1.1 and later. It provides a standard set of
interfaces to SQL-compliant databases.

This is a `type 4 JDBC driver`_. The driver is written in pure Java, and
communicates with the database using the `PostgreSQL Wire Protocol`_.

Prerequisites
=============

The CrateDB JDBC driver requires `Java 8`_, preferably update 20 or later. We
recommend using `Oracle’s Java`_ on macOS and `OpenJDK`_ on Linux Systems.

Consult the `compatibility notes`_ for additional information.

Installation
============

These instructions show you how to build the CrateDB JDBC driver from the
source code. For a conventional install (using pre-built JAR files) follow the
`getting started`_ documentation.

Clone the repository::

    $ git clone --recursive https://github.com/crate/crate-jdbc

Change directory into the repository::

    $ cd crate-jdbc

Build a regular JAR file::

    $ ./gradlew jar

Or, build a JAR file that includes dependencies::

    $ ./gradlew standaloneJar

Afterwards you can find the JAR file in the ``build/lib`` directory.

Contributing
============

This project is primarily maintained by Crate.io_, but we welcome community
contributions!

See the `developer docs`_ and the `contribution docs`_ for more information.

Help
====

Looking for more help?

- Read `the project documentation`_
- Check `StackOverflow`_ for common problems
- Chat with us on `Slack`_
- Get `paid support`_

.. _compatibility notes: https://crate.io/docs/clients/jdbc/en/latest/compatibility.html
.. _contribution docs: CONTRIBUTING.rst
.. _Crate.io: http://crate.io/
.. _CrateDB: https://github.com/crate/crate
.. _developer docs: DEVELOP.rst
.. _getting started: https://crate.io/docs/projects/crate-jdbc/getting-started.html
.. _Java 8: http://www.oracle.com/technetwork/java/javase/downloads/index.html
.. _JDBC: http://www.oracle.com/technetwork/java/overview-141217.html
.. _Jitpack: https://jitpack.io/#crate/crate-jdbc
.. _OpenJDK: http://openjdk.java.net/projects/jdk8/
.. _Oracle’s Java: http://www.java.com/en/download/help/mac_install.xml
.. _paid support: https://crate.io/pricing/
.. _PostgreSQL Wire Protocol: https://crate.io/docs/crate/reference/en/latest/interfaces/postgres.html
.. _Slack: https://crate.io/docs/support/slackin/
.. _StackOverflow: https://stackoverflow.com/tags/crate
.. _the project documentation: https://crate.io/docs/projects/crate-jdbc/
.. _type 4 JDBC driver: https://en.wikipedia.org/wiki/JDBC_driver#Type_4_driver_.E2.80.93_Database-Protocol_driver_.28Pure_Java_driver.29
