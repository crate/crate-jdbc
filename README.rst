===================
CrateDB JDBC Driver
===================

|tests| |docs| |rtd| |maven-central|

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

The driver comes in two variants, available on Maven Central at `crate-jdbc`_
and `crate-jdbc-standalone`_.

The package specification is ``io.crate:crate-jdbc-standalone:2.6.0``.

Build
=====

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

Note that building the JAR files requires your environment locale set to
``UTF-8``.

Contributing
============

This project is primarily maintained by Crate.io_, but we welcome community
contributions!

See the `developer docs`_ and the `contribution docs`_ for more information.

Help
====

Looking for more help?

- Read the `project docs`_
- Check out our `support channels`_


.. _compatibility notes: https://crate.io/docs/clients/jdbc/en/latest/compatibility.html
.. _contribution docs: CONTRIBUTING.rst
.. _crate-jdbc: https://repo1.maven.org/maven2/io/crate/crate-jdbc/
.. _crate-jdbc-standalone: https://repo1.maven.org/maven2/io/crate/crate-jdbc-standalone/
.. _Crate.io: http://crate.io/
.. _CrateDB: https://github.com/crate/crate
.. _developer docs: DEVELOP.rst
.. _getting started: https://crate.io/docs/projects/crate-jdbc/getting-started.html
.. _Java 8: http://www.oracle.com/technetwork/java/javase/downloads/index.html
.. _JDBC: http://www.oracle.com/technetwork/java/overview-141217.html
.. _OpenJDK: http://openjdk.java.net/projects/jdk8/
.. _Oracle’s Java: http://www.java.com/en/download/help/mac_install.xml
.. _PostgreSQL Wire Protocol: https://crate.io/docs/crate/reference/en/latest/interfaces/postgres.html
.. _project docs: https://crate.io/docs/projects/crate-jdbc/
.. _support channels: https://crate.io/support/
.. _type 4 JDBC driver: https://en.wikipedia.org/wiki/JDBC_driver#Type_4_driver_.E2.80.93_Database-Protocol_driver_.28Pure_Java_driver.29



.. |tests| image:: https://github.com/crate/crate-jdbc/actions/workflows/tests.yml/badge.svg?branch=master
    :alt: Build status
    :target: https://github.com/crate/crate-jdbc/actions/workflows/tests.yml?query=branch=master

.. |docs| image:: https://github.com/crate/crate-jdbc/actions/workflows/docs.yml/badge.svg
    :alt: Documentation: Link checker
    :target: https://github.com/crate/crate-jdbc/actions/workflows/docs.yml

.. |rtd| image:: https://readthedocs.org/projects/crate-jdbc/badge/
    :alt: Read the Docs status
    :target: https://readthedocs.org/projects/crate-jdbc/

.. |maven-central| image:: https://maven-badges.herokuapp.com/maven-central/io.crate/crate-jdbc/badge.svg
    :alt: Latest release on Maven Central
    :target: https://repo1.maven.org/maven2/io/crate/crate-jdbc/
