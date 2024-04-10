==========================
CrateDB legacy JDBC driver
==========================

|tests| |docs| |rtd| |maven-central|

|

A `JDBC`_ driver for `CrateDB`_, based on the `PostgreSQL JDBC Driver`_.

This is a `type 4 JDBC driver`_ written in pure Java. It communicates with the
database using the `PostgreSQL Wire Protocol`_.

`JDBC`_  is a standard Java API that provides common interfaces for accessing
databases in Java.

Installation
============

The driver comes in two variants, available on Maven Central at `crate-jdbc`_
and `crate-jdbc-standalone`_.

For a conventional install (using pre-built JAR files), follow the
`installation documentation`_. For setting up a development sandbox, to build
the JAR from source, please follow up reading the `developer guide`_.

Documentation and help
======================

- `CrateDB legacy JDBC driver documentation`_
- `CrateDB reference documentation`_
- `JDBC tutorial`_
- `JDBC API documentation`_
- `Developer guide`_
- `Contributing`_
- Other `support channels`_

Contributing
============

The CrateDB JDBC driver library is an open source project, and is `managed on
GitHub`_. We appreciate contributions of any kind. Thank you!


.. _Contributing: CONTRIBUTING.rst
.. _crate-jdbc: https://repo1.maven.org/maven2/io/crate/crate-jdbc/
.. _crate-jdbc-standalone: https://repo1.maven.org/maven2/io/crate/crate-jdbc-standalone/
.. _Crate.io: http://crate.io/
.. _CrateDB: https://github.com/crate/crate
.. _CrateDB legacy JDBC driver documentation: https://crate.io/docs/projects/crate-jdbc/
.. _CrateDB reference documentation: https://crate.io/docs/reference/
.. _developer guide: DEVELOP.rst
.. _installation documentation: https://crate.io/docs/jdbc/en/latest/getting-started.html
.. _JDBC: https://en.wikipedia.org/wiki/Java_Database_Connectivity
.. _JDBC API documentation: https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/
.. _JDBC tutorial: https://docs.oracle.com/javase/tutorial/jdbc/basics/
.. _managed on GitHub: https://github.com/crate/crate-jdbc
.. _PostgreSQL JDBC Driver: https://github.com/pgjdbc/pgjdbc
.. _PostgreSQL Wire Protocol: https://crate.io/docs/crate/reference/en/latest/interfaces/postgres.html
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
