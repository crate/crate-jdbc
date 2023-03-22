.. _index:

##########################
CrateDB legacy JDBC driver
##########################

.. rubric:: Table of contents

.. contents::
    :local:
    :depth: 1


************
Introduction
************

A `JDBC`_ driver for `CrateDB`_, based on the `PostgreSQL JDBC Driver`_.

This is a `type 4 JDBC driver`_ written in pure Java. It communicates with the
database using the `PostgreSQL Wire Protocol`_.

`JDBC`_  is a standard Java API that provides common interfaces for accessing
databases in Java.


*************
Documentation
*************

For general help about `JDBC`_, please consult the `JDBC tutorial`_ and the `JDBC
API documentation`_.

.. toctree::
    :titlesonly:

    getting-started
    connect
    appendices/index


.. _examples:

Examples
========

- The `Basic example for connecting to CrateDB using JDBC`_ demonstrates
  CrateDB's PostgreSQL wire protocol compatibility by exercising a basic
  example using both the vanilla pgJDBC Driver and the CrateDB JDBC Driver.
- The `sample application`_ and the corresponding `sample application
  documentation`_ demonstrate the use of the driver on behalf of an example
  "guestbook" application, using `Spring Data JDBC`_.


.. _Basic example for connecting to CrateDB using JDBC: https://github.com/crate/cratedb-examples/tree/main/by-language/java-jdbc
.. _CrateDB: https://crate.io/products/cratedb/
.. _hosted on GitHub: https://github.com/crate/crate-jdbc/
.. _JDBC API documentation: https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/
.. _JDBC tutorial: https://docs.oracle.com/javase/tutorial/jdbc/basics/
.. _JDBC: https://en.wikipedia.org/wiki/Java_Database_Connectivity
.. _PostgreSQL JDBC Driver: https://github.com/pgjdbc/pgjdbc
.. _PostgreSQL Wire Protocol: https://www.postgresql.org/docs/current/protocol.html
.. _sample application: https://github.com/crate/crate-sample-apps/tree/main/java-spring
.. _sample application documentation: https://github.com/crate/crate-sample-apps/blob/main/java-spring/documentation.md
.. _Spring Data JDBC: https://spring.io/projects/spring-data-jdbc/
.. _type 4 JDBC driver: https://en.wikipedia.org/wiki/JDBC_driver#Type_4_driver_%E2%80%93_Database-Protocol_driver/Thin_Driver(Pure_Java_driver)
