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


*****
About
*****

Overview
========

- This JDBC driver is needed in certain scenarios like the one outlined at
  `Apache Kafka, Apache Flink, and CrateDB`_.
- Officially, and for general purpose use, we recommend to use the canonical
  `PostgreSQL JDBC Driver`_ instead.

.. _implementations:
.. _jdbc-implementation:

What's inside
=============

The driver is based upon a fork of the `PostgreSQL JDBC Driver`_, see
`pgjdbc driver fork`_.

Please take notice of the corresponding implementation notes:

- `CallableStatement`_ is not supported, as CrateDB itself does not support
  stored procedures.
- `DataSource`_ is not supported.
- `ParameterMetaData`_, e.g. as returned by `PreparedStatement`_, is not
  supported.
- `ResultSet`_ objects are read only (``TYPE_FORWARD_ONLY``, ``CONCUR_READ_ONLY``),
  so changes to a ``ResultSet`` are not supported.


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
- The article `Build a data ingestion pipeline using Kafka, Flink, and CrateDB`_,
  and the accompanying repositories `Apache Kafka, Apache Flink, and CrateDB`_
  and `Flink example jobs for CrateDB`_.


.. _Apache Kafka, Apache Flink, and CrateDB: https://github.com/crate/cratedb-examples/tree/main/stacks/kafka-flink
.. _Basic example for connecting to CrateDB using JDBC: https://github.com/crate/cratedb-examples/tree/main/by-language/java-jdbc
.. _Build a data ingestion pipeline using Kafka, Flink, and CrateDB: https://dev.to/crate/build-a-data-ingestion-pipeline-using-kafka-flink-and-cratedb-1h5o
.. _CallableStatement: https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
.. _CrateDB: https://crate.io/products/cratedb/
.. _DataSource: https://docs.oracle.com/javase/8/docs/api/javax/sql/DataSource.html
.. _Flink example jobs for CrateDB: https://github.com/crate/cratedb-flink-jobs
.. _hosted on GitHub: https://github.com/crate/crate-jdbc/
.. _JDBC API documentation: https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/
.. _JDBC tutorial: https://docs.oracle.com/javase/tutorial/jdbc/basics/
.. _JDBC: https://en.wikipedia.org/wiki/Java_Database_Connectivity
.. _ParameterMetaData: https://docs.oracle.com/javase/8/docs/api/java/sql/ParameterMetaData.html
.. _pgjdbc driver fork: https://github.com/crate/pgjdbc
.. _PostgreSQL JDBC Driver: https://github.com/pgjdbc/pgjdbc
.. _PostgreSQL Wire Protocol: https://www.postgresql.org/docs/current/protocol.html
.. _PreparedStatement: https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html
.. _ResultSet: https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html
.. _sample application: https://github.com/crate/crate-sample-apps/tree/main/java-spring
.. _sample application documentation: https://github.com/crate/crate-sample-apps/blob/main/java-spring/documentation.md
.. _Spring Data JDBC: https://spring.io/projects/spring-data-jdbc/
.. _type 4 JDBC driver: https://en.wikipedia.org/wiki/JDBC_driver#Type_4_driver_%E2%80%93_Database-Protocol_driver/Thin_Driver(Pure_Java_driver)
