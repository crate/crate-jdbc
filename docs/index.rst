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
It can be used with CrateDB version 0.38.0 and newer.

This is a `JDBC Type 4 driver`_, adhering to the `JDBC 4.1 specification`_. It
is written in pure Java, and communicates with the database using the
`PostgreSQL Wire Protocol`_.

`JDBC`_  is a standard Java API that provides common interfaces for accessing
databases in Java.


.. _synopsis:

********
Synopsis
********

Connect to CrateDB instance running on ``localhost``.

.. code-block:: java

    import java.sql.Connection;
    import java.sql.DriverManager;

    Connection conn = DriverManager.getConnection("jdbc:crate://localhost:5432/");

Connect to CrateDB Cloud.

.. code-block:: java

    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.util.Properties;

    Properties connectionProps = new Properties();
    connectionProps.put("user", "admin");
    connectionProps.put("password", "<PASSWORD>");
    connectionProps.put("tcpKeepAlive", true);

    Connection conn = DriverManager.getConnection("jdbc:crate://example.aks1.westeurope.azure.cratedb.net:5432/", connectionProps);


.. _details:

*******
Details
*******

Overview
========

For general purpose use, we recommend to use the official `PostgreSQL JDBC
Driver`_.

*This* JDBC driver is needed in certain scenarios like the one outlined at
`Apache Kafka, Apache Flink, and CrateDB`_. The background is that, when using
the ``postgresql://`` JDBC driver prefix, the `Apache Flink JDBC Connector`_
will implicitly select the corresponding dialect implementation for PostgreSQL.

In turn, this will employ a few behaviours that strictly expect a PostgreSQL
server on the other end, so that some operations will fail on databases
offering wire-compatibility with PostgreSQL, but do not provide certain
features like the `hstore`_ or `jsonb`_ extensions. Also, tools like `Dataiku`_
need this driver to implement transaction commands like ``ROLLBACK`` as a
no-op.


.. _differences:

Differences
===========

The driver is based upon a fork of the `PostgreSQL JDBC Driver`_, see `pgjdbc
driver fork`_. On a high-level perspective, this list enumerates a few
behavioral differences.

- The CrateDB driver deserializes objects to a Map, the official one treats them as JSON.
- A few metadata functions have been adjusted to better support CrateDB's type system.

Read up on further details at the :ref:`internals` section.



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
    internals


.. _examples:

Examples
========

- The `Basic example for connecting to CrateDB and CrateDB Cloud using JDBC`_
  demonstrates CrateDB's PostgreSQL wire protocol compatibility by exercising a
  basic example using both the vanilla pgJDBC Driver and the CrateDB JDBC Driver.
- The `sample application`_ and the corresponding `sample application
  documentation`_ demonstrate the use of the driver on behalf of an example
  "guestbook" application, using `Spring Data JDBC`_.
- The article `Build a data ingestion pipeline using Kafka, Flink, and CrateDB`_,
  and the accompanying repositories `Apache Kafka, Apache Flink, and CrateDB`_
  and `Flink example jobs for CrateDB`_.



*******************
Project information
*******************

Resources
=========
- `Source code <https://github.com/crate/crate-jdbc>`_
- `Documentation <https://crate.io/docs/jdbc/>`_
- `Maven Repository <https://repo1.maven.org/maven2/io/crate/>`_

Contributions
=============
The CrateDB legacy JDBC driver library is an open source project, and is
managed on GitHub. Every kind of contribution, feedback, or patch, is much
welcome. `Create an issue`_ or submit a patch if you think we should include a
new feature, or to report or fix a bug.

Development
===========
In order to setup a development environment on your workstation, please head
over to the `development sandbox`_ documentation. When you see the software
tests succeed, you should be ready to start hacking.

License
=======
The project is licensed under the terms of the Apache 2.0 license, like
`CrateDB itself <CrateDB source_>`_, see `LICENSE`_.


.. _Apache Flink JDBC Connector: https://github.com/apache/flink-connector-jdbc
.. _Apache Kafka, Apache Flink, and CrateDB: https://github.com/crate/cratedb-examples/tree/main/stacks/kafka-flink
.. _Basic example for connecting to CrateDB and CrateDB Cloud using JDBC: https://github.com/crate/cratedb-examples/tree/main/by-language/java-jdbc
.. _Build a data ingestion pipeline using Kafka, Flink, and CrateDB: https://dev.to/crate/build-a-data-ingestion-pipeline-using-kafka-flink-and-cratedb-1h5o
.. _CrateDB: https://crate.io/products/cratedb/
.. _CrateDB source: https://github.com/crate/crate
.. _Create an issue: https://github.com/crate/crate-jdbc/issues
.. _Dataiku: https://www.dataiku.com/
.. _development sandbox: https://github.com/crate/crate-jdbc/blob/master/DEVELOP.rst
.. _Flink example jobs for CrateDB: https://github.com/crate/cratedb-flink-jobs
.. _hosted on GitHub: https://github.com/crate/crate-jdbc/
.. _hstore: https://www.postgresql.org/docs/current/hstore.html
.. _JDBC: https://en.wikipedia.org/wiki/Java_Database_Connectivity
.. _JDBC 4.1 specification: https://download.oracle.com/otn-pub/jcp/jdbc-4_1-mrel-spec/jdbc4.1-fr-spec.pdf
.. _JDBC API documentation: https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/
.. _JDBC tutorial: https://docs.oracle.com/javase/tutorial/jdbc/basics/
.. _JDBC Type 4 driver: https://en.wikipedia.org/wiki/JDBC_driver#Type_4_driver_%E2%80%93_Database-Protocol_driver/Thin_Driver(Pure_Java_driver)
.. _jsonb: https://www.postgresql.org/docs/current/datatype-json.html
.. _LICENSE: https://github.com/crate/crate-jdbc/blob/master/LICENSE
.. _pgjdbc driver fork: https://github.com/crate/pgjdbc
.. _PostgreSQL JDBC Driver: https://github.com/pgjdbc/pgjdbc
.. _PostgreSQL Wire Protocol: https://www.postgresql.org/docs/current/protocol.html
.. _sample application: https://github.com/crate/crate-sample-apps/tree/main/java-spring
.. _sample application documentation: https://github.com/crate/crate-sample-apps/blob/main/java-spring/documentation.md
.. _Spring Data JDBC: https://spring.io/projects/spring-data-jdbc/
