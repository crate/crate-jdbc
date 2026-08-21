.. _index:

###################
CrateDB JDBC Driver
###################

************
Introduction
************

A `JDBC`_ driver for `CrateDB`_, built on the official `PostgreSQL JDBC
Driver`_ (JDBC 4.2). It is written in pure Java, speaks the `PostgreSQL Wire
Protocol`_, and adds a thin adaptation layer for the behaviors where CrateDB
differs from PostgreSQL, described under :ref:`internals`. It requires Java 11
and CrateDB 6.0 or later.

.. _synopsis:

********
Synopsis
********

Connect to a CrateDB instance running on ``localhost``:

.. code-block:: java

    import java.sql.Connection;
    import java.sql.DriverManager;

    Connection conn = DriverManager.getConnection(
        "jdbc:crate://localhost:5432/doc?user=crate");

Connect to CrateDB Cloud. Connection properties are strings, including the
ones that stand for a boolean:

.. code-block:: java

    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.util.Properties;

    Properties connectionProps = new Properties();
    connectionProps.put("user", "admin");
    connectionProps.put("password", "<PASSWORD>");
    connectionProps.put("sslmode", "require");
    connectionProps.put("tcpKeepAlive", "true");

    Connection conn = DriverManager.getConnection(
        "jdbc:crate://example.aks1.westeurope.azure.cratedb.net:5432/doc", connectionProps);

.. _examples:

Examples
========

- A `basic example for connecting to CrateDB and CrateDB Cloud using JDBC`_,
  written against both the vanilla pgJDBC driver and this one.
- A "guestbook" `sample application`_ built on `Spring Data JDBC`_, with its own
  `sample application documentation`_.
- `Build a data ingestion pipeline using Kafka, Flink, and CrateDB`_, with the
  repositories `Apache Kafka, Apache Flink, and CrateDB`_ and `Flink example
  jobs for CrateDB`_.

*************
Documentation
*************

The `JDBC tutorial`_ and the `JDBC API documentation`_ cover the API itself.

.. toctree::
    :titlesonly:

    getting-started
    connect
    data-types
    internals


.. SEEALSO::

    The CrateDB JDBC driver is an open source project, `hosted on GitHub`_.
    Contributions, feedback and patches are welcome.


.. _Apache Kafka, Apache Flink, and CrateDB: https://github.com/crate/cratedb-examples/tree/main/framework/flink
.. _basic example for connecting to CrateDB and CrateDB Cloud using JDBC: https://github.com/crate/cratedb-examples/tree/main/by-language/java-jdbc
.. _Build a data ingestion pipeline using Kafka, Flink, and CrateDB: https://dev.to/crate/build-a-data-ingestion-pipeline-using-kafka-flink-and-cratedb-1h5o
.. _CrateDB: https://cratedb.com/database
.. _CrateDB source: https://github.com/crate/crate
.. _Flink example jobs for CrateDB: https://github.com/crate/cratedb-flink-jobs
.. _hosted on GitHub: https://github.com/crate/crate-jdbc/
.. _JDBC: https://en.wikipedia.org/wiki/Java_Database_Connectivity
.. _JDBC API documentation: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/package-summary.html
.. _JDBC tutorial: https://docs.oracle.com/javase/tutorial/jdbc/basics/
.. _PostgreSQL JDBC Driver: https://jdbc.postgresql.org/
.. _PostgreSQL Wire Protocol: https://cratedb.com/docs/crate/reference/en/latest/interfaces/postgres.html
.. _sample application: https://github.com/crate/crate-sample-apps/tree/main/java-spring
.. _sample application documentation: https://github.com/crate/crate-sample-apps/blob/main/java-spring/documentation.md
.. _Spring Data JDBC: https://spring.io/projects/spring-data-jdbc/
