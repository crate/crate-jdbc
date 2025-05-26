.. _index:

###################
CrateDB JDBC Driver
###################

************
Introduction
************

A `JDBC`_ driver for `CrateDB`_, based on the `PostgreSQL JDBC Driver`_ which is adhering to the `JDBC 4.1 specification`_. It is written in pure Java, and communicates with the database using the `PostgreSQL Wire Protocol`_.

.. _synopsis:

********
Synopsis
********

Connect to CrateDB instance running on ``localhost``:

.. code-block:: java

    import java.sql.Connection;
    import java.sql.DriverManager;

    Connection conn = DriverManager.getConnection("jdbc:crate://localhost:5432/");

Connect to CrateDB Cloud:

.. code-block:: java

    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.util.Properties;

    Properties connectionProps = new Properties();
    connectionProps.put("user", "admin");
    connectionProps.put("password", "<PASSWORD>");
    connectionProps.put("tcpKeepAlive", true);

    Connection conn = DriverManager.getConnection("jdbc:crate://example.aks1.westeurope.azure.cratedb.net:5432/?user=crate", connectionProps);

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

*************
Documentation
*************

For general help about `JDBC`_, please consult the `JDBC tutorial`_ and the `JDBC API documentation`_.

.. toctree::
    :titlesonly:

    getting-started
    connect
    data-types
    internals


.. SEEALSO::

    The CrateDB JDBC driver is an open source project and is `hosted on
    GitHub`_. Every kind of contribution, feedback, or patch, is much
    welcome!


.. _Apache Kafka, Apache Flink, and CrateDB: https://github.com/crate/cratedb-examples/tree/main/framework/flink
.. _Basic example for connecting to CrateDB and CrateDB Cloud using JDBC: https://github.com/crate/cratedb-examples/tree/main/by-language/java-jdbc
.. _Build a data ingestion pipeline using Kafka, Flink, and CrateDB: https://dev.to/crate/build-a-data-ingestion-pipeline-using-kafka-flink-and-cratedb-1h5o
.. _CrateDB: https://crate.io/products/cratedb/
.. _CrateDB source: https://github.com/crate/crate
.. _Flink example jobs for CrateDB: https://github.com/crate/cratedb-flink-jobs
.. _hosted on GitHub: https://github.com/crate/crate-jdbc/
.. _JDBC: https://en.wikipedia.org/wiki/Java_Database_Connectivity
.. _JDBC 4.1 specification: https://download.oracle.com/otn-pub/jcp/jdbc-4_1-mrel-spec/jdbc4.1-fr-spec.pdf
.. _JDBC API documentation: https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/
.. _JDBC tutorial: https://docs.oracle.com/javase/tutorial/jdbc/basics/
.. _PostgreSQL JDBC Driver: https://github.com/pgjdbc/pgjdbc
.. _PostgreSQL Wire Protocol: https://www.postgresql.org/docs/current/protocol.html
.. _sample application: https://github.com/crate/crate-sample-apps/tree/main/java-spring
.. _sample application documentation: https://github.com/crate/crate-sample-apps/blob/main/java-spring/documentation.md
.. _Spring Data JDBC: https://spring.io/projects/spring-data-jdbc/
