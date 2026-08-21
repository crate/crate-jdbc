===================
CrateDB JDBC driver
===================

|tests| |docs| |rtd| |maven-central|

|

A `JDBC`_ driver for `CrateDB`_, built on top of the official `PostgreSQL JDBC
Driver`_ (pgJDBC).

This is a `type 4 JDBC driver`_ written in pure Java. It communicates with the
database using the `PostgreSQL Wire Protocol`_ and adds a thin adaptation layer
for the four behaviors where CrateDB differs from PostgreSQL: ``OBJECT`` columns
as ``java.util.Map``, CrateDB type names in ``createArrayOf()``, a
``rollback()`` that stays on the client, CrateDB having no ``ROLLBACK``
statement, and ``Crate`` as the reported database product name.

Requirements
============

- Java 11 or later
- CrateDB 6.0 or later

For older servers or an older JRE, use crate-jdbc 2.7.0.

Quick start
===========

.. code-block:: xml

    <dependency>
      <groupId>io.crate</groupId>
      <artifactId>crate-jdbc</artifactId>
      <version>3.0.0</version>
    </dependency>

.. code-block:: java

    Connection conn = DriverManager.getConnection(
        "jdbc:crate://localhost:5432/doc?user=crate");

    ResultSet rs = conn.createStatement().executeQuery(
        "SELECT mountain, height FROM sys.summits ORDER BY height DESC LIMIT 3");
    while (rs.next()) {
        System.out.println(rs.getString("mountain") + " " + rs.getInt("height"));
    }

CrateDB's ``OBJECT`` columns read as ``java.util.Map``, and ``float_vector``
columns support `vector search`_ with the query vector bound as an ordinary
parameter:

.. code-block:: java

    PreparedStatement search = conn.prepareStatement(
        "SELECT id FROM embeddings WHERE knn_match(embedding, ?, 10)");
    search.setArray(1, conn.createArrayOf("float_vector", new Float[]{0.1f, 0.2f, 0.3f}));
    ResultSet matches = search.executeQuery();

Which artifact
==============

+--------------------------+------------------------------------------------+
| Artifact                 | Use it when                                    |
+==========================+================================================+
| `crate-jdbc`_            | The application resolves its dependencies      |
|                          | through Maven or Gradle. pgJDBC and jackson    |
|                          | come along as ordinary dependencies, visible   |
|                          | to vulnerability scanners.                     |
+--------------------------+------------------------------------------------+
| `crate-jdbc-standalone`_ | A single jar is required: the driver           |
|                          | directory of a tool such as Apache Hop,        |
|                          | Pentaho, DBeaver or SQuirreL. Everything it    |
|                          | bundles is relocated under ``io.crate.shade``  |
|                          | so it cannot clash with another pgJDBC.        |
+--------------------------+------------------------------------------------+

Stock pgJDBC also works against CrateDB. `Choosing a JDBC driver`_ compares
the two.

Documentation and help
======================

- `CrateDB JDBC driver documentation`_
- `Limitations`_
- `CrateDB reference documentation`_
- `JDBC tutorial`_
- `JDBC API documentation`_
- `Developer guide`_
- `Contributing`_
- Other `support channels`_

Contributing
============

The CrateDB JDBC driver library is an open source project, and is `managed on
GitHub`_. We appreciate contributions of any kind.

License
=======

Licensed under the Apache License, Version 2.0. See `LICENSE`_ and `NOTICE`_.


.. _Choosing a JDBC driver: https://cratedb.com/docs/jdbc/en/latest/choosing.html
.. _Contributing: CONTRIBUTING.rst
.. _crate-jdbc: https://central.sonatype.com/artifact/io.crate/crate-jdbc
.. _crate-jdbc-standalone: https://central.sonatype.com/artifact/io.crate/crate-jdbc-standalone
.. _CrateDB: https://github.com/crate/crate
.. _CrateDB JDBC driver documentation: https://cratedb.com/docs/jdbc/en/latest/
.. _CrateDB reference documentation: https://cratedb.com/docs/crate/reference/en/latest/
.. _Developer guide: DEVELOP.rst
.. _JDBC: https://en.wikipedia.org/wiki/Java_Database_Connectivity
.. _JDBC API documentation: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/package-summary.html
.. _JDBC tutorial: https://docs.oracle.com/javase/tutorial/jdbc/basics/
.. _LICENSE: LICENSE
.. _Limitations: https://cratedb.com/docs/jdbc/en/latest/limitations.html
.. _managed on GitHub: https://github.com/crate/crate-jdbc
.. _NOTICE: NOTICE
.. _PostgreSQL JDBC Driver: https://jdbc.postgresql.org/
.. _PostgreSQL Wire Protocol: https://cratedb.com/docs/crate/reference/en/latest/interfaces/postgres.html
.. _support channels: https://cratedb.com/support/
.. _type 4 JDBC driver: https://en.wikipedia.org/wiki/JDBC_driver#Type_4_driver_.E2.80.93_Database-Protocol_driver_.28Pure_Java_driver.29
.. _vector search: https://cratedb.com/docs/jdbc/en/latest/data-types.html#vector-search



.. |tests| image:: https://github.com/crate/crate-jdbc/actions/workflows/tests.yml/badge.svg?branch=master
    :alt: Build status
    :target: https://github.com/crate/crate-jdbc/actions/workflows/tests.yml?query=branch%3Amaster

.. |docs| image:: https://github.com/crate/crate-jdbc/actions/workflows/docs.yml/badge.svg
    :alt: Documentation: Link checker
    :target: https://github.com/crate/crate-jdbc/actions/workflows/docs.yml

.. |rtd| image:: https://readthedocs.org/projects/crate-jdbc/badge/
    :alt: Read the Docs status
    :target: https://readthedocs.org/projects/crate-jdbc/

.. |maven-central| image:: https://img.shields.io/maven-central/v/io.crate/crate-jdbc
    :alt: Latest release on Maven Central
    :target: https://central.sonatype.com/artifact/io.crate/crate-jdbc
