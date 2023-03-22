.. _compatibility:
.. _compatibility-notes:

===================
Compatibility notes
===================

.. rubric:: Table of contents

.. contents::
   :local:

.. _versions:

Version notes
=============

.. _cratedb-versions:

CrateDB
-------

Consult the following table for CrateDB version compatibility notes:

+----------------+-----------------+-------------------------------------------+
| Driver Version | CrateDB Version | Notes                                     |
+================+=================+===========================================+
| 1.x            | < 0.38          | Not supported.                            |
+----------------+-----------------+-------------------------------------------+
| 1.x            | >= 0.38         | Supported.                                |
|                |                 |                                           |
|                |                 | Uses the (legacy, now removed) binary     |
|                |                 | transport protocol, with a default port   |
|                |                 | of 4300.                                  |
+----------------+-----------------+-------------------------------------------+
| Any            | < 0.55          | Default schema selection is not           |
|                |                 | supported.                                |
+----------------+-----------------+-------------------------------------------+
| 1.x            | >= 0.55         | Not supported.                            |
+----------------+-----------------+-------------------------------------------+
| 2.x            | 0.56.x          | Experimental support.                     |
|                |                 |                                           |
|                |                 | The CrateDB ``psql.enabled`` setting must |
|                |                 | be set to ``true``.                       |
|                |                 |                                           |
|                |                 | For the JDBC driver, the                  |
|                |                 | ``prepareThreshold`` connection property  |
|                |                 | must be set to ``0``.                     |
+----------------+-----------------+-------------------------------------------+
| 2.x            | >= 0.57.x       | Supported.                                |
|                |                 |                                           |
|                |                 | The `connection string`_ changed to the   |
|                |                 | same format used by `the PostgreSQL JDBC  |
|                |                 | driver`_.                                 |
|                |                 |                                           |
|                |                 | This means that setting the default       |
|                |                 | schema with the connection string is not  |
|                |                 | supported. Use `setSchema()`_ instead, or |
|                |                 | provide a schema name in the SQL          |
|                |                 | statement.                                |
|                |                 |                                           |
|                |                 | Also, support for the experimental        |
|                |                 | ``showsubcolumns`` parameter has been     |
|                |                 | dropped.                                  |
+----------------+-----------------+-------------------------------------------+
| Any            | >= 2.1.x        | Client needs to connect with a valid      |
|                |                 | database user to access CrateDB.          |
|                |                 |                                           |
|                |                 | The default CrateDB user is ``crate`` and |
|                |                 | has no password set.                      |
+----------------+-----------------+-------------------------------------------+

.. _implementations:

Implementation notes
====================

.. _jdbc-implementation:

JDBC
----

The CrateDB JDBC driver follows the JDBC 4.1 standard.

However, the following notes apply:

- `ParameterMetaData`_ (e.g. as returned by `PreparedStatement`_) is not
  supported.
- `DataSource`_ is not supported.
- `CallableStatement`_ is not supported, as CrateDB itself does not support
  stored procedures.
- `ResultSet`_ objects are read only (``TYPE_FORWARD_ONLY``, ``CONCUR_READ_ONLY``),
  so changes to a ``ResultSet`` are not supported.

.. _ParameterMetaData: https://docs.oracle.com/javase/8/docs/api/java/sql/ParameterMetaData.html
.. _PreparedStatement: https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html
.. _DataSource: https://docs.oracle.com/javase/8/docs/api/javax/sql/DataSource.html
.. _CallableStatement: https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
.. _ResultSet: https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html
.. _setSchema(): https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html#setSchema-java.lang.String-
.. _connection string: https://jdbc.postgresql.org/documentation/use/#connecting-to-the-database
.. _the PostgreSQL JDBC driver: https://jdbc.postgresql.org/
