.. _details:
.. _internals:

#########
Internals
#########

pgJDBC employs a few behaviours that strictly expect a PostgreSQL
server on the other end, so that some operations will fail on databases
offering wire-compatibility with PostgreSQL, but do not provide certain
features like the `hstore`_ or `jsonb`_ extensions.

The background is that, when using the ``jdbc:postgresql://`` JDBC driver
prefix, downstream applications and frameworks will implicitly select
the corresponding dialect implementation for PostgreSQL.


*************
Compatibility
*************

While CrateDB is compatible with PostgreSQL on the wire protocol, it needs
JDBC support for its SQL dialect, provided by the ``jdbc:crate://`` protocol
identifier.

- IDEs like `DataGrip`_ and `DBeaver`_ need this driver, because they would
  otherwise emit SQL statements too specific to PostgreSQL.

- The `Apache Flink JDBC Connector`_ needs this driver to not select the
  PostgreSQL dialect, see also `Apache Kafka, Apache Flink, and CrateDB`_.

- Tools like `Dataiku`_ need this driver to implement transaction commands
  like ``ROLLBACK`` as a no-op.

.. note::

    For basic and general purpose use, the official `PostgreSQL JDBC Driver`_
    can also be used. Sometimes, it is easier because pgJDBC is included
    into many applications out of the box.


.. _differences:
.. _implementations:
.. _jdbc-implementation:

*********************
Differences to pgJDBC
*********************

The driver is based upon a fork of the `PostgreSQL JDBC Driver`_, see `pgjdbc
driver fork`_, and adjusts a few details to compensate for behavioral
differences of CrateDB.
Please take notice of the corresponding implementation notes:

:Supported:

    - A few metadata functions have been adjusted to better support CrateDB's type system.
    - The CrateDB JDBC driver deserializes objects to a Map, pgJDBC treats them as JSON.
    - DDL and DML statements are supported through adjustments to the
      `PgPreparedStatement`_ and `PgStatement`_ interfaces.

:Unsupported:

    - `CallableStatement`_ is not supported, as CrateDB itself does not support
      stored procedures.
    - `DataSource`_ is not supported.
    - `ParameterMetaData`_, e.g. as returned by `PreparedStatement`_, is not
      supported.
    - `ResultSet`_ objects are read only (``TYPE_FORWARD_ONLY``, ``CONCUR_READ_ONLY``),
      so changes to a ``ResultSet`` are not supported.

To learn further details about the compatibility with JDBC and PostgreSQL
features, see the specific code changes to the `PgConnection`_,
`PgDatabaseMetaData`_, and `PgResultSet`_ classes.



.. _Apache Flink JDBC Connector: https://github.com/apache/flink-connector-jdbc
.. _Apache Kafka, Apache Flink, and CrateDB: https://github.com/crate/cratedb-examples/tree/main/framework/flink
.. _CallableStatement: https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
.. _Dataiku: https://www.dataiku.com/
.. _DataGrip: https://www.jetbrains.com/datagrip/
.. _DataSource: https://docs.oracle.com/javase/8/docs/api/javax/sql/DataSource.html
.. _DBeaver: https://dbeaver.io/about/
.. _hstore: https://www.postgresql.org/docs/current/hstore.html
.. _jsonb: https://www.postgresql.org/docs/current/datatype-json.html
.. _ParameterMetaData: https://docs.oracle.com/javase/8/docs/api/java/sql/ParameterMetaData.html
.. _pgjdbc driver fork: https://github.com/crate/pgjdbc
.. _PostgreSQL JDBC Driver: https://jdbc.postgresql.org/
.. _PreparedStatement: https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html
.. _ResultSet: https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html


.. _PgConnection: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-8ee30bec696495ec5763a3e1c1b216776efc124729f72e18dbaa35064af0aef0
.. _PgDatabaseMetaData: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-0571f8ac3385a7f7bb34e5c77f8afd24810311506989379c2e85c6c16eea6ce4
.. _PgResultSet: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-7e93771092eab9084402e3c7c81319a1f037febdc7614264329bd29f11d39ef2
.. _PgPreparedStatement: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-d4946409bd7c59e525f34b4c974a3df76638dc84adc060cc5d13d5409c6aeb21
.. _PgStatement: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-2abcc60e1b1ef8eeadd6372bf7afd0c0ebae0ebd691b0965fc914fea794eb6d0
