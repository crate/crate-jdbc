.. _internals:

#########
Internals
#########


.. _implementations:
.. _jdbc-implementation:

What's inside
=============

Please take notice of the corresponding implementation notes:

- `CallableStatement`_ is not supported, as CrateDB itself does not support
  stored procedures.
- `DataSource`_ is not supported.
- `ParameterMetaData`_, e.g. as returned by `PreparedStatement`_, is not
  supported.
- `ResultSet`_ objects are read only (``TYPE_FORWARD_ONLY``, ``CONCUR_READ_ONLY``),
  so changes to a ``ResultSet`` are not supported.
- DDL and DML statements are supported through adjustments to the
  `PgPreparedStatement`_ and `PgStatement`_ interfaces.

To learn further details about the compatibility with JDBC and PostgreSQL
features, see the specific code changes to the `PgConnection`_,
`PgDatabaseMetaData`_, and `PgResultSet`_ classes.


.. _CallableStatement: https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
.. _DataSource: https://docs.oracle.com/javase/8/docs/api/javax/sql/DataSource.html
.. _ParameterMetaData: https://docs.oracle.com/javase/8/docs/api/java/sql/ParameterMetaData.html
.. _PgConnection: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-8ee30bec696495ec5763a3e1c1b216776efc124729f72e18dbaa35064af0aef0
.. _PgDatabaseMetaData: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-0571f8ac3385a7f7bb34e5c77f8afd24810311506989379c2e85c6c16eea6ce4
.. _PgResultSet: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-7e93771092eab9084402e3c7c81319a1f037febdc7614264329bd29f11d39ef2
.. _PgPreparedStatement: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-d4946409bd7c59e525f34b4c974a3df76638dc84adc060cc5d13d5409c6aeb21
.. _PgStatement: https://github.com/pgjdbc/pgjdbc/compare/REL42.2.5...crate:pgjdbc:REL42.2.5_crate?expand=1#diff-2abcc60e1b1ef8eeadd6372bf7afd0c0ebae0ebd691b0965fc914fea794eb6d0
.. _PreparedStatement: https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html
.. _ResultSet: https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html
