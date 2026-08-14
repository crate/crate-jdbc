.. _migration:

##################
Migrating to 3.0.x
##################

For most applications the upgrade from 2.7.0 is a version bump: connection
URLs, ``OBJECT`` values as ``java.util.Map``, CrateDB type names in
``createArrayOf()`` and the no-op ``rollback()`` all keep working. Everything
below is what does not.


************
Requirements
************

3.0.x requires **Java 11 or later** (2.7.0 ran on Java 8) and **CrateDB 6.0
or later**. For older servers or an older JRE, stay on crate-jdbc 2.7.0.


**********
Timestamps
**********

2.7.0 bound ``setTimestamp()`` values as epoch milliseconds and read
``timestamp`` columns back epoch-faithfully regardless of the JVM time zone.
3.0.x follows the JDBC specification: a ``TIMESTAMP`` column without a time
zone holds wall-clock time in the JVM's default zone. An application that
stores ``timestamp`` rather than ``timestamptz`` columns and runs in a
non-UTC JVM reads shifted values. Use ``timestamptz`` columns, pass a
``Calendar`` to ``getTimestamp()``/``setTimestamp()``, or run the JVM with
``-Duser.timezone=UTC``.

``createArrayOf("timestamp", ...)`` builds an array of ``timestamp without
time zone``, which is what a CrateDB ``timestamp`` column is; 2.7.0 built an
array of ``timestamp with time zone``. Scalar and array timestamps therefore
agree on time-zone handling. Use ``createArrayOf("timestamptz", ...)`` for a
``timestamptz`` column.


*************
Update counts
*************

CrateDB reports an unknown row count as −1 on the wire, as it does for a
``DELETE`` over partitions. 2.7.0 surfaced this as −2
(``Statement.SUCCESS_NO_INFO``); 3.0.x reports 0. Code that checks update
counts after such statements has to accept 0.


***********************
Reported SQL type codes
***********************

Type codes are read off the wire instead of from a table of the driver's own.
Boolean array elements report ``Types.BIT`` where 2.7.0 reported
``Types.BOOLEAN``, ``OBJECT`` arrays report ``Types.OTHER`` where it reported
``Types.JAVA_OBJECT``, and a ``byte`` column reports the type CrateDB sends it
as (``Types.CHAR``, or ``Types.SMALLINT`` from CrateDB 6.5 on) where it
reported ``Types.TINYINT``.

The value a column's typed getter reads is unchanged, so ``getByte()`` on a
``byte`` column keeps returning the byte, whatever the server. Untyped reads
follow the wire type: ``getObject()`` on a ``byte`` column returns a
``String`` before CrateDB 6.5 and an ``Integer`` from 6.5 on, where 2.7.0
always returned an ``Integer``. Read such a column with ``getByte()``.


*******************
Strict mode removed
*******************

The ``strict`` connection property is gone, together with the
``SQLFeatureNotSupportedException``\ s the driver raised for transactional API
calls. ``setAutoCommit(false)``, ``commit()``, ``setReadOnly()`` and every
transaction isolation level JDBC defines are accepted, ``rollback()`` undoes
nothing, and ``prepareCall()`` works.

``rollback()`` does still raise ``SQLException`` in the two states the JDBC
specification forbids it in, on a closed connection and while auto-commit is
enabled, so a mis-sequenced call is not silently swallowed. Savepoints
raise ``SQLFeatureNotSupportedException``: CrateDB has no savepoint
statements, so the alternative is a syntax error from the server.

``DatabaseMetaData`` now describes that database rather than the PostgreSQL
release it emulates, so a framework that asks before relying on
transactional bookkeeping handles CrateDB as non-transactional. See
:ref:`limitations` for what else it answers differently.


********************
TLS is on by default
********************

Leaving ``ssl`` and ``sslmode`` unset no longer means "no TLS": the default
is ``sslmode=prefer``, so the driver asks for an encrypted connection and
falls back to an unencrypted one. See :ref:`connection_properties` for what
each mode checks.


************************
Dependencies are visible
************************

The ``crate-jdbc`` artifact declares pgJDBC and jackson-databind as ordinary
dependencies instead of bundling them, which puts both in reach of dependency
and vulnerability scanners. A build resolving from a mirror or an air-gapped
repository therefore needs both available, and a project that also depends on
pgJDBC directly resolves one version through its build tool's usual rules.

With that, its classes live under ``org.postgresql``, so code importing
``io.crate.shade.org.postgresql.*`` types must either import the plain names
or switch to ``crate-jdbc-standalone``, which keeps the relocation.
Configuration values naming shaded classes follow the same rule — for
example ``sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory`` with
``crate-jdbc``, and the ``io.crate.shade``-prefixed name with
``crate-jdbc-standalone``.
