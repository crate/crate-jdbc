.. _migration:

##################
Migrating to 3.0.x
##################

Version 3.0.0 replaces the driver's foundation: instead of bundling a
patched copy of pgJDBC 42.2.5, the driver builds on the current official
`PostgreSQL JDBC Driver`_ and adapts CrateDB-specific behavior in a thin
wrapper layer (see :ref:`internals`). For most applications the upgrade
is a version bump — connection URLs, ``OBJECT`` values as
``java.util.Map``, CrateDB type names in ``createArrayOf()``, and the
no-op ``rollback()`` all keep working. The differences that can require
attention are listed here.


*******************
Server requirements
*******************

3.0.x requires **CrateDB 6.0 or later**. For older servers, stay on
crate-jdbc 2.7.0.


****************
Behavior changes
****************

Timestamps
==========

2.7.0 bound ``setTimestamp()`` values as epoch milliseconds and read
``timestamp`` columns back epoch-faithfully regardless of the JVM time
zone. 3.0.x follows the JDBC specification: values of ``TIMESTAMP``
(without time zone) columns are interpreted as wall-clock time in the
JVM's default time zone. If your application stores ``timestamp``
(rather than ``timestamptz``) columns and runs in a non-UTC JVM, read
values shift accordingly. Use ``timestamptz`` columns, pass an explicit
``Calendar`` to ``getTimestamp()``/``setTimestamp()``, or run the JVM
with ``-Duser.timezone=UTC`` for the previous behavior.

Update counts
=============

CrateDB reports an unknown row count (for example for ``DELETE`` on
partitions) as −1 on the wire. 2.7.0 surfaced this as −2
(``Statement.SUCCESS_NO_INFO``); 3.0.x reports 0, like pgJDBC does.
Code that verifies update counts after such statements needs to accept
0.

Reported SQL type codes
=======================

Type codes now come from pgJDBC: boolean array elements report
``Types.BIT`` instead of ``Types.BOOLEAN``, ``byte`` columns report
``Types.CHAR`` instead of ``Types.TINYINT``, and ``OBJECT`` arrays
report ``Types.OTHER`` instead of ``Types.JAVA_OBJECT``. The Java
values themselves are unaffected.

Strict mode removed
===================

The ``strict`` connection property is gone, together with the
``SQLFeatureNotSupportedException``\ s the driver used to raise for
transactional API calls. ``setAutoCommit(false)`` and ``commit()`` are
always allowed (the server treats transaction statements as no-ops),
``rollback()`` is a client-side no-op, and ``prepareCall()`` works.

Metadata
========

- ``getTables()`` with a ``null`` types argument also returns index
  rows; pass an explicit types array such as ``{"TABLE", "VIEW"}`` to
  filter.
- ``getColumns()`` lists nested object columns (for example
  ``settings['udc']['enabled']``) alongside top-level columns, matching
  CrateDB's own ``information_schema.columns``.
- ``getPseudoColumns()`` raises pgJDBC's not-implemented exception
  instead of returning an empty result.


*******************************
Shaded class names (standalone)
*******************************

The ``crate-jdbc`` Maven artifact no longer relocates pgJDBC: classes
live under ``org.postgresql`` again, and code that imported
``io.crate.shade.org.postgresql.*`` types must either import the plain
``org.postgresql.*`` names or switch to the ``crate-jdbc-standalone``
artifact, which keeps the ``io.crate.shade`` relocation. Configuration
values naming shaded classes (for example
``sslfactory=io.crate.shade.org.postgresql.ssl.DefaultJavaSSLFactory``)
follow the same rule: plain names with ``crate-jdbc``, shaded names with
``crate-jdbc-standalone``.


****************************
Coming from vanilla pgJDBC
****************************

Applications using plain pgJDBC against CrateDB can switch to this
driver by changing the URL scheme from ``jdbc:postgresql://`` to
``jdbc:crate://``. This adds: ``Map`` binding and reading for ``OBJECT``
columns, CrateDB type names in ``createArrayOf()``, a ``rollback()``
that does not raise a server error, tolerance for empty-string catalog
arguments in metadata calls, and ``Crate`` as the reported product name
for dialect-sniffing tools.


.. _PostgreSQL JDBC Driver: https://jdbc.postgresql.org/
