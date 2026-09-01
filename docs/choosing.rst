.. _choosing:

######################
Choosing a JDBC driver
######################

CrateDB speaks the `PostgreSQL Wire Protocol`_, so Java applications have two
working options: the official `PostgreSQL JDBC Driver`_ (pgJDBC), and this
driver, a thin adaptation layer on top of it. Both require **CrateDB 6.0 or
later** for full metadata support.


********************
Stock pgJDBC is fine
********************

For plain SQL the vanilla driver works against CrateDB unchanged: queries,
inserts, updates, prepared statements, batches, cursor-based fetching,
connection pools.

.. code-block:: java

    Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/doc?user=crate");

Two things to know when using it directly:

- ``Connection.rollback()`` reaches the server, and CrateDB has no
  ``ROLLBACK`` statement, so it raises an error. Frameworks and pools that
  roll back during cleanup need ``autoCommit`` left on, or a dialect that
  skips the call.
- CrateDB ``OBJECT`` columns arrive as json strings, and a
  ``java.util.Map`` parameter is bound as a PostgreSQL ``hstore`` value,
  which CrateDB does not provide.


****************************
What this driver adds on top
****************************

Use ``crate-jdbc`` when the application works with CrateDB's own types or
runs inside a framework that expects transactional bookkeeping:

- ``OBJECT`` columns read back as ``Map<String, Object>``, and ``Map``
  parameters bind to them, including arrays of ``OBJECT``.
- ``createArrayOf()`` accepts CrateDB type names (``string``, ``long``,
  ``object``, ``geo_point``, ``float_vector``) next to the PostgreSQL ones.
- ``rollback()`` is a client-side no-op, and savepoints are reported as
  unsupported instead of failing at the server.
- Metadata calls tolerate an empty-string catalog argument, which several
  tools pass.
- ``DatabaseMetaData.getDatabaseProductName()`` reports ``Crate``, so tools
  that pick an SQL dialect by product name do not fall back to
  PostgreSQL-specific SQL.

Everything else is pgJDBC, so anything documented there applies here too: wire
protocol, authentication, TLS, pooling behavior, cursors, the metadata API.
Switching between the two is a change of URL scheme, ``jdbc:postgresql://``
against ``jdbc:crate://``. What neither of them can do is listed under
:ref:`limitations`.


.. _PostgreSQL JDBC Driver: https://jdbc.postgresql.org/
.. _PostgreSQL Wire Protocol: https://cratedb.com/docs/crate/reference/en/latest/interfaces/postgres.html
