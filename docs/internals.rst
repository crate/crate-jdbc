.. _details:
.. _internals:

#########
Internals
#########

The CrateDB JDBC driver is a thin adaptation layer on top of the official
`PostgreSQL JDBC Driver`_ (pgJDBC), which it declares as a regular
dependency. All wire communication, connection handling, authentication,
and SSL support is stock pgJDBC; the CrateDB layer adjusts only the
behaviors where CrateDB differs from PostgreSQL.


************
Architecture
************

``io.crate.client.jdbc.CrateDriver`` registers with the
``DriverManager`` for the ``crate://`` and ``jdbc:crate://`` URL schemes,
rewrites them to ``jdbc:postgresql://``, and delegates the connection to
pgJDBC. The resulting connection is wrapped in delegating classes that
override a small set of methods:

:``CrateConnection``:

    - ``rollback()`` is a client-side no-op. CrateDB has no transactions:
      it accepts ``BEGIN`` and ``COMMIT`` as no-ops, but ``ROLLBACK`` is
      not part of its SQL grammar, so forwarding it would raise a server
      error in every framework that calls ``rollback()`` during routine
      cleanup. ``rollback(Savepoint)`` raises
      ``SQLFeatureNotSupportedException``.
    - ``createArrayOf()`` accepts CrateDB type names (``string``,
      ``long``, ``short``, ``byte``, ``float``, ``double``, ``ip``,
      ``timestamp``, ``object``, ...) in addition to the PostgreSQL names
      pgJDBC resolves against the server.

:``CratePreparedStatement`` / ``CrateResultSet`` / ``CrateArray``:

    CrateDB ``OBJECT`` values travel as json over the wire.
    ``setObject()`` accepts a ``java.util.Map`` and binds it as json;
    ``getObject()`` on an ``OBJECT`` column returns a
    ``Map<String, Object>``; arrays of ``OBJECT`` yield arrays of maps.

:``CrateDatabaseMetaData``:

    - ``getDatabaseProductName()`` reports ``Crate``, so tools that pick
      an SQL dialect by product name (DataGrip, DBeaver, the
      `Apache Flink JDBC Connector`_) do not fall back to
      PostgreSQL-specific SQL.
    - An empty-string catalog argument is treated like ``null``. CrateDB
      has a single catalog named ``crate``; passing ``""`` to metadata
      methods would otherwise filter every result out.

:``CrateDriver``:

    Connection defaults suited to CrateDB clusters are applied when not
    set by the caller: ``loadBalanceHosts=true`` and
    ``assumeMinServerVersion=9.5``.

Everything else — including ``DataSource`` implementations,
``CallableStatement``, cursor-based fetching with ``setFetchSize()``
under manual commit mode, and the complete metadata API — is stock
pgJDBC behavior and works to the extent that CrateDB's PostgreSQL
compatibility supports it.


*********
Artifacts
*********

:``crate-jdbc``:

    The regular Maven dependency. pgJDBC and jackson-databind appear as
    ordinary transitive dependencies, visible to dependency and
    vulnerability scanners, so security updates arrive by bumping one
    version.

:``crate-jdbc-standalone``:

    A self-contained jar for dropping into the driver directory of tools
    like `Apache Hop`_, Pentaho, DBeaver, or SQuirreL. Bundled
    dependencies are relocated under ``io.crate.shade`` so the jar cannot
    clash with another pgJDBC or jackson on the same classpath; the
    bundled pgJDBC version is declared in the jar manifest as
    ``Bundled-PgJdbc-Version``. The bundled pgJDBC does not register
    itself with the ``DriverManager``: ``jdbc:postgresql://`` URLs remain
    the province of a real PostgreSQL driver.


**********************
Supported server range
**********************

The driver requires **CrateDB 6.0 or later**: pgJDBC's metadata queries
rely on server support (for example ``current_catalog``) that CrateDB
gained in the 6.x line. For older CrateDB versions, use crate-jdbc
2.7.0, the last release of the former fork-based driver.


.. _Apache Flink JDBC Connector: https://github.com/apache/flink-connector-jdbc
.. _Apache Hop: https://hop.apache.org/
.. _PostgreSQL JDBC Driver: https://jdbc.postgresql.org/
