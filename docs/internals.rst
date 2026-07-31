.. _details:
.. _internals:

#########
Internals
#########

The CrateDB JDBC driver is a thin adaptation layer over the official
`PostgreSQL JDBC Driver`_ (pgJDBC). All wire communication, connection
handling, authentication and TLS is pgJDBC's; the CrateDB layer adjusts only
the behaviors where CrateDB differs from PostgreSQL.


************
Architecture
************

``io.crate.client.jdbc.CrateDriver`` registers with the
``DriverManager`` for the ``crate://`` and ``jdbc:crate://`` URL schemes,
rewrites them to ``jdbc:postgresql://``, and delegates the connection to
pgJDBC. The resulting connection is wrapped in delegating classes that
override a small set of methods:

:``CrateConnection``:

    - ``rollback()`` undoes nothing. CrateDB has no transactions: it
      accepts ``BEGIN`` and ``COMMIT`` as no-ops, but ``ROLLBACK`` is not
      part of its SQL grammar, so forwarding it would raise a server error
      in every framework that calls ``rollback()`` during routine cleanup.
      What it does do is end the transaction block pgJDBC opens under
      manual commit mode, with the ``COMMIT`` that CrateDB parses and
      ignores — a connection whose block is left open refuses to change
      its read-only flag or its isolation level. It still raises
      ``SQLException`` on a closed connection and when auto-commit is on,
      the two states JDBC forbids it in.
    - ``TRANSACTION_NONE`` is the isolation level, the one
      ``DatabaseMetaData`` reports as supported. pgJDBC rejects it, having
      no PostgreSQL equivalent, so the connection answers it itself.
    - ``createArrayOf()`` accepts CrateDB type names (``string``,
      ``long``, ``short``, ``byte``, ``float``, ``double``, ``ip``,
      ``timestamp``, ``object``, ``geo_point``, ``geo_shape``,
      ``float_vector`` among them) next to the PostgreSQL names pgJDBC
      resolves against the server, in any case.
    - Savepoint methods raise ``SQLFeatureNotSupportedException``, which
      ``DatabaseMetaData.supportsSavepoints()`` announces.

:``CratePreparedStatement`` / ``CrateCallableStatement`` / ``CrateResultSet`` / ``CrateArray``:

    CrateDB ``OBJECT`` values travel as json over the wire.
    ``setObject()`` accepts a ``java.util.Map`` and binds it as json, and a
    collection of maps as an ``OBJECT`` array; ``getObject()`` on an
    ``OBJECT`` column returns a ``Map<String, Object>``, and
    ``getObject(column, type)`` reads it into any requested ``Map`` type;
    arrays of ``OBJECT`` yield arrays of maps, whether they are read with
    ``getArray()``, ``getObject()`` or ``Array.getResultSet()``.

    The metadata says the same: ``ResultSetMetaData.getColumnClassName()``
    and ``ParameterMetaData.getParameterClassName()`` name
    ``java.lang.Object`` for a json column or parameter — the class the
    ``Map`` and ``List`` forms have in common — rather than the pgJDBC
    ``PGobject`` an application neither receives from this driver nor has to
    build for it.

    Columns of ``array(array(...))`` travel as json too, since the
    PostgreSQL array format cannot hold sub-arrays of differing length.
    ``CrateJsonArray`` reads them as a ``java.sql.Array`` whose elements are
    arrays, and binds them back as the json the server types from the column
    they land in.

:``CrateDatabaseMetaData``:

    - ``getDatabaseProductName()`` reports ``Crate``, so a tool that picks
      an SQL dialect by product name does not take this for a PostgreSQL
      server and emit SQL CrateDB has no grammar for. ``getDriverName()``
      and ``getDriverVersion()`` likewise report this driver rather than
      the pgJDBC release underneath, and ``getURL()`` reports the URL in
      this driver's scheme.

      No framework picks a CrateDB dialect from this by itself, and none
      is expected to: the ones that dispatch on the URL (Flyway, jOOQ, the
      `Apache Flink JDBC Connector`_) see a ``jdbc:crate://`` scheme they
      have no entry for, and the ones that dispatch on the product name
      (Hibernate, Liquibase) see a name they have no entry for either.
      Configure the dialect explicitly. ``getDatabaseProductVersion()`` and
      the major and minor versions are deliberately left as pgJDBC reports
      them — the PostgreSQL release CrateDB emulates on the wire — because
      that is the version a dialect so configured reasons about. The
      CrateDB version is on ``CrateConnection.getCrateVersion()``.
    - An empty-string catalog argument is treated like ``null``. CrateDB
      has a single catalog named ``crate``; passing ``""`` to metadata
      methods would otherwise filter every result out.
    - Transactions and savepoints are reported as unsupported, and the
      default transaction isolation as ``TRANSACTION_NONE``, matching what
      the connection does with them. So are the SQL features CrateDB has no
      grammar for: foreign keys
      (``supportsIntegrityEnhancementFacility()``), ``SELECT ... FOR
      UPDATE`` and cursors returned from functions.
    - Identifier lengths are reported as unbounded. pgJDBC answers with
      PostgreSQL's 63-character limit, which CrateDB does not have, and a
      tool that shortens names to fit it would rename what it touches.
    - A call CrateDB's catalog cannot answer raises
      ``SQLFeatureNotSupportedException`` explaining why in CrateDB's terms,
      rather than reporting the PostgreSQL catalog object that pgJDBC's
      query happens to read.

:``CrateDriver``:

    CrateDB defaults are applied to the connection properties pgJDBC gives
    a PostgreSQL meaning, where the caller sets none: the schema a URL
    without a path segment connects to is ``doc`` rather than the user name
    pgJDBC would fill in, ``loadBalanceHosts`` is on, and
    ``assumeMinServerVersion`` is ``9.5``.

Every JDBC object the driver hands out is one of these wrappers, so
navigating from a result set to its statement and connection, or from
metadata rows and array rows, stays inside the driver. The wrappers
implement pgJDBC's own ``PGConnection`` and ``PGStatement`` interfaces, so
code that reaches for the pgJDBC API by cast or through ``unwrap()`` keeps
working. ``CrateDataSource`` provides the same connections to applications
configured with a ``DataSource`` rather than a URL.

Everything else — cursor-based fetching with ``setFetchSize()`` under
manual commit mode, authentication, SSL, and the rest of the metadata API —
is stock pgJDBC behavior and works to the extent that CrateDB's PostgreSQL
compatibility supports it.

``Statement.setQueryTimeout()`` is one place where that extent matters.
pgJDBC delivers a query timeout as a PostgreSQL cancel request on a second
connection, which carries no routing: it acts on the node holding the
session, while a second connection reaches whichever node a load balancer
picks. A request that does land on the node holding the session cancels it and
is answered with the silence the protocol prescribes; one that lands anywhere
else is forwarded over a transport action bound on no node, so the sending node
finds nothing to dispatch to and — binding being what instantiates the action —
the receiving node has no handler registered for it either. So the driver also
gives the server the timeout directly, as ``statement_timeout`` on the
connection already holding the session, and puts the session's own value back
as the execution ends. A statement that sets no timeout never touches the
setting.

Both mechanisms stay in play, because each covers a case the other does not.
Neither bounds a query CrateDB answers inline rather than handing to its
execution pool, which is what a query over ``sys`` tables is. The server arms
``statement_timeout`` once ``Plan.execute`` has returned, and an inline
execution is already complete by then, so the timer is never scheduled at all;
the thread that would read a cancel request is the one running the query. A
query over a table function ends on its timeout like any other, which puts the
difference in the execution path rather than in how long a query runs. What
CrateDB answers inline has to be bounded in its own text, with a ``LIMIT`` or a
narrower filter.

Such a query is also more than its own connection's problem. CrateDB runs the
PostgreSQL wire protocol, the inter-node transport and the channel that accepts
connections on one shared Netty event loop group, so a query occupying a loop
stops the node answering new connections at all, and configuring a second loop
does not divide that.

What is bounded is the execution, not the reading of its rows. A statement
with a fetch size leaves a cursor open under manual commit mode, and the
fetches that bring the remaining batches run after the setting has been given
back — so they carry no timeout, and the same ``LIMIT`` is what bounds them.

The bracket covers the statements this driver hands out. Queries pgJDBC issues
on its own connection are outside it — the mutations an updatable cursor makes
for ``updateRow()`` and ``insertRow()``, and the queries behind the metadata
API.

``Statement.cancel()`` is the cancel request alone, since it has no execution
to bracket — a request, not a guarantee. Reaching the node holding the session,
it takes effect, and the silence that follows is the whole of the answer the
protocol defines. Reaching any other node, the forwarding failure comes back as
an error on a connection that has no reply channel, and ``cancel()`` raises
rather than quietly doing nothing.

The ``Forwarding*`` base classes the wrappers extend are generated from the
JDBC and pgJDBC interfaces by ``devtools/GenerateForwarding.java``, so that
they forward everything the wrappers do not override, including whatever a
later JDBC release adds. Where ``java.sql`` nests one interface inside
another — a ``CallableStatement`` is a ``PreparedStatement`` is a
``Statement`` — the generated class picks up where the inner interface's
wrapper leaves off, so behavior is written once and inherited down the
chain: bracketing an execution with the query timeout lives in
``CrateStatement`` and holds for prepared statements and calls too. The
build checks the checked-in classes against a fresh generation.


*********
Artifacts
*********

:``crate-jdbc``:

    The regular Maven dependency. pgJDBC and jackson-databind come along as
    ordinary transitive dependencies, where dependency and vulnerability
    scanners can see them and a build can pin them.

:``crate-jdbc-standalone``:

    A self-contained jar for dropping into the driver directory of tools
    like `Apache Hop`_, Pentaho, DBeaver, or SQuirreL. Everything it
    bundles is relocated under ``io.crate.shade``, so the jar cannot clash
    with another pgJDBC or jackson on the same classpath, and each bundled
    dependency's license travels with it under ``META-INF/licenses/``. The
    bundled pgJDBC version is declared in the jar manifest as
    ``Bundled-PgJdbc-Version``, and the bundled pgJDBC does not register
    itself with the ``DriverManager``: ``jdbc:postgresql://`` URLs remain
    the province of a real PostgreSQL driver.

Both artifacts publish a `CycloneDX`_ SBOM under the ``cyclonedx``
classifier, which lists what the standalone jar bundles — relocation hides
those dependencies from scanners that read a pom.


**********************
Supported server range
**********************

The driver requires **CrateDB 6.0 or later**: pgJDBC's metadata queries rely
on server support (``current_catalog`` among it) that CrateDB gained in the
6.x line. For older servers, use crate-jdbc 2.7.0.

Queries and writes work against older servers, and so do the metadata calls
that do not read the catalog columns those servers lack; the ones that do
raise ``SQLFeatureNotSupportedException`` naming the version found, rather
than reporting a missing column. Which calls those are is not a contract —
treat 6.0 as the floor.

+----------------+---------------------+-------------------+
| Driver         | CrateDB             | JRE               |
+================+=====================+===================+
| 3.0.x          | 6.0 and later       | 11 and later      |
+----------------+---------------------+-------------------+
| 2.7.0          | 2.0 and later       | 8 and later       |
+----------------+---------------------+-------------------+

:ref:`getCrateVersion() <server-version>` reports the server's CrateDB
version to applications that need to branch on it. What the JDBC API reports
is the PostgreSQL release CrateDB emulates, since that is what PostgreSQL
tooling reasons about.

The parts of the API CrateDB cannot answer are listed under
:ref:`limitations`.


.. _Apache Flink JDBC Connector: https://github.com/apache/flink-connector-jdbc
.. _Apache Hop: https://hop.apache.org/
.. _CycloneDX: https://cyclonedx.org/
.. _PostgreSQL JDBC Driver: https://jdbc.postgresql.org/
