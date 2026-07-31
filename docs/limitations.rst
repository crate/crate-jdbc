.. _limitations:

###########
Limitations
###########

CrateDB is not PostgreSQL, and the parts of the JDBC API that describe a
PostgreSQL database reach for things it does not have.


************
Transactions
************

There are none. ``BEGIN`` and ``COMMIT`` parse and do nothing, ``ROLLBACK``
is not part of CrateDB's SQL grammar at all, and a statement is durable by
the time it returns. ``rollback()`` therefore undoes nothing — it only ends
the transaction block pgJDBC opens under manual commit mode — and savepoints
raise ``SQLFeatureNotSupportedException``.

``DatabaseMetaData`` says so: ``supportsTransactions()`` and
``supportsSavepoints()`` report ``false``, and ``TRANSACTION_NONE`` is the
only isolation level. Frameworks that need a working ``rollback()`` to undo
writes — Spring's ``PROPAGATION_NESTED``, Hibernate's savepoint rollback —
do not get one.


********
Metadata
********

:``getIndexInfo()``:

    Answers with no rows, and four gaps stand behind that, each enough on
    its own: ``pg_get_indexdef()``, which the query reads, is absent from
    CrateDB's partial ``pg_catalog``; ``pg_am`` is there but empty, where
    PostgreSQL's holds seven rows; ``pg_indexes`` is empty too, so the other
    route to the same information is closed as well; and ``pg_index``
    describes a primary key's own index as ``indisunique = false`` spanning
    ``indnatts = 0`` columns on the same row that says
    ``indisprimary = true`` — a wrong answer rather than a missing one.
    Primary keys are still readable through ``getPrimaryKeys()``.

:Comments and defaults:

    ``REMARKS`` and ``COLUMN_DEF`` are null in every metadata result, and
    ``IS_AUTOINCREMENT`` is always ``NO``: CrateDB's catalog carries no
    descriptions, and no column defaults in the form PostgreSQL keeps them
    in.

:``getTables()`` with a null types argument:

    Also lists the index entries CrateDB exposes through ``pg_class``. Pass
    an explicit types array such as ``{"TABLE", "VIEW"}``.

:Foreign keys:

    ``getImportedKeys()`` and ``getExportedKeys()`` answer with no rows.
    CrateDB has no foreign key constraints.

:An empty-string catalog argument:

    JDBC reads ``""`` as "objects belonging to no catalog", which every
    CrateDB object fails — the answer by the letter of the specification is
    no rows at all. It is read as ``null`` instead, so such a call answers
    as though the catalog had not been named. CrateDB has one catalog, and
    a caller that spells "any" the other way is better served with the rows
    than with silence.

:Column descriptions:

    ``ResultSetMetaData`` describes a column by its type and little else.
    An unaliased column carries the table oid and attribute number that name
    where it came from; aliasing one sends both as zero, and nothing about
    the base table can be looked up from that. ``getColumnName()`` answers
    with the label the query gave it, and ``isNullable()`` is always
    ``columnNullableUnknown``.

    ``getPrecision()`` and ``getScale()`` report 0 even for a
    ``numeric(10, 2)``: the type modifier that would carry the length
    arrives as ``-1``. ``bit(n)`` is the one parameterised type that keeps
    its length, because a ``bit`` type is constructed per length while
    ``numeric``, ``varchar`` and ``character`` are shared singletons holding
    that constant ``-1``.


*********
Functions
*********

CrateDB's user-defined functions are functions rather than procedures, so
``getFunctions()`` lists them and ``getProcedures()`` does not. They are
callable through the JDBC escape ``{call f(?)}``, which pgJDBC rewrites into
a ``SELECT``; the ``{?= call f(?)}`` form, which asks for the return value as
an out parameter, is not supported.


************************
``COPY`` from the client
************************

``PGConnection.getCopyAPI()`` is reachable, but CrateDB does not accept
``COPY ... FROM STDIN``. Its own ``COPY FROM`` reads a URI the server
resolves. :ref:`Batched inserts <batching>` are the way to load rows through
the driver.


*******************
``preferQueryMode``
*******************

The driver binds the values the server has to type for itself — a nested
array, an empty array, an array of nothing but nulls — without naming a type,
leaving the server to take one from the column they land in. That needs the
extended protocol, where a parameter travels beside the statement rather than
inside it.

``preferQueryMode=simple`` writes parameters into the statement text instead,
so those values arrive as text and the server has nothing to convert them
from::

    Cannot convert VALUES element in row 1 of type `text` to `boolean_array_array`

Every other value binds as it does otherwise. Leave ``preferQueryMode`` at its
default to use nested or empty arrays.

Under that mode, ``PreparedStatement.getMetaData()`` and
``getParameterMetaData()`` ask for a description the simple query protocol has
no message to carry. With assertions enabled the driver fails an internal
assertion. With assertions disabled — every JVM not started with ``-ea`` — the
request falls through into the simple-query path and the statement is executed:
``getMetaData()`` on a prepared ``INSERT`` performs the insert and answers
``null``, and ``getParameterMetaData()`` on one performs it too. A metadata call
writes to the database and reports nothing. Do not call either accessor on a
statement that has not been executed under this mode. A statement that has
already been executed has a result to describe, so no describe request is sent
and neither accessor touches the database.


**********************
First-contact failures
**********************

:``No suitable driver found for jdbc:crate://…``:

    The driver is not on the classpath, or something has taken it out of the
    ``DriverManager``. With the standalone jar, check that it is the jar in
    the tool's driver directory and that the driver class is
    ``io.crate.client.jdbc.CrateDriver``.

:``mismatched input 'ROLLBACK'``:

    A plain pgJDBC driver is answering the connection, not this one. Check
    that the URL scheme is ``jdbc:crate://`` — ``jdbc:postgresql://`` URLs
    are deliberately left to a PostgreSQL driver.

:``This metadata call needs CrateDB 6.0 or later``:

    pgJDBC's catalog queries read columns that arrived in the 6.x line. Use
    crate-jdbc 2.7.0 against an older server.
