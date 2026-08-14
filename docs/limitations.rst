.. _limitations:

###########
Limitations
###########

CrateDB is not PostgreSQL, and the parts of the JDBC API that describe a
PostgreSQL database reach for things it does not have.


************
Transactions
************

There are none. ``BEGIN`` and ``COMMIT`` parse and do nothing, ``ROLLBACK`` is
absent from CrateDB's SQL grammar, and a statement is durable by the time it
returns. ``rollback()`` therefore undoes nothing, ending only the transaction
block pgJDBC opens under manual commit mode, and savepoints raise
``SQLFeatureNotSupportedException``.

``DatabaseMetaData`` says so: ``supportsTransactions()`` and
``supportsSavepoints()`` report ``false``, and ``TRANSACTION_NONE`` is the
only isolation level.


********
Metadata
********

:``getIndexInfo()``:

    Answers with no rows. Four gaps stand behind that, each enough on its
    own: ``pg_get_indexdef()``, which the query reads, is absent from CrateDB's
    partial ``pg_catalog``; ``pg_am`` is there but empty, where PostgreSQL's
    holds seven rows; ``pg_indexes`` is empty too, closing the other route to
    the same information; and ``pg_index`` describes a primary key's own index
    as ``indisunique = false`` spanning ``indnatts = 0`` columns on the same
    row that says ``indisprimary = true``, which is a wrong answer where the
    others are missing ones. Primary keys are still readable through
    ``getPrimaryKeys()``.

:Comments and defaults:

    ``REMARKS`` and ``COLUMN_DEF`` are null in every metadata result, and
    ``IS_AUTOINCREMENT`` is always ``NO``. CrateDB's catalog carries no
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
    CrateDB object fails, so by the letter of the specification such a call
    answers with no rows. It is read as ``null`` instead, as though the catalog
    had not been named. CrateDB has one catalog, and a caller spelling "any"
    the other way is better served with the rows than with silence.

:Column descriptions:

    ``ResultSetMetaData`` describes a column by its type and little else. An
    unaliased column carries the table oid and attribute number that name where
    it came from; aliasing one sends both as zero, leaving nothing about the
    base table to look up. ``getColumnName()`` answers with the label the query
    gave it, and ``isNullable()`` is always ``columnNullableUnknown``.

    ``getPrecision()`` and ``getScale()`` report 0 even for a
    ``numeric(10, 2)``, the type modifier that would carry the length arriving
    as ``-1``. ``bit(n)`` is the one parameterised type that keeps its length,
    a ``bit`` type being constructed per length where ``numeric``, ``varchar``
    and ``character`` are shared singletons holding that constant ``-1``.


*********
Functions
*********

CrateDB's user-defined functions are functions and not procedures, so
``getFunctions()`` lists them and ``getProcedures()`` does not. They are
callable through the JDBC escape ``{call f(?)}``, which pgJDBC rewrites into a
``SELECT``. The ``{?= call f(?)}`` form, which asks for the return value as an
out parameter, is unsupported.


*************************
Binding by ``SQLType``
*************************

JDBC defines five ways to hand a value to ``setObject``, three naming the
target type as an ``int`` and two as a ``java.sql.SQLType``. This driver binds
what it converts itself (an ``OBJECT``, a nested array) through any of the five.
Anything else it passes to pgJDBC, which has not implemented the two
``SQLType`` forms and raises ``SQLFeatureNotSupportedException`` from them.

So ``setObject(1, value, JDBCType.VARCHAR)`` fails where
``setObject(1, value, Types.VARCHAR)`` succeeds. Name the target type as an
``int``, or leave it out.


************************
``COPY`` from the client
************************

``PGConnection.getCopyAPI()`` is reachable, though CrateDB does not accept
``COPY ... FROM STDIN``. Its own ``COPY FROM`` reads a URI the server resolves.
:ref:`Batched inserts <batching>` are how rows are loaded through the
driver.


*******************
``preferQueryMode``
*******************

Some values have to be typed by the server: a nested array, an empty array, an
array of nothing but nulls. The driver binds those without naming a type, for
the server to take one from the column they land in. That needs the extended
protocol, where a parameter travels beside the statement instead of inside
it.

``preferQueryMode=simple`` writes parameters into the statement text instead,
so those values arrive as text and the server has nothing to convert them
from::

    Cannot convert VALUES element in row 1 of type `text` to `boolean_array_array`

Every other value binds as it does otherwise. Leave ``preferQueryMode`` at its
default to use nested or empty arrays.

Under that mode, ``PreparedStatement.getMetaData()`` and
``getParameterMetaData()`` ask for a description the simple query protocol has
no message to carry. With assertions enabled the driver fails an internal
assertion. With assertions disabled, which is every JVM not started with
``-ea``, the request falls through into the simple-query path and the statement
is executed: ``getMetaData()`` on a prepared ``INSERT`` performs the insert and
answers ``null``, and ``getParameterMetaData()`` on one performs it too. A
metadata call writes to the database and reports nothing.

Executing the statement first helps only one of the two. A query that has run
carries its own result description, so ``getMetaData()`` answers from it and
asks the server nothing. ``getParameterMetaData()`` describes the parameters
through a request of its own, sent every time, so on a statement that has
already run the fall-through runs it a second time. Under this mode, call
``getMetaData()`` only on a statement that has been executed, and
``getParameterMetaData()`` not at all.


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
    that the URL scheme is ``jdbc:crate://``; ``jdbc:postgresql://`` URLs are
    deliberately left to a PostgreSQL driver.

:``This metadata call needs CrateDB 6.0 or later``:

    pgJDBC's catalog queries read columns that arrived in the 6.x line. Use
    crate-jdbc 2.7.0 against an older server.
