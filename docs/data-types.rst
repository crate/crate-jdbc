.. _data-types:

==========
Data types
==========

Type mapping
============

Every `CrateDB type`_, against the `JDBC type`_ it is reported as and the
`ResultSet`_ method that reads it. Types CrateDB shares with PostgreSQL are
decoded by pgJDBC; the rest are this driver's, and the notes say what it does
with them.

+-----------------------+------------------+----------------------+---------------------------------------+
| CrateDB type          | JDBC type        | ResultSet method     | Notes                                 |
+=======================+==================+======================+=======================================+
| ``boolean``           | ``BOOLEAN``      | ``getBoolean``       |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``byte``              | see note         | ``getByte``          | Reported as ``SMALLINT`` by CrateDB   |
|                       |                  |                      | 6.5 and later, as ``CHAR`` before it. |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``short``             | ``SMALLINT``     | ``getShort``         |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``integer``           | ``INTEGER``      | ``getInt``           |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``long``              | ``BIGINT``       | ``getLong``          |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``float``             | ``REAL``         | ``getFloat``         |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``double``            | ``DOUBLE``       | ``getDouble``        |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``numeric``           | ``NUMERIC``      | ``getBigDecimal``    |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``text``, ``string``  | ``VARCHAR``      | ``getString``        | ``varchar(n)`` and ``character(n)``   |
|                       |                  |                      | read the same way.                    |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``ip``                | ``VARCHAR``      | ``getString``        |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``uuid``              | ``OTHER``        | ``getObject``        | Returns a ``java.util.UUID``.         |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``timestamp``         | ``TIMESTAMP``    | ``getTimestamp``     | Wall-clock time, read in the JVM's    |
|                       |                  |                      | zone. Pass a ``Calendar`` to pin one. |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``timestamptz``       | ``TIMESTAMP``    | ``getTimestamp``     | An instant, unaffected by the JVM     |
|                       |                  |                      | zone.                                 |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``date``              | ``DATE``         | ``getDate``          |                                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``time``              | ``TIME``         | ``getTime``          | CrateDB's ``time`` is always          |
|                       |                  |                      | ``time with time zone``.              |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``interval``          | ``OTHER``        | ``getObject``        | Returns a ``PGInterval``.             |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``bit(n)``            | ``OTHER``        | ``getObject``        | Returns a ``PGobject``; its value is  |
|                       |                  |                      | the bit string.                       |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``object``            | ``OTHER``        | ``getObject``        | Returns a ``Map<String, Object>``.    |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``geo_point``         | ``OTHER``        | ``getObject``        | Returns a ``PGpoint``, whose ``x`` is |
|                       |                  |                      | the longitude and ``y`` the latitude. |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``geo_shape``         | ``OTHER``        | ``getObject``        | Returns a ``Map<String, Object>``.    |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``array(...)``        | ``ARRAY``        | ``getArray``         | See `Array types`_.                   |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``array(array(...))`` | ``OTHER``        | ``getArray``,        | Travels as JSON; see                  |
|                       |                  | ``getObject``        | `Arrays of arrays`_.                  |
+-----------------------+------------------+----------------------+---------------------------------------+
| ``float_vector(n)``   | ``ARRAY``        | ``getArray``         | See `Vector search`_.                 |
+-----------------------+------------------+----------------------+---------------------------------------+

``json`` exists for PostgreSQL interoperability and cannot be used in a table
definition — CrateDB stores JSON data in ``object`` columns. A value cast to
``json`` reads back as a ``Map``, the same way an ``object`` does.

Binding a ``java.time`` value carries the zone it has. A ``LocalDateTime`` has
none, and a ``timestamptz`` column reads one as UTC instead of in the JVM's
zone, which is what CrateDB does with any timestamp naming no offset. A
local-zone PostgreSQL leaves the opposite habit. Bind an ``OffsetDateTime`` or
an ``Instant`` where the offset matters.


.. _vector-search:

Vector search
-------------

A ``float_vector(n)`` column stores a dense vector of ``real`` values. Read it
with ``getArray`` and bind it with ``createArrayOf("float_vector", ...)``:

.. code-block:: java

    PreparedStatement insert = connection.prepareStatement(
        "INSERT INTO embeddings (id, embedding) VALUES (?, ?)"
    );
    insert.setInt(1, 1);
    insert.setArray(2, connection.createArrayOf(
        "float_vector", new Float[]{1.0f, 1.0f, 1.0f}
    ));
    insert.execute();

``knn_match`` restricts a query to its k nearest rows, and
``vector_similarity`` scores a row against a query vector. Both take the
query vector as an ordinary parameter, so an embedding computed at runtime
does not have to be spliced into the statement text:

.. code-block:: java

    PreparedStatement search = connection.prepareStatement(
        "SELECT id, vector_similarity(embedding, ?) AS score " +
        "FROM embeddings WHERE knn_match(embedding, ?, 10) " +
        "ORDER BY score DESC"
    );
    Array queryVector = connection.createArrayOf("float_vector", embedding); // Float[]
    search.setArray(1, queryVector);
    search.setArray(2, queryVector);
    ResultSet matches = search.executeQuery();

A ``float_vector`` cannot be the element type of an array.

.. _array-types:

Array types
-----------

An array column reads back as a `java.sql.Array`_. Its ``getArray()`` hands
over the elements as a Java array, which is safe to cast to ``Object[]``:

.. code-block:: java

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "SELECT array_field FROM my_table"
    );
    resultSet.next();
    Array arrayField = resultSet.getArray("array_field");
    Object[] elements = (Object[]) arrayField.getArray();

``getResultSet()`` reads the same elements as rows instead, with the
``ResultSetMetaData`` that describes them: one row per element, and two
columns — the element's position in the array and its value, typed as
``Array.getBaseType()`` reports.

An array parameter is bound as a ``java.sql.Array`` too, built with
``createArrayOf()`` from a CrateDB type name and the values:

.. code-block:: java

    PreparedStatement preparedStatement = connection.prepareStatement(
        "INSERT into my_table (string_array) VALUES (?)"
    );
    preparedStatement.setArray(1, connection.createArrayOf(
        "string", new String[]{"a", "b"}
    ));
    preparedStatement.execute();

Arrays of timestamps
~~~~~~~~~~~~~~~~~~~~

A ``timestamp with time zone`` holds an instant, so an array of them is bound
with each element's offset written out and means the same moment wherever the
JVM stands. A ``timestamp without time zone`` holds a wall clock, and an array
of them is bound in the JVM's own zone. That is the reading ``setTimestamp()``
gives a single value, and the one ``getTimestamp()`` gives back.

Binding a collection is the one case with nothing to go on. The column type is
unknown at the point the values are converted, so the elements are written as
the instants they name, and a ``List<java.sql.Timestamp>`` lands in a
``timestamp without time zone`` column as its UTC wall clock instead of the
JVM's. Name the type to say which is meant:

.. code-block:: java

    // the instants, whatever zone the JVM runs in
    statement.setArray(1, connection.createArrayOf(
        "timestamp with time zone", new Timestamp[]{one, other}
    ));

    // the wall clocks, as the JVM reads them
    statement.setArray(1, connection.createArrayOf(
        "timestamp without time zone", new Timestamp[]{one, other}
    ));

Arrays of geographic points
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``geo_point`` is a pair of doubles, which leaves an array of them
indistinguishable from an array of arrays. CrateDB accepts no array of numbers
for a ``geo_point`` array, so bind the points as the WKT text it does accept:

.. code-block:: java

    statement.setObject(1, List.of("POINT (9.7419 47.4048)"));

Arrays of arrays
~~~~~~~~~~~~~~~~

The PostgreSQL array format cannot hold sub-arrays of differing length, so a
column of ``array(array(...))`` travels as JSON. The driver reads it either way
round:

.. code-block:: java

    // as nested lists
    List<List<Integer>> rows = resultSet.getObject("rows_", List.class);

    // or as a java.sql.Array whose elements are arrays
    Object[] alsoRows = (Object[]) resultSet.getArray("rows_").getArray();

Bind one as the nested collections or arrays it reads back as, or build it
with ``createArrayOf()``:

.. code-block:: java

    statement.setObject(1, List.of(List.of(1, 2), List.of(3)));
    statement.setArray(1, connection.createArrayOf(
        "integer", new Object[][]{{1, 2}, {3}}
    ));

.. NOTE::

   ``Array.getResultSet()`` is the one thing such a column cannot offer. Its
   elements are arrays, and the PostgreSQL protocol has no column descriptor
   for those, so it raises ``SQLFeatureNotSupportedException``. ``getArray()``
   reads the same elements.

Type descriptions
-----------------

``ResultSetMetaData`` and ``DatabaseMetaData.getTypeInfo()`` describe
columns as pgJDBC sees them, in PostgreSQL's terms. For a few types the
described class and the value ``getObject()`` returns differ:

+------------------------+--------------------------+------------------------+
| Column type            | ``getColumnClassName()`` | ``getObject()`` returns|
+========================+==========================+========================+
| ``object``,            | ``java.lang.Object``     | ``Map``                |
| ``geo_shape``          |                          |                        |
+------------------------+--------------------------+------------------------+
| ``array(array(...))``  | ``java.lang.Object``     | ``List``               |
+------------------------+--------------------------+------------------------+
| ``bit(n)``             | ``java.lang.Boolean``    | ``PGobject``           |
+------------------------+--------------------------+------------------------+
| ``regclass``,          | ``java.lang.String``     | ``PGobject``           |
| ``regproc``            |                          |                        |
+------------------------+--------------------------+------------------------+

Code that maps values by their described type, instead of reading them through
``getObject()``, has to account for that. Casting a value to the class named for
it throws ``ClassCastException`` for ``bit(n)``, ``regclass`` and ``regproc``,
which is the use ``getColumnClassName()`` exists for.

CrateDB sends the first three rows under one column type, so the described class
is the one their forms have in common. An ``object`` reads as a ``Map`` and a
column of nested arrays as a ``List``, and the described type cannot say which
of the two a column holds.

The last two rows are pgJDBC's own type descriptions rather than anything
CrateDB does. The ``bit(n)`` one is `pgjdbc#2955`_, open upstream.

Object types
------------

An ``object`` column reads back as a `java.util.Map<String, Object>`_ holding
every nested column of the object, through ``getObject()``:

.. code-block:: java

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "SELECT object_field FROM my_table"
    );
    resultSet.next();

    Map<String, Object> objectValue = resultSet.getObject("object_field", Map.class);
    Object nested = objectValue.get("nested_field");

An ``object`` column can hold null, in which case ``getObject()`` returns
null rather than an empty map.

The value arrives as json text, and the map is built from it on each read. The
map is several times the larger of the two, so code that only forwards an object
elsewhere (an export, a copy between stores) can take the text itself with
``getString()`` and skip building it.

Json carries fewer types than Java does, so a map written and read back is
not always the map that went in:

- A whole number reads back as a ``Long``, whatever it was written as: a
  whole number in an ``object`` is a ``bigint``, and sizing the Java type to
  the value would make what a nested column reads as depend on the row. One
  too large for a ``bigint`` is a ``numeric``, which holds 38 digits and
  reads back as a ``BigInteger``.
- A ``java.time`` value is written as ISO-8601 text, which CrateDB reads a
  ``timestamp`` from. A ``java.sql.Timestamp`` is written as epoch
  milliseconds, which it equally reads a ``timestamp`` from, though a
  *dynamic* object infers ``bigint`` from it. Declare the nested column where
  a ``timestamp`` is what it should be.
- A ``byte[]`` is written as base64 text and reads back as a ``String``.
- A ``BigDecimal`` reads back as a ``Double``, since a nested column of a
  fractional number is a ``double precision``.

.. _CrateDB type: https://cratedb.com/docs/crate/reference/en/latest/general/ddl/data-types.html
.. _java.sql.Array: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/Array.html
.. _java.util.Map<String, Object>: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Map.html
.. _JDBC type: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/Types.html
.. _pgjdbc#2955: https://github.com/pgjdbc/pgjdbc/issues/2955
.. _ResultSet: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/ResultSet.html
