.. _crate_jdbc_data-types:

==========
Data types
==========

Type mapping
============

JDBC maps SQL types to `Plain Old Java Objects`_ (POJOs).

While this mapping is straightforward for most CrateDB types, for some it is
not.

The following table shows how the CrateDB types are mapped to `JDBC types`_ and
what method can be used to fetch them from a `ResultSet`_ instance:

+---------------+-----------------+------------------+
| CrateDB Type  | JDBC Type       | ResultSet Method |
+===============+=================+==================+
| `boolean`__   | `BOOLEAN`__     | ``getBoolean``   |
+---------------+-----------------+------------------+
| `byte`__      | `TINYINT`__     | ``getByte``      |
+---------------+-----------------+------------------+
| `short`__     | `SMALLINT`__    | ``getShort``     |
+---------------+-----------------+------------------+
| `integer`__   | `INTEGER`__     | ``getInteger``   |
+---------------+-----------------+------------------+
| `long`__      | `BIGINT`__      | ``getLong``      |
+---------------+-----------------+------------------+
| `float`__     | `REAL`__        | ``getFloat``     |
+---------------+-----------------+------------------+
| `double`__    | `DOUBLE`__      | ``getDouble``    |
+---------------+-----------------+------------------+
| `string`__    | `VARCHAR`__     | ``getString``    |
+---------------+-----------------+------------------+
| `ip`__        | `VARCHAR`__     | ``getString``    |
+---------------+-----------------+------------------+
| `timestamp`__ | `TIMESTAMP`__   | ``getDate``,     |
|               |                 | ``getTime``, or  |
|               |                 | ``getTimestamp`` |
+---------------+-----------------+------------------+
| `geo_point`__ | `ARRAY`__       | ``getArray``     |
+---------------+-----------------+------------------+
| `geo_shape`__ | `JAVA_OBJECT`__ | ``getObject``    |
+---------------+-----------------+------------------+
| `object`__    | `JAVA_OBJECT`__ | ``getObject``    |
+---------------+-----------------+------------------+
| `array`__     | `ARRAY`__       | ``getArray``     |
+---------------+-----------------+------------------+

__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#boolean
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#BOOLEAN
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#numeric-data
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#TINYINT
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#numeric-data
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#SMALLINT
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#numeric-data
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#INTEGER
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#numeric-data
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#BIGINT
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#numeric-data
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#REAL
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#numeric-data
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#DOUBLE
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#character-data
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#VARCHAR
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#ip
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#VARCHAR
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#dates-and-times
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#TIMESTAMP
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#geo-point
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#ARRAY
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#geo-shape
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#JAVA_OBJECT
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#object
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#JAVA_OBJECT
__ https://crate.io/docs/crate/reference/en/latest/general/ddl/data-types.html#array
__ https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html#ARRAY

Array types
-----------

Array types map to `java.sql.Array`_ instances.

Use the ``.getArray()`` to get the underlying array (it is usually safe to
cast it to ``Object[]``) if you prefer. Otherwise, to have JDBC handle type
mapping, you can use ``.getResultSet()`` and use the related
``ResultSetMetaData`` to get the array values converted to Java POJOs.

The ``ResultSet`` will have one column with the inner array type and the name
of the array field (``Array.getBaseType()``) and as many rows as there are
elements in the array.

Here's one example:

.. code-block:: java

	Statement statement = connection.createStatement();
	ResultSet resultSet = statement.executeQuery(
		"SELECT array_field FROM my_table"
	);
	resultSet.first();
	Array arrayField = resultSet.getArray("array_field");
	Object[] arrayFieldValue = (Object[]) arrayFieldValue.getArray();

When inserting arrays using a prepared statement, you must convert your array
to a `java.sql.Array`_ by the use of ``createArrayOf()``. This function takes
as its first argument, a CrateDB type as described above and as its second the
array you want to convert.

You can then use ``setArray()`` to set this converted array.

For example:

.. code-block:: java

    PreparedStatement preparedStatement = connection.prepareStatement(
        "INSERT into my_table (string_array) VALUES (?)"
    );
    preparedStatement.setArray(1, connection.createArrayOf(
        "string", new String[]{"a", "b"}
    ));
    preparedStatement.execute();

Object types
------------

Object columns map to a `java.util.Map<String, Object>`_ object.

You can fetch them using ``ResultSet.getObject()`` and cast the result to
``Map<String, Object>``. This ``Map`` will contain all nested columns defined in
the object.

Here's an example:

.. code-block:: java

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "SELECT object_field FROM my_table"
    );
    resultSet.first();

    Map<String, Object> objectValue = (Map<String, Object>) resultSet.getObject("object_field");
    Object objectField = objectValue.get("nested_field");

.. CAUTION::

   Objects can be ``null``.

.. _java.sql.Array: https://docs.oracle.com/javase/8/docs/api/java/sql/Array.html
.. _java.util.Map<String, Object>: https://docs.oracle.com/javase/8/docs/api/java/util/Map.html
.. _JDBC types: https://docs.oracle.com/javase/8/docs/api/java/sql/Types.html
.. _Plain Old Java Objects: https://en.wikipedia.org/wiki/Plain_old_Java_object
.. _ResultSet: https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html
