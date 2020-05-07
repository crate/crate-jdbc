=================
Crate JDBC Driver
=================

JDBC is a core API of Java 1.1 and later. It provides a standard set of
interfaces to SQL-compliant databases.

Crate provides a type 4 JDBC driver. The driver is written in Pure Java, and
communicates in the database system's own network protocol. Because of this,
the driver is platform independent.

Download and Setup
==================

The ``crate-jdbc`` jar files are hosted on `Bintray`_ and available via `JCenter`_.

Additionally to ``crate-jdbc`` there is also a standalone version
named ``crate-jdbc-standalone`` which already includes its dependencies.

.. note:: ``crate-jdbc-standalone`` should not be used in Maven projects,
    but might want to be used e.g. as a driver for the `Squirrel`_ SQL client.

You can download the latest standalone version directly from the
`Bintray Repository <https://bintray.com/crate/crate/crate-jdbc/view/files/io/crate/crate-jdbc-standalone>`_.

If you want to use ``crate-jdbc`` with your Maven project you need to
add the Bintray repository to your ``pom.xml``:

.. code-block:: xml

    ...
    <repositories>
        ...
        <repository>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
            <id>central</id>
            <name>bintray</name>
            <url>http://dl.bintray.com/crate/crate</url>
        </repository>
    </repositories>
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>io.crate</groupId>
            <artifactId>crate-jdbc</artifactId>
            <version>...</version>
        </dependency>
    </dependencies>
    ...


Using Gradle:

.. code-block:: groovy

    repositories {
        ...
        jcenter()
    }

    dependencies {
        compile 'io.crate:crate-jdbc:...'
        ...
    }

Alternatively you can follow the instructions on the Bintray repository overview page by clicking the "Set me up!" button.


JDBC Driver Class
=================

A connection can be established using ``DriverManager.getConnection()``
method, e.g.::

    Connection conn = DriverManager.getConnection("crate://localhost:4300");

JDBC URL Format
===============

With JDBC, a database is represented by a URL (Uniform Resource Locator).
With Crate, this takes the following form::

    [jdbc:]crate://<host>:<transport-port>[,<host>:<transport-port> , ...][/<schemaName>]
        [?propertyName1=propertyValue1[&propertyName2=propertyValue2]...]

The ``jdbc:`` prefix is optional. For example. To connect to a single server
the following two formats are both allowed::

    crate://localhost:4300

::

    jdbc:crate://localhost:4300


In order to connect to multiple servers multiple ``<host>:<transport-port>``
pairs can be specified by delimit them using a comma::

    crate://host1.example.com:4300,host2.example.com:4300

The optional ``/<schemaName>`` part can be used to use the specified schema by
default instead of the ``doc`` schema which Crate would use otherwise if no
schemaName is specified.

This will implicitly call ``setSchema`` on the ``Connection`` class. So the
following two statements are equivalent::

    Connection conn = DriverManager.getConnection("crate://localhost:4300/foo");

Is the same as::

    Connection conn = DriverManager.getConnection("crate://localhost:4300");
    conn.setSchema("foo");


.. note::

    Default schema support requires at least Crate 0.48.1. If the Crate server
    that is used has a version that is lower than 0.48.1 the specified schema
    will be ignored and the default ``doc`` schema will be used instead.

Crate JDBC properties
=====================

Properties can be specified when connecting to Crate using the JDBC driver::

    Properties properties = new Properties();
    properties.put(<key>, <value>);
    Connection conn = DriverManager.getConnection("crate://localhost:4300", properties);

In addition connection properties can be passed via the JDBC URL::

    Connection conn = DriverManager.getConnection("crate://localhost:4300?property1=value1&property2=value2");

Crate JDBC driver supports following properties:

:strict:
    **Default**: ``false``.

    Setting the strict property to ``true`` will
    enforce the driver to be compliant with the JDBC specification.
    Therefore, calling any operations that are not supported by the
    driver will result in an exception. For instance, disabling the
    auto-commit mode or setting a savepoint will raise an unsupported
    operation exception.
    By default the ``strict`` property is optimistically set to ``false``
    which will make the Crate JDBC driver compatible with most 3rd party
    applications that usually require transactional databases.

:showsubcolumns:
    **Default**: ``false``.

    .. warning:: This property is experimental and may be subject to change in
                 future releases.

    If this property value is set to ``true`` the method ``getColumns()``
    will also list all subcolumns for columns of type ``OBJECT``.
    The output is then equivalent to::

        SELECT column_name
        FROM information_schema.columns
        WHERE schema_name = ? AND table_name = ?

    By setting the property to ``false`` only the root column (the object
    field name) itself is returned.

    .. note:: 3rd party tool ususally quote the identifiers obtained by the
              ``getColumns()`` method. Quoting needs to be disabled when this
              property is set to ``true``. This however limits the naming of
              columns, because they must only contain lower case ASCII letters.


Compatibility
=============

JDBC
----

This JDBC driver complies to the `JDBC 4.1`_ standard as good as possible and reasonable for
a Crate driver. The following mandatory features are not supported:

* ``java.sql.ParameterMetaData`` as returned by e.g. ``java.sql.PreparedStatement``
* ``DataSource`` is not implemented
* ``CallableStatement`` is not implemented as CRATE has no stored procedures to call

For further details about compatibility with all possible JDBC features,
see the ``ResultSet``, ``ResultSetMetaData`` and ``DatabaseMetaData`` implementations.

Though only an optional feature, it is still worth mentioning:

* the ``ResultSet`` implementation is read only (``TYPE_FORWARD_ONLY``, ``CONCUR_READ_ONLY``),
  so changes to a ``ResultSet`` are not supported. Though DDL and DML statements are supported
  using the ``Statement`` and ``PreparedStatement`` interfaces.

Crate
-----

This JDBC driver can only be used with *Crate version 0.38.0* and higher.

Besides using the most recent version, it is recommended to use the JDBC driver version
whose crate-client dependency matches the Crate server. Below, you can see the
compatibility table for the JDBC driver version ``1.x.x``:

.. csv-table::
   :header: "Crate JDBC driver", "Crate"

   "1.8.X", "0.49.X"
   "1.9.X", "0.51.X"
   "1.10.X", "0.54.X"
   "1.11.X", "0.54.X"
   "1.12.X", "0.54.X"
   "1.13.X", "0.55.X"
   "1.14.X", "0.56.X"


Types
=====

JDBC maps SQL types to POJOs. While this mapping is straightforward
for some crate types, for some it is not. This table shows how the crate types
are mapped by JDBC and how they should be fetched from a ``ResultSet`` instance:

.. csv-table:: Frozen Delights!
   :header: "Crate type", "JDBC type", "ResultSet method used to get a value"

   "boolean", "BOOLEAN", "``getBoolean``"
   "byte", "TINYINT", "``getByte``"
   "short", "SMALLINT", "``getShort``"
   "integer", "INTEGER", "``getInteger``"
   "long", "BIGINT", "``getLong``"
   "float", "REAL", "``getFloat``"
   "double", "DOUBLE", "``getDouble``"
   "string", "VARCHAR", "``getString``"
   "timestamp", "TIMESTAMP", "``getDate``, ``getTime``, ``getTimestamp``"
   "ip", "VARCHAR", "``getString``"
   "array", "ARRAY", "``getArray``"
   "object", "JAVA_OBJECT", "``getObject``"

Array Types
-----------

Array types map to ``java.sql.Array`` instances. Use the ``.getArray()`` to get
the underlying java array (it is usually safe to cast it to ``Object[]``) if you prefer.
Otherwise, to have JDBC handle the type mapping stuff for you, you can use ``.getResultSet()``
and use the related ``ResultSetMetaData`` or another way to get the array values
converted to Java POJOs. The ``ResultSet`` will have one column with the inner array type
and the name of the array field (``Array.getBaseType()``) and as much rows as
there are elements in the array.

.. code-block:: java

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select array_field from my_table");
    resultSet.first();
    Array arrayFieldArray = resultSet.getArray("array_field");
    Object[] arrayFieldValue = arrayFieldValue.getArray();

    ResultSet arrayFieldResultSet = arrayFieldArray.getResultSet();
    arrayFieldResultSet.first();
    String firstValue = arrayFieldResultSet.getString("array_field");


Object Types
------------

Object columns map to a ``java.util.Map<String, Object>``. You can fetch them
using ``ResultSet.getObject()`` and cast the result to ``Map<String, Object>``.
Be aware that it can be ``null``. This map will contain all the nested columns
defined in that object:

.. code-block:: java

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select object_field from my_table");
    resultSet.first();
    Map<String, Object> objValue = (Map<String, Object>)resultSet.getObject("object_field");
    Object nestedValue = objValue.get("nested_field");


.. _`Bintray`: https://bintray.com/crate/crate/

.. _`JCenter`: https://bintray.com/bintray/jcenter

.. _`Squirrel`: http://squirrel-sql.sourceforge.net/

.. _`JDBC 4.1`: http://download.oracle.com/otn-pub/jcp/jdbc-4_1-mrel-spec/jdbc4.1-fr-spec.pdf
