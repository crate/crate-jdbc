.. _connect:

==================
Connect to CrateDB
==================

.. _introduction:

Introduction
============

The driver registers ``io.crate.client.jdbc.CrateDriver`` with the
``DriverManager`` as soon as it is on the class path. The jar names it in a
``META-INF/services/java.sql.Driver`` entry that the ``DriverManager``
reads.

.. _basics:

The basics
==========

Connect to CrateDB using the ``DriverManager`` class, like so::

    Connection conn = DriverManager.getConnection(
        "jdbc:crate://localhost:5432/doc?user=crate");

Connections are authenticated; :ref:`connection_properties` covers supplying
a user and a password.

.. _database-urls:

Database connection URLs
========================

A CrateDB connection URL names one or more nodes and the schema unqualified
statements resolve against::

    jdbc:crate://<HOST>/<SCHEMA>

``<HOST>`` is a host string, ``<HOST_ADDR>:<PORT>``, where ``<PORT>`` is a
:ref:`psql.port <crate-reference:conf_ports>`: ``localhost:5432`` or
``198.51.100.1:5432``. The ``jdbc:`` prefix may be left off, though tools and
connection pools that validate JDBC URLs expect it.

Leave the schema out and CrateDB's default schema, ``doc``, is used::

    jdbc:crate://<HOST>/

Name as many nodes as the cluster has, separated by ``,``. The last host
string is followed by the ``/``::

    jdbc:crate://<HOST_ADDR_1>,<HOST_ADDR_2>/

The driver connects to one node and uses it for the duration of that
connection. Which one it tries first depends on ``loadBalanceHosts``, which
this driver turns on by default: the host strings are shuffled, so that
connections spread across the cluster. Set it to ``false`` to try the nodes
in the order they appear.

.. _schema-selection:

Schema selection
================

The schema unqualified statements resolve against is the last segment of the
connection URL::

    jdbc:crate://localhost:5432/my_schema?user=crate

It can be changed on an open connection with ``setSchema``:

.. code-block:: java

    Connection conn = DriverManager.getConnection(
        "jdbc:crate://localhost:5432/doc?user=crate");
    conn.setSchema("my_schema");

Either way it decides how unqualified names resolve and nothing more. A
statement can name any schema it likes, whatever the connection is set to.

.. NOTE::

   The pgJDBC ``currentSchema`` property has no effect: pgJDBC sends it as a
   PostgreSQL startup parameter that CrateDB does not read. Use the URL
   segment, ``setSchema``, or the ``options`` property
   (``options=-c%20search_path%3Dmy_schema``).

.. _connection_properties:

Connection properties
=====================

A connection is configured with properties, passed either as `URL
parameters`_ or as a ``Properties`` object:

.. code-block:: java

    Properties properties = new Properties();
    properties.put("user", "crate");
    Connection conn = DriverManager.getConnection(
        "jdbc:crate://localhost:5432/doc", properties
    );

The driver accepts every `pgJDBC connection property`_. A property set in the
URL wins over the same property passed in the ``Properties`` object. Those
with a CrateDB-specific default or meaning are listed below.

:``user``:

  Specifies the CrateDB user.

  Defaults to the name of the OS user running the JVM, which is rarely a
  CrateDB user — set it explicitly.

  .. NOTE::

     If you have not configured a custom
     :ref:`database user <crate-reference:administration_user_management>`,
     you probably want to authenticate as the CrateDB superuser, which is
     ``crate``. The superuser does not have a password, so you can omit the
     ``password`` property.

:``password``:

  Sets the password for authentication.

  A login the server turns down, whether for a wrong password or an unknown
  user, raises a ``SQLException`` whose SQLState is ``28000``. Leaving the
  property out where the server asks for a password raises ``08004`` instead:
  the driver has nothing to send and gives up before answering, so it reports a
  connection it never established. Code that branches on the state to tell a bad
  credential from an unreachable server has to accept both.

:``sslmode``:

  How far the driver goes to secure the connection, from ``disable`` through
  ``prefer``, ``require`` and ``verify-ca`` to ``verify-full``. See the
  `pgJDBC SSL documentation`_ for what each mode checks.

  Defaults to ``prefer``: the driver asks for TLS and falls back to an
  unencrypted connection if the server does not offer it. Set it to
  ``require`` or higher to make encryption a condition of connecting.

:``ssl``:

  Setting this to ``true`` is shorthand for ``sslmode=verify-full``, which
  requires the server's certificate to be verifiable against a trusted CA
  *and* to match the hostname. A self-signed server certificate needs
  ``sslmode=require`` or a truststore instead.

:``loadBalanceHosts``:

  Whether the host strings are shuffled before a connection is attempted, so
  that connections spread across the cluster as `client-side random load
  balancing`_. Set it to ``false`` to try the hosts in the order they are
  written.

  Defaults to ``true`` for this driver, where stock pgJDBC defaults to
  ``false``.

:``assumeMinServerVersion``:

  The server version the driver assumes before it has asked, which saves a
  round trip on connect.

  Defaults to ``9.5`` for this driver.

:``connectTimeout``, ``socketTimeout``:

  Seconds to wait for a connection to be established (``10`` by default), and
  for a reply on an established one (``0``, meaning no limit, so a network that
  fails silently blocks the calling thread indefinitely).

  ``socketTimeout`` bounds a query too, by closing the connection. Use
  ``Statement.setQueryTimeout()`` to bound a statement and keep the
  connection.

.. _batching:

Writing many rows
=================

``addBatch()``/``executeBatch()`` on a ``PreparedStatement`` sends the rows
in one round trip, and is how bulk loads should be written:

.. code-block:: java

    PreparedStatement insert = conn.prepareStatement(
        "INSERT INTO t (id, name) VALUES (?, ?)");
    for (Record record : records) {
        insert.setInt(1, record.id());
        insert.setString(2, record.name());
        insert.addBatch();
    }
    int[] written = insert.executeBatch();

A batch is all or nothing. Anything in it the server rejects leaves the whole
batch unwritten, and ``executeBatch()`` raises ``BatchUpdateException`` whose
update counts are ``EXECUTE_FAILED`` throughout. Every entry has to be a
statement that writes, since a query among them fails the batch.

Values a write produced are read back through ``getGeneratedKeys()``. Name
the columns when preparing the statement and they are appended to it as a
``RETURNING`` clause:

.. code-block:: java

    PreparedStatement insert = conn.prepareStatement(
        "INSERT INTO t (id, name) VALUES (?, ?)", new String[]{"id"});
    insert.setInt(1, 1);
    insert.setString(2, "one");
    insert.execute();

    ResultSet keys = insert.getGeneratedKeys();

.. _pooling:

Data sources and connection pools
=================================

The driver works with the usual JDBC connection pools. A pool needs a
``DataSource``, and ``io.crate.client.jdbc.CrateDataSource`` is the one
carrying the CrateDB behavior. A plain ``PGSimpleDataSource`` hands out
connections without it:

.. code-block:: java

    CrateDataSource dataSource = new CrateDataSource();
    dataSource.setUrl("jdbc:crate://localhost:5432/doc");
    dataSource.setUser("crate");

    HikariConfig config = new HikariConfig();
    config.setDataSource(dataSource);
    config.setMaximumPoolSize(10);

    HikariDataSource pool = new HikariDataSource(config);

In Spring Boot, point ``spring.datasource.url`` at a ``jdbc:crate://`` URL
and set ``spring.datasource.driver-class-name`` to
``io.crate.client.jdbc.CrateDriver``.

A connection is not safe to share between threads; a pool is how an
application gives each thread one of its own.

.. _jndi:

In an application server, the data source is bound into JNDI and looked up
by name. Tomcat's ``context.xml``, for example:

.. code-block:: xml

    <Resource name="jdbc/cratedb"
              auth="Container"
              type="io.crate.client.jdbc.CrateDataSource"
              factory="io.crate.client.jdbc.CrateDataSourceFactory"
              url="jdbc:crate://localhost:5432/doc"
              user="crate"/>

The ``factory`` attribute is what matters. pgJDBC's own factory does not
answer for a CrateDB data source, so the lookup fails without it.

.. _server-version:

The CrateDB version
===================

``DatabaseMetaData`` reports the PostgreSQL release CrateDB emulates, which
is what PostgreSQL tooling reasons about. An application that needs the
CrateDB version itself asks the connection:

.. code-block:: java

    CrateVersion version = conn.unwrap(CrateConnection.class).getCrateVersion();
    if (version.atLeast(6, 2)) {
        // ...
    }

Next steps
==========

Use the standard `JDBC API`_ documentation for the rest of your setup process.
Also have a look at corresponding code :ref:`examples`.


.. _client-side random load balancing: https://en.wikipedia.org/wiki/Load_balancing_(computing)#Client-side_random_load_balancing
.. _JDBC API: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/package-summary.html
.. _pgJDBC connection property: https://jdbc.postgresql.org/documentation/use/#connection-parameters
.. _pgJDBC SSL documentation: https://jdbc.postgresql.org/documentation/ssl/
.. _URL parameters: https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html#db_connection_url
