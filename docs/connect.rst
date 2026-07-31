.. _connect:

==================
Connect to CrateDB
==================

.. _introduction:

Introduction
============

The CrateDB JDBC driver provides the ``io.crate.client.jdbc.CrateDriver``
class. JDBC 4.0 will initialise this class automatically if it is found on your
`class path`_.

.. NOTE::

    For CrateDB versions 2.1.x and later, you must configure a database user
    when connecting. Consult the `Connection Properties`_ section for more
    information.

.. SEEALSO::

    Please also consult the JDBC documentation for general information about
    how to `establish a connection using the DriverManager`_.

.. _basics:

The basics
==========

Connect to CrateDB using the ``DriverManager`` class, like so::

    Connection conn = DriverManager.getConnection("crate://localhost:5432/");

.. _database-urls:

Database connection URLs
========================

A JDBC database is represented by special type of *Uniform Resource Locator*
(URL)  called a `database connection URL`_.

The simplest database connection URL for CrateDB looks like this::

    crate://<HOST>/

Here, ``<HOST>`` is the node *host string*.

A host string looks like this::

    <HOST_ADDR>:<PORT>

Here, ``<HOST_ADDR>`` is the hostname or IP address of the CrateDB node and
``<PORT>`` is a valid :ref:`psql.port <crate-reference:conf_ports>` number.

Example host strings:

- ``localhost:5432``
- ``crate-1.vm.example.com:5432``
- ``198.51.100.1:5432``

You can specify a second CrateDB node, like so::

    crate://<HOST_ADDR_1>,<HOST_ADDR_2>/

Here, ``<HOST_ADDR_1>`` and ``<HOST_ADDR_2>`` are the host strings for the
first and second CrateDB nodes, respectively.

In fact, you can specify as many nodes as you like. Each corresponding host
string must be separated from the previous one using a ``,`` character.

The driver will attempt to connect to each node in the order they appear. The
first successul connection will be used, and all other nodes will be ignored
for the duration of that connection.

.. NOTE::

   The last host string must be followed by a ``/`` character.

.. _schema-selection:

Schema selection
================

To specify a different schema, use the ``setSchema`` method, like so:

.. code-block:: java

    Connection conn = DriverManager.getConnection("crate://localhost:5432/");
    conn.setSchema("my_schema");

.. TIP::

   The default CrateDB schema is ``doc``, and if you do not specify a schema,
   this is what will be used.

   However, you can query any schema you like by specifying it in the query.

.. _connection_properties:

Connection properties
=====================

Database connections have number of configurable properties.

Here's a simple example:

.. code-block:: java

    Properties properties = new Properties();
    properties.put("user", "crate");
    Connection conn = DriverManager.getConnection(
        "crate://localhost:5432/", properties
    );

Here, we set the ``user`` property to ``crate`` so that the driver will attempt
to connect to the CrateDB node as the ``crate`` user.

.. NOTE::

   For simplicity, we only document use of the ``Properties`` class for setting
   properties. However, you can also set properties using `URL parameters`_ if
   you wish.

The CrateDB JDBC driver supports following properties:

The driver accepts every `pgJDBC connection property`_. Properties with
CrateDB-specific defaults or meaning are listed below.

.. NOTE::

   CrateDB has no transactions. ``BEGIN`` and ``COMMIT`` are accepted by
   the server as no-ops, ``rollback()`` is a client-side no-op, and
   savepoints are not supported. Auto-commit can be disabled freely —
   which is also a prerequisite for cursor-based fetching with
   ``setFetchSize()``.

:``user``:

  Specifies the CrateDB user.

  Defaults to the same string as the OS system user.

  .. NOTE::

     Authentication was introduced in CrateDB versions 2.1.x.

     If you are using CrateDB 2.1.x or later, you must supply a username. If
     you are using earlier versions of CrateDB, this argument is not supported.

     See the :ref:`compatibility notes <cratedb-versions>` for more
     information.

     If you have not configured a custom
     :ref:`database user <crate-reference:administration_user_management>`,
     you probably want to authenticate as the CrateDB superuser, which is
     ``crate``. The superuser does not have a password, so you can omit the
     ``password`` property.

     If you are authenticating as a custom user, that user will need to have
     :ref:`DQL privileges <crate-reference:privileges-intro>` on the
     ``sys.nodes`` table, because this table is used for version negotiation.

:``password``:

  Sets the password for authentication.

:``ssl``:

  If set to ``true``, the driver will attempt to establish a secure connection
  to CrateDB using SSL. If a secure connection is not possible, no connection
  will be made.

  Defaults to ``false``.

:``loadBalanceHosts``:

  If set to ``true``, the driver will randomly shuffle the order of the host
  strings. Over multiple connection attempts, this distributes connection
  attempts across the whole cluster, functioning as `client-side random load
  balancing`_.
  If ``false``, the driver will try the hosts in the order they are defined.

  Defaults to ``true``.

Next steps
==========

Use the standard `JDBC API`_ documentation for the rest of your setup process.
Also have a look at corresponding code :ref:`examples`.


.. _class path: https://docs.oracle.com/javase/tutorial/essential/environment/paths.html
.. _client-side random load balancing: https://en.wikipedia.org/wiki/Load_balancing_(computing)#Client-side_random_load_balancing
.. _database connection URL: https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html#db_connection_url
.. _documentation: https://github.com/crate/crate-sample-apps/blob/master/java/documentation.md
.. _establish a connection using the DriverManager: https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html
.. _failover: https://en.wikipedia.org/wiki/Failover
.. _JDBC API: https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/
.. _pgJDBC connection property: https://jdbc.postgresql.org/documentation/use/#connection-parameters
.. _URL parameters: https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html#db_connection_url
.. _User Management: https://crate.io/docs/crate/reference/en/latest/sql/administration/user_management.html
