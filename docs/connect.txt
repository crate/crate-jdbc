.. _connect:

==================
Connect to CrateDB
==================

.. rubric:: Table of Contents

.. contents::
   :local:

.. _basics:

The Basics
==========

Connect to CrateDB using the ``DriverManager`` class, like so::

    Connection conn = DriverManager.getConnection("crate://localhost:5432/");

.. NOTE::

   For CrateDB versions 2.1.x and later, you must configure a database user
   when connecting.

   Consult the `Connection Properties`_ section for more information.

.. SEEALSO::

   Consult the `JDBC documentation`_ for general information about using the
   ``DriverManager`` class.

The CrateDB JDBC driver provides the ``io.crate.client.jdbc.CrateDriver``
class. JDBC 4.0 will initialise this automatically if it is found on your
`class path`_.

.. _database-urls:

Database Connection URLs
========================

A JDBC database is represented by special type of *Uniform Resource Locator*
(URL)  called a `database connection URL`_.

The simplest database connection URL for CrateDB looks like this::

    crate://<HOST>/

Here, ``<HOST>`` is the node *host string*.

A host string looks like this::

    <HOST_ADDR>:<PORT>

Here, ``<HOST_ADDR>`` is the hostname or IP address of the CrateDB node and
``<PORT>`` is a valid `psql.port`_ number.

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

Schema Selection
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

Connection Properties
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

:``strict``:

    If set to ``false``, the CrateDB JDBC driver silently ignores unsupported
    JDBC features.

    This will, for example, allow the driver to be used by most third-party
    applications that attempt to use transactional features.

    .. WARNING::

       Silently ignoring transactions may result in data corruption or data
       loss.

    If set to ``true``, the CrateDB JDBC driver behaves strictly according to
    CrateDB's capabilities and the JDBC specification.

    In strict mode, attempts to use unsupported features will result in an
    exception being raised.

    Unsupported features include:

    - `Transactions`_, e.g.:

      - Any `isolation level`_ that isn't ``TRANSACTION_NONE``

      - `Disabling auto-commit mode`_

      - `Setting and rolling back to savepoints`_

    - `Read-only connections`_

    Defaults to ``false``.

:``user``:

  Specifies the CrateDB user.

  Defaults to the same string as the OS system user.

  .. NOTE::

     Authentication was introduced in CrateDB versions 2.1.x.

     If you are using CrateDB 2.1.x or later, you must supply a username. If
     you are using earlier versions of CrateDB, this argument is not supported.

     See the :ref:`compatibility notes <cratedb-versions>` for more
     information.

     If you have not configured a custom `database user`_, you probably want to
     authenticate as the CrateDB superuser, which is ``crate``. The superuser
     does not have a password, so you can omit the ``password`` property.

     If you are authenticating as a custom user, that user will need to have
     `DQL privileges`_ on the ``sys.nodes`` table, because this table is used
     for version negotiation.

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

Next Steps
==========

Use the standard `JDBC API`_ documentation for the rest of your setup process.

.. SEEALSO::

   Check out the `sample application`_ (and the corresponding `documentation`_)
   for a practical demonstration of this driver in use.

.. _class path: https://docs.oracle.com/javase/tutorial/essential/environment/paths.html
.. _client-side random load balancing: https://en.wikipedia.org/wiki/Load_balancing_(computing)#Client-side_random_load_balancing
.. _database connection URL: https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html#db_connection_url
.. _database user: https://crate.io/docs/crate/reference/en/latest/admin/user-management.html
.. _Disabling auto-commit mode: https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html#disable_auto_commit
.. _documentation: https://github.com/crate/crate-sample-apps/blob/master/java/documentation.md
.. _DQL privileges: https://crate.io/docs/crate/reference/en/latest/admin/privileges.html#dql
.. _failover: https://en.wikipedia.org/wiki/Failover
.. _isolation level: https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html#transactions_data_integrity
.. _JDBC API: https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/
.. _JDBC documentation: https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html
.. _psql.port: https://crate.io/docs/crate/reference/en/latest/config/node.html#ports
.. _Read-only connections: https://docs.oracle.com/javase/7/docs/api/java/sql/Connection.html#setReadOnly(boolean)
.. _sample application: https://github.com/crate/crate-sample-apps/tree/master/java
.. _schema: https://crate.io/docs/crate/reference/en/latest/sql/statements/create-table.html#description
.. _Setting and rolling back to savepoints: https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html#set_roll_back_savepoints
.. _Transactions: https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html
.. _URL parameters: https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html#db_connection_url
.. _User Management: https://crate.io/docs/crate/reference/en/latest/sql/administration/user_management.html
