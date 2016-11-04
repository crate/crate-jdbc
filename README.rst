.. image:: https://cdn.crate.io/web/2.0/img/crate-avatar_100x100.png
   :width: 100px
   :height: 100px
   :alt: Crate.IO
   :target: https://crate.io

.. image:: https://travis-ci.org/crate/crate-jdbc.svg?branch=master
        :target: https://travis-ci.org/crate/crate-jdbc
        :alt: Test

=================
Crate JDBC Driver
=================

.. highlight:: java

This is the JDBC driver for `Crate`_.

Currently we don't provide the testing version of the Crate JDBC driver.
Therefore, using the driver with the latest testing releases of the Crate
might not be possible due compatibility issues. As a workaround, you can build
the JDBC driver jar from sources. To build the Crate JDBC driver you would need
to replace the ``crate-client`` version in the ``build.gradle`` file, adopt sources
to make it compile, if needed and follow instructions in `Build JAR from source`_.

Installation
============

.. note::

   Crate JDBC driver 1.x requires a Crate version greater or equal than ``0.38.0``
   but lower than ``0.57``.
   Crate JDBC driver 2.x requires a Crate version greater or equal than ``0.57.0``.

Build JAR from source
---------------------

Clone the repo::

  git clone https://github.com/crate/crate-jdbc
  cd crate-jdbc

and build a JAR including all dependencies::

   ./gradlew jar

or::

   ./gradlew jarStandalone

Afterwards a JAR file of the current version exists under ``build/lib``.


Version 2.x
===========

Version 2 of the Crate JDBC driver is using the `PostgreSQL Wire Protocol`_ and
is currently only intended for testing purposes.
It is only available via `Jitpack`_ and won't be available on `Bintray`_ until
Crate version ``0.57.x`` is released.

Differences to version 1.x are:

- The Crate specific ``strict`` parameter is not supported.
- The `connection string`_ changed to the same format as PostgreSQL JDBC,
  which means that setting the default schema with the connection string is not
  supported. Use ``setDefaultSchema()`` instead.
- Support for the experimentaal ``showsubcolumns`` parameter has been dropped.


Documentation
=============

For the documentation of the latest stable version visit
https://crate.io/docs/projects/crate-jdbc/

Help & Contact
==============

Do you have any questions? Or suggestions? We would be very happy
to help you. So, feel free to swing by our support channel on Slack_.
Or for further information and official contact please
visit `https://crate.io/ <https://crate.io/>`_.

.. _Slack: https://crate.io/docs/support/slackin/

License
=======

Copyright 2013-2014 CRATE Technology GmbH ("Crate")

Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
license agreements.  See the NOTICE file distributed with this work for
additional information regarding copyright ownership.  Crate licenses
this file to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.  You may
obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
License for the specific language governing permissions and limitations
under the License.

However, if you have executed another commercial license agreement
with Crate these terms will supersede the license and you may use the
software solely pursuant to the terms of the relevant commercial agreement.


.. _Crate: https://github.com/crate/crate
.. _`PostgreSQL Wire Protocol`: https://www.postgresql.org/docs/current/static/protocol.html
.. _Bintray: https://bintray.com/crate/crate/crate-jdbc
.. _Jitpack: https://jitpack.io/#crate/crate-jdbc
.. _`connection string`: https://jdbc.postgresql.org/documentation/80/connect.html
