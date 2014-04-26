.. image:: https://cdn.crate.io/web/1.0.0/img/logo-solid.png
   :width: 155px
   :height: 45px
   :alt: Crate
   :target: https://crate.io

========================
 Crate Data Java Client
========================

.. highlight:: java

This is the Java client library for `Crate Data`_ using the transport
protocol. The library exposes a very simply interface to query Crate
using SQL.

Installation
============

Build JAR including dependencies from source
--------------------------------------------

Clone the repo::

  git clone https://github.com/crate/crate-java
  cd crate-java

and build a JAR including all dependencies::

   ./gradlew jar

Afterwards a JAR file of the current version exists under ``build/lib``.

Build JAR without dependencies from source
------------------------------------------

Clone the repo::

  git clone https://github.com/crate/crate-java
  cd crate-java

and build a JAR without dependencies::

   ./gradlew jarNoDeps

Afterwards a JAR file of the current version exists under ``build/lib``.

Usage
=====

A minimal example is just a few lines of code::

    import io.crate.client.CrateClient;

    CrateClient client = new CrateClient("server1.crate.org:4300", "server2.crate.org:4300");
    SQLResponse r = client.sql("select firstName, lastName from users").actionGet();

    System.out.println(Arrays.toString(r.cols()));
    // outputs ["firstName", "lastName"]

    for (Object[] row: r.rows()){
        System.out.println(Arrays.toString(row));
    }
    // outputs the users. For example:
    // ["Arthur", "Dent"]
    // ["Ford", "Perfect"]

The `CrateClient` takes multiple servers as arguments. They are used in a
round-robin fashion to distribute the load. In case a server is unavailable it
will be skipped.

Queries are executed asynchronous. `client.sql("")` will return a
`Future<SQLResponse>` and code execution is only blocked if `.actionGet()` is
called on it.

Help & Contact
==============

Do you have any questions? Or suggestions? We would be very happy
to help you. So, feel free to swing by our IRC channel #crate on Freenode_.
Or for further information and official contact please
visit `https://crate.io/ <https://crate.io/>`_.

.. _Freenode: http://freenode.net

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



.. _Crate Data: https://github.com/crate/crate
