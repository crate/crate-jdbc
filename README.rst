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
As a workaround, you can build the JDBC driver jar using `Jitpack`_ or
build it from source.

Installation
============

.. note::

   Crate JDBC driver 1.x requires a Crate version equal or greater than 0.38.0
   but lower than 0.56.0.
   Crate JDBC driver 2.x requires a Crate version equal or greater than 0.56.0.

Build JAR from source
---------------------

Clone the repo::

  git clone --recursive https://github.com/crate/crate-jdbc
  cd crate-jdbc

and build a JAR::

   ./gradlew jar

or build a jar including all dependencies::

   ./gradlew standaloneJar

Afterwards a JAR file of the current version exists under ``build/lib``.


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
.. _Jitpack: https://jitpack.io/#crate/crate-jdbc
