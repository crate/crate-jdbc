============
Installation
============

Learn how to install and get started with the :ref:`CrateDB JDBC driver
<index>`.

Prerequisites
=============

The CrateDB JDBC driver requires Java 11 or later and CrateDB 6.0 or
later. For older CrateDB versions, use crate-jdbc 2.7.0.

Install
=======

The driver comes in two variants, both published to Maven Central: use
`crate-jdbc`_ where a build tool resolves dependencies, and
`crate-jdbc-standalone`_ where a single jar is required. :ref:`internals`
describes what each contains.

As a dependency
---------------

With Maven:

.. code-block:: xml

    <dependencies>
        <dependency>
            <groupId>io.crate</groupId>
            <artifactId>crate-jdbc</artifactId>
            <version>3.0.0</version>
        </dependency>
    </dependencies>

With `Gradle`_, from the Maven Central repository:

.. code-block:: groovy

    repositories {
        mavenCentral()
    }

    dependencies {
        implementation 'io.crate:crate-jdbc:3.0.0'
    }

.. _standalone-jar:

In a database tool
------------------

Tools such as `Apache Hop`_, Pentaho, `DBeaver`_ and `SQuirreL`_ load a driver
from a directory of jars instead of resolving it. Download
`crate-jdbc-standalone`_ from Maven Central, drop the jar in, and register the
driver with:

:Driver class: ``io.crate.client.jdbc.CrateDriver``
:URL template: ``jdbc:crate://<host>:5432/<schema>``
:Default port: ``5432``

The standalone jar bundles pgJDBC under a namespace of its own, so it can sit
next to a PostgreSQL driver in the same directory without either shadowing the
other. It is not meant for use as a build dependency.

.. SEEALSO::

   To build the CrateDB JDBC driver from the source code, follow the
   `developer guide`_.

Next steps
==========

With the driver set up, the next step is to :ref:`connect to CrateDB
<connect>`.


.. _crate-jdbc: https://central.sonatype.com/artifact/io.crate/crate-jdbc
.. _crate-jdbc-standalone: https://central.sonatype.com/artifact/io.crate/crate-jdbc-standalone
.. _developer guide: https://github.com/crate/crate-jdbc/blob/master/DEVELOP.rst
.. _Apache Hop: https://hop.apache.org/
.. _DBeaver: https://dbeaver.io/about/
.. _Gradle: https://gradle.org/
.. _SQuirreL: http://www.squirrelsql.org
