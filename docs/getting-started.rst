============
Installation
============

Learn how to install and get started with the :ref:`CrateDB JDBC driver
<index>`.

Prerequisites
=============

The CrateDB JDBC driver requires Java 8, preferably update 20 or later. We
recommend using `Oracle’s Java`_ on macOS and `OpenJDK`_ on Linux Systems.

Install
=======

The driver comes in two variants, available on Maven Central at the
`repository root folder`_.

- `crate-jdbc`_

  The driver JAR, suitable to be used as a dependency in your Maven or
  Gradle project.

- `crate-jdbc-standalone`_

  A single, standalone JAR file, that bundles all the
  driver dependencies, suitable to be used as a plugin for tools such as
  `DataGrip`_, `DBeaver`_, `SQuirreL`_, etc.. This variant should not be
  used as a dependency in a Maven or Gradle project.

.. SEEALSO::

   To build the CrateDB JDBC driver from the source code, follow the
   `developer guide`_.

Set up as a dependency
======================

This section shows you how to set up the CrateDB JDBC driver as a
dependency using Maven or Gradle, two popular build tools for Java projects.

Maven
-----

Add ``crate-jdbc`` as a dependency, like so:

.. code-block:: xml

    <dependencies>
        <dependency>
            <groupId>io.crate</groupId>
            <artifactId>crate-jdbc</artifactId>
            <version>2.7.0</version>
        </dependency>
    </dependencies>

Gradle
------

If you're using `Gradle`_, you will need to add the Maven Central repository to your
``build.gradle`` file:

.. code-block:: groovy

    repositories {
        mavenCentral()
    }

Then, add ``crate-jdbc`` as a dependency:

.. code-block:: groovy

    dependencies {
        implementation 'io.crate:crate-jdbc:2.7.0'
    }

Next steps
==========

Once the JDBC driver is set up, you probably want to :ref:`connect to CrateDB
<connect>`.


.. _crate-jdbc: https://repo1.maven.org/maven2/io/crate/crate-jdbc/
.. _crate-jdbc-standalone: https://repo1.maven.org/maven2/io/crate/crate-jdbc-standalone/
.. _developer guide: https://github.com/crate/crate-jdbc/blob/master/DEVELOP.rst
.. _DataGrip: https://www.jetbrains.com/datagrip/
.. _DBeaver: https://dbeaver.io/about/
.. _Gradle: https://gradle.org/
.. _instructions on GitHub: https://github.com/crate/crate-jdbc
.. _OpenJDK: https://openjdk.org/
.. _Oracle’s Java: https://www.oracle.com/java/technologies/downloads/
.. _repository root folder: https://repo1.maven.org/maven2/io/crate/
.. _SQuirreL: https://crate.io/blog/use-cratedb-squirrel-basic-java-desktop-client
