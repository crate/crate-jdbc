===============
Getting started
===============

Learn how to install and get started with the :ref:`CrateDB JDBC driver
<index>`.

.. rubric:: Table of contents

.. contents::
   :local:

Prerequisites
=============

The CrateDB JDBC driver requires `Java 8`_, preferably update 20 or later. We
recommend using `Oracle’s Java`_ on macOS and `OpenJDK`_ on Linux Systems.

Consult the :ref:`compatibility notes <compatibility>` for additional information.

Install
=======

Following the sunsetting of Bintray/JCenter, `crate-jdbc`_ has moved to Maven Central.
Versions < 2.6.0 will not be migrated. If you are using an older version, please
consider upgrading, or building the artifacts manually.

.. NOTE::

   These instructions show you how to do a conventional install.

   To build the CrateDB JDBC driver from the source code, follow the
   `instructions on GitHub`_.

There are two ways to install the driver.

The regular CrateDB JDBC driver JAR files `crate-jdbc`_ are hosted on Maven Central.

Alternatively, you can download a single, standalone JAR file that bundles the
driver dependencies, called `crate-jdbc-standalone`_.

.. CAUTION::

   The standalone JAR file should not be used in a Maven project. It does,
   however, function nicely as a plugin for tools such as `SQuirreL`_.

Set up as a dependency
======================

This section shows you how to set up the CrateDB JDBC driver as a
dependency using Maven or Gradle; two popular build tools for Java projects.

Maven
-----

Add ``crate-jdbc`` as a dependency, like so:

.. code-block:: xml

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

Gradle
------

If you're using `Gradle`_, you first need to add the Maven Central repository to your
``build.gradle`` file:

.. code-block:: groovy

    repositories {
        ...
        mavenCentral()
    }

Then add ``crate-jdbc`` as a dependency, like so:

.. code-block:: groovy

    dependencies {
        compile 'io.crate:crate-jdbc:...'
        ...
    }

Next steps
==========

Once the JDBC driver is set up, you probably want to :ref:`connect to CrateDB
<connect>`.

.. _crate-jdbc: https://repo1.maven.org/maven2/io/crate/crate-jdbc/
.. _crate-jdbc-standalone: https://repo1.maven.org/maven2/io/crate/crate-jdbc-standalone/
.. _Gradle: https://gradle.org/
.. _instructions on GitHub: https://github.com/crate/crate-jdbc/
.. _Java 8: http://www.oracle.com/technetwork/java/javase/downloads/index.html
.. _OpenJDK: http://openjdk.java.net/projects/jdk8/
.. _Oracle’s Java: http://www.java.com/en/download/help/mac_install.xml
.. _SQuirreL: https://crate.io/a/use-cratedb-squirrel-basic-java-desktop-client/
