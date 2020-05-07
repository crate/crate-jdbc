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

Consult the `compatibility notes`_ for additional information.

Install
=======

.. NOTE::

   These instructions show you how to do a conventional install.

   To build the CrateDB JDBC driver from the source code, follow the
   `instructions on GitHub`_.

There are two ways to install the driver.

The regular CrateDB JDBC driver JAR files are hosted on `Bintray`_ and are
available via `JCenter`_.

Alternatively, you can download a single, standalone JAR file that bundles the
driver dependencies. Use the `Bintray file navigator`_ to locate the version you
want and download manually.

.. CAUTION::

   The standalone JAR file should not be used in a Maven project. It does,
   however, function nicely as a plugin for tools such as `SQuirreL`_.

Set up as a dependency
======================

This section shows you how to set up the CrateDB JDBC driver as a
dependency using Maven or Gradle; two popular build tools for Java projects.

.. SEEALSO::

   Select the blue *SET ME UP!* button located in the top right-hand corner of
   the `Bintray overview page`_ for supplementary instructions.

Maven
-----

If you're using `Maven`_, you first need to add the Bintray repository to your
``pom.xml`` file:

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

Then add ``crate-jdb`` ass a dependency, like so:

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

If you're using `Gradle`_, you first need to add the JCenter repository to your
``build.gradle`` file:

.. code-block:: groovy

    repositories {
        ...
        jcenter()
    }

Then add ``crate-jdb`` as a dependency, like so:

.. code-block:: groovy

    dependencies {
        compile 'io.crate:crate-jdbc:...'
        ...
    }

Next steps
==========

Once the JDBC driver is set up, you probably want to :ref:`connect to CrateDB
<connect>`.

.. _Bintray file navigator: https://bintray.com/crate/crate/crate-jdbc/view/files/io/crate/crate-jdbc-standalone
.. _Bintray overview page: https://bintray.com/crate/crate/crate-jdbc
.. _Bintray: https://bintray.com/crate/crate/crate-jdbc
.. _compatibility notes: https://crate.io/docs/clients/jdbc/en/latest/compatibility.html
.. _Gradle: https://gradle.org/
.. _instructions on GitHub: https://github.com/crate/crate-jdbc/
.. _Java 8: http://www.oracle.com/technetwork/java/javase/downloads/index.html
.. _JCenter: https://bintray.com/bintray/jcenter
.. _Maven: https://maven.apache.org/
.. _OpenJDK: http://openjdk.java.net/projects/jdk8/
.. _Oracle’s Java: http://www.java.com/en/download/help/mac_install.xml
.. _SQuirreL: https://crate.io/a/use-cratedb-squirrel-basic-java-desktop-client/
