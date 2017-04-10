===================
CrateDB JDBC Driver
===================

.. image:: https://travis-ci.org/crate/crate-jdbc.svg?branch=master
        :target: https://travis-ci.org/crate/crate-jdbc
        :alt: Test

|

A JDBC_ driver for `CrateDB`_.

Currently, we do not provide the testing version of the Crate JDBC driver.
As a workaround, you can build the JDBC driver jar using `Jitpack`_, or
build it from source.

Prerequisites
=============

The CrateDB JDBC driver 2.x requires a CrateDB version greater than or equal to
0.56.

The CrateDB JDBC driver 1.x requires a CrateDB version less than 0.56 but
greater than or equal to 0.38.

Installation
============

Clone the repository::

    $ git clone --recursive https://github.com/crate/crate-jdbc

Change directory into the repository::

    $ cd crate-jdbc

Build a regular JAR file::

    $ ./gradlew jar

Or, build a JAR file that includes dependencies::

    $ ./gradlew standaloneJar

Afterwards you can find the JAR file in the ``build/lib`` directory.

Contributing
============

This project is primarily maintained by Crate.io_, but we welcome community
contributions!

See the `developer docs`_ and the `contribution docs`_ for more information.

Help
====

Looking for more help?

- Read `the project documentation`_
- Check `StackOverflow`_ for common problems
- Chat with us on `Slack`_
- Get `paid support`_

.. _contribution docs: CONTRIBUTING.rst
.. _Crate.io: http://crate.io/
.. _CrateDB: https://github.com/crate/crate
.. _developer docs: DEVELOP.rst
.. _JDBC: http://www.oracle.com/technetwork/java/overview-141217.html
.. _Jitpack: https://jitpack.io/#crate/crate-jdbc
.. _paid support: https://crate.io/pricing/
.. _Slack: https://crate.io/docs/support/slackin/
.. _StackOverflow: https://stackoverflow.com/tags/crate
.. _the project documentation: https://crate.io/docs/projects/crate-jdbc/
