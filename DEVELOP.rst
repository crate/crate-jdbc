===============
Developer guide
===============

These instructions show you how to build the CrateDB JDBC driver from the
source code, and how to invoke the test suite. For a conventional install
(using pre-built JAR files), follow the `installation documentation`_.
`AGENTS.md`_ describes how the repository is laid out and the constraints to
honor when changing it.


Acquire source
==============

Clone the repository::

    $ git clone https://github.com/crate/crate-jdbc

Change directory into the repository::

    $ cd crate-jdbc

Building
========

This project builds with Gradle_, which the ``Makefile`` in the repository
root wraps: every workflow below has a target, and ``make`` on its own lists
them all. Gradle downloads and bootstraps itself the first time a target
runs it.

Build both JAR files into ``build/libs``::

    $ make build

Building requires the environment locale set to ``UTF-8``.

Publish both artifacts to the local Maven repository, to try the packaging
an application will resolve::

    $ make publish-local

The bare ``./gradlew publish`` task does something else. The Sonatype plugin
registers Maven Central as a publishing repository, so ``publish`` targets
it.

Signing is skipped unless a key is configured, through the environment
variables the release workflow passes:

 - ``ORG_GRADLE_PROJECT_signingKey``       — the private key, in ascii format
 - ``ORG_GRADLE_PROJECT_signingPassword``  — the password for that key


Testing
=======

The unit tests need no server and no Docker::

    $ make test

Gradle runs on JDK 17 or later while the driver is built for Java 11, so the
baseline is exercised by launching a suite on a Java 11 toolchain instead of on
the JVM running the build. Gradle resolves that toolchain, downloading one if
the machine has none::

    $ make test-baseline

Integration tests boot a CrateDB server in Docker via Testcontainers_, so a
running Docker daemon is required::

    $ make itest
    $ make itest-floor   # on the oldest supported CrateDB and JRE

Select another server with the ``CRATEDB_VERSION`` (a tag of the ``crate``
image) or ``CRATEDB_IMAGE`` (a full image reference) environment variable::

    $ CRATEDB_VERSION=6.2.2 make itest
    $ CRATEDB_IMAGE=crate/crate:nightly make itest

To run against an externally managed server instead, point ``CRATE_URL``
at a full JDBC URL; no container is started then::

    $ CRATE_URL=crate://localhost:5432/doc?user=crate make itest

Two axes of the substrate change what the suites can see, and both have a
target of their own. A cluster is where the driver's load balancing and its
cancel routing stop being inert, and a JVM away from UTC is the only place a
conversion through the default calendar and one without it disagree::

    $ make itest-cluster   # three nodes instead of one
    $ make itest-zoned     # the suites in Europe/Berlin

Gradle enables assertions in its test JVMs, where applications run without
them. pgJDBC guards a request that ``preferQueryMode=simple`` cannot carry with
an assertion of its own, so the outcome a plain JVM meets, the request falling
through and executing the statement, is reachable only with assertions off::

    $ make itest-noassert

The tests that apply to one setting and not the other skip themselves under
the other, so both runs report skips.

Coverage is measured over the hand-written classes. The generated
``Forwarding*`` classes are left out, being delegation the build already
verifies against a fresh generation. It runs off by default, so no other run
carries the agent::

    $ make coverage

The report lands in ``build/reports/jacoco/test``.

Coverage counts the lines a run reached and says nothing about whether a test
would object to one of them being wrong. That is a separate run. It changes a
line (an operator, a constant, a returned value) and reruns the tests that
covered it, reporting the changes nothing failed on::

    $ CRATE_URL=crate://localhost:5432/doc?user=crate make mutation

The server has to come from outside. Each batch of changes is tried in a JVM of
its own, and a run booting a container per JVM would spend its time on
containers. Narrow it to one class with ``-PmutationClasses`` and
``-PmutationTests``, to ask that class again once a gap the report found is
closed. The report lands in ``build/reports/pitest``.

The published artifacts come in two shapes, and the suites run against the
plain one. The standalone artifact carries pgJDBC and Jackson relocated under
``io.crate.shade``, where a name that failed to relocate fails only once used,
so the suites run against that jar too, with the ordinary pgJDBC kept off the
classpath::

    $ make itest-standalone

A suite that names a pgJDBC type by its plain package cannot run there, since
under that artifact the type has another name. Such a suite carries the
``pgjdbc-types`` tag and is left out of this run.

``make check`` runs the unit tests together with the code style and the
checks on the artifacts: the contents of the standalone jar and its behavior
on each classpath it lands on, and the generated forwarding classes against a
fresh generation. ``make verify`` runs the checks and both suites across the
supported server and JRE ranges, which is what the release workflow gates on.

Generated sources
=================

The ``Forwarding*`` classes under ``driver/main/java`` are generated from
the JDBC and pgJDBC interfaces by ``devtools/GenerateForwarding.java``.
Regenerate them, against the pgJDBC version the build pins, with::

    $ make forwarding

``make check`` fails when the checked-in classes differ from a fresh
generation, which is what a newer JDBC release adding methods looks like.

Upgrading pgJDBC
================

Change ``ext.pgjdbcVersion`` in ``build.gradle``, regenerate the forwarding
classes as above, and run the tests. The version is also written into the
standalone jar's manifest as ``Bundled-PgJdbc-Version``.

Preparing a release
===================

To cut a release:

- Set the release version in ``gradle.properties``; it is the one place the
  version lives, and the driver reports it through ``DatabaseMetaData``

- Move the ``Unreleased`` notes in ``CHANGES.txt`` under a heading for the
  new version, dated (``YYYY/MM/DD x.y.z``) — both the tagging script and
  the release workflow look for that form

- Commit your changes with a message like "prepare release x.x.x"

- Push to origin

- Create a tag by running ``make tag``, which refuses anything the
  release workflow would reject once the tag is public

- Cut the ``x.y`` branch from the tag, which is where fixes for that minor
  land and which CI builds like ``master``

- Set the version in ``gradle.properties`` to the next ``x.y.z-SNAPSHOT``, so
  that what follows is not built as a released version

- Archive docs for old releases (see section below)

Pushing the tag runs the ``Release`` workflow, which verifies the tag
against the project version, runs the tests, and publishes to Maven Central.

To publish by hand instead::

    $ ./gradlew clean publishToSonatype closeAndReleaseSonatypeStagingRepository

That needs the signing key and password from Building_, plus the Sonatype
token as ``-PsonatypeTokenUsername`` and ``-PsonatypeTokenPassword`` (or the
matching ``ORG_GRADLE_PROJECT_`` environment variables).

Archiving docs versions
-----------------------

Check the `versions hosted on ReadTheDocs`_.

Only three kinds of version stay hosted: ``latest``, the minor release branches
of the current major release, and the last minor release branch of the previous
one. Today that is ``latest``, ``3.0`` and ``2.7``.

Activating or deactivating a version is an RTD configuration change: ask the
`@crate/docs`_ team.

Writing documentation
=====================

The docs live under the ``docs`` directory, written in ReStructuredText_ and
processed with Sphinx_. Build them with::

    cd docs
    make html
    open .crate-docs/.build/index.html

``make check`` there runs the same build the CI does, together with the link
and prose checks.

`Read the Docs`_ builds the published docs from Git on every push.

.. _@crate/docs: https://github.com/orgs/crate/teams/docs
.. _Gradle: https://gradle.org/
.. _Testcontainers: https://java.testcontainers.org/
.. _AGENTS.md: AGENTS.md
.. _installation documentation: https://cratedb.com/docs/jdbc/en/latest/getting-started.html
.. _ReStructuredText: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx-doc.org/
.. _Read the Docs: http://readthedocs.org/
.. _versions hosted on ReadTheDocs: https://readthedocs.org/projects/crate-jdbc/versions/
