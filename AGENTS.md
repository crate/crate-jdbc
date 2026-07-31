# Working on the CrateDB JDBC driver

A type-4 JDBC driver for CrateDB. It is a thin adaptation layer over the stock
[pgJDBC](https://jdbc.postgresql.org/) driver, which it consumes as a regular
dependency — it holds only the behaviors where CrateDB differs from PostgreSQL.
`docs/internals.rst` describes the architecture; read it before changing the
wrapper layer.

## Layout

| Path | What lives there |
| --- | --- |
| `driver/main/java/io/crate/client/jdbc/Crate*.java` | Hand-written behavior. This is where CrateDB-specific logic belongs. |
| `driver/main/java/io/crate/client/jdbc/Forwarding*.java` | **Generated.** Do not hand-edit — see below. |
| `driver/test/java/…/*Test.java` | Unit tests. No server, no Docker. |
| `driver/test/java/…/integrationtests/*IT.java` | Integration tests against a real CrateDB. |
| `devtools/` | Source-generation and artifact-verification tools, run from the build. |
| `docs/` | Sphinx documentation published to cratedb.com. |

## The testing layer

Tests are split by what they need, so the fast ones stay usable:

```sh
./gradlew test              # unit tests only — no Docker, runs in about a second
./gradlew integrationTest   # boots CrateDB in Docker via Testcontainers
./gradlew check             # unit tests + Spotless + the generated-code and artifact checks
```

`check` deliberately excludes `integrationTest`; run both before calling work
done. The `Makefile` wraps these — `make` lists what it offers, and
`make verify` runs the checks and both test suites across the supported
server and JRE ranges.

Gradle needs a JDK 17 or later to run, while the driver is built for Java 11.
`-PtestJavaVersion=11` runs a test task on a Java 11 toolchain instead of on
the build's own JDK, which is how the baseline gets exercised rather than
only compiled for (`make test-baseline`, and every cell of the CI matrix).
Gradle resolves that toolchain, fetching one if the machine has none.

Integration tests extend `BaseIntegrationTest`, which starts one container per
test JVM and connects through the `crate://` scheme — that is, through this
driver rather than through pgJDBC directly. Point them elsewhere with
environment variables:

| Variable | Effect |
| --- | --- |
| `CRATEDB_VERSION` | Tag of the `crate` image to boot. |
| `CRATEDB_IMAGE` | Full image reference, for `crate/crate:nightly`. |
| `CRATEDB_NODES` | Boot a cluster of that many nodes instead of one. |
| `CRATE_URL` | Use an already-running server and start no container. |

`-PtestTimeZone=Europe/Berlin` is the other axis of the substrate, and the one
easiest to forget: the test tasks otherwise pin `user.timezone=UTC`, where a
conversion that goes through the JVM's default calendar and one that does not
give the same answer. Every path that reads or writes a moment through an array
goes through that calendar, and a fault in one is invisible from inside UTC. A
rule about a moment also has to read what the server stored — `select
column::bigint` — rather than what the driver reads back, or an error on the
way in and its mirror on the way out cancel and the round trip hides both.

`CRATEDB_NODES` is the topology axis, and it is where a set of the driver's
decisions stops being inert. `loadBalanceHosts` is on unless a caller turns it
off and has nothing to balance over on one node; the whole reason a query
timeout is handed to the server as `statement_timeout` is that pgJDBC delivers
one as a cancel request on a second connection, which a URL naming three nodes
will point wherever the balancing lands. `CrateDBCluster` starts the nodes on a
Docker network of their own and waits for `sys.nodes` to report them all —
each container answers HTTP long before it has found the others, so its own
wait strategy proves nothing. `ClusterIT` starts a cluster of its own rather
than using that variable, so the cases only several nodes can reach run
wherever the integration tests run; its first test asserts the cluster formed
at all, because three nodes that came up as three clusters of one would let
everything after it pass while measuring nothing.

Each integration test class covers one area — connections, statements, types,
metadata, batching, cursors, transactions, pooling, concurrency, vector search
— and pins behavior a user could depend on. The driver serves a range of
servers, so a test of behavior a later CrateDB introduced guards itself with
`serverAtLeast(major, minor)` rather than describing only the newest release.
Beyond the JUnit suites, `check` also runs:

- `verifyForwardingClasses` — regenerates the `Forwarding*` classes and fails
  if the checked-in ones differ.
- `verifyArtifacts` — inspects the shaded jar's entries, then loads each
  artifact on the classpaths it lands on. Three things vary and each decides
  what registers with the `DriverManager`: the artifact, the arrangement
  (system classpath or a plugin class loader of its own), and which class an
  application touches first (a URL lookup, a data source, the driver class by
  name). The two artifacts owe opposite things — the standalone's bundled
  pgJDBC must never answer a `jdbc:postgresql://` URL, and the thin artifact's
  is the application's own and must always keep answering it. The plugin loader
  is the arrangement the service entry cannot carry: the `DriverManager` scans
  for services with the thread context class loader, which does not reach into
  a plugin loader.
- `WrapperCompletenessTest` — reflects over the JDBC interfaces and fails if
  any method would hand out a raw pgJDBC object instead of a wrapper. A future
  JDBC release adding methods surfaces here.

Pin what the driver decides, not what pgJDBC does. A metadata answer or a
protocol behavior the driver forwards untouched can only change with pgJDBC,
so a test holding it fails the suite for reasons that have nothing to do with
CrateDB.

When you fix a defect, pin it with a test that states the invariant
("rejects an expired token"), not the defect. Prefer extending a table-driven
test over adding a near-duplicate one.

## Keeping in sync with CrateDB

The driver trails a moving server. When a CrateDB release lands, when you are
auditing driver/server drift, or when someone reports a type or metadata gap,
follow the playbook in
[`.claude/skills/sync-with-crate/SKILL.md`](.claude/skills/sync-with-crate/SKILL.md).
It records where to look in `crate/crate`, what the driver owes each kind of
server change, and how to measure behavior instead of guessing at it.

That file is the single source for this procedure, and this one is the single
source for the rest; `CLAUDE.md` points here rather than restating either.

## Things that will bite you

- **`Forwarding*.java` are generated** by `devtools/GenerateForwarding.java`.
  Change the generator and regenerate with `make forwarding`; a hand-edit fails
  `check`. Regenerate that way rather than by launching the generator yourself:
  it reflects over `java.sql`, whose interfaces grow from one release to the
  next, so the build pins the JDK it runs on and `NOT_IN_RELEASE_11` holds the
  output to what the driver's own `--release 11` compile accepts. Where
  `java.sql` nests one interface in another, generated plumbing and
  hand-written behavior alternate: `CrateStatement` →
  `ForwardingPreparedStatement` → `CratePreparedStatement` →
  `ForwardingCallableStatement` → `CrateCallableStatement`. Each wrapper
  inherits everything the one below it does and adds only what its own
  interface brings, so behavior such as bracketing an execution with the
  query timeout is written once and holds for all three.
- **Two artifacts.** `crate-jdbc` is thin, with pgJDBC as a visible transitive
  dependency. `crate-jdbc-standalone` bundles everything relocated under
  `io.crate.shade`, carries the bundled licenses, and deliberately keeps its
  pgJDBC out of the `DriverManager`. Code reached from the DataSource must not
  route through `DriverManager`, or it breaks in that artifact only.
- **CrateDB is not PostgreSQL.** It has no transactions, `ROLLBACK`, or
  savepoints, and its `pg_catalog` is partial. Verify against a running server
  rather than reasoning from PostgreSQL behavior — the container makes that
  cheap.
- **Minimum server is CrateDB 6.0**, which is where `current_catalog` arrived;
  pgJDBC's metadata queries need it.
- Documentation is built with `cd docs && make html`, checked with `make check`.
- `CHANGES.txt` is the one place that narrates change. Entries stay under
  `Unreleased` until a release dates the heading.

Everything left in the tree — comments, names, docstrings, test titles — should
read as though the code had always been that way, describing what it is rather
than what was changed.

## Rules for AI Agents

- **Do not raise findings based on static analysis alone.** You must write, compile,
  and execute a JUnit test in this workspace that empirically proves a failure before
  flagging it as a bug or architectural hole.
