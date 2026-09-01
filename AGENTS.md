# Working on the CrateDB JDBC driver

A type-4 JDBC driver for CrateDB. It is a thin adaptation layer over the stock
[pgJDBC](https://jdbc.postgresql.org/) driver, consumed as a regular dependency,
and holds only the behaviors where CrateDB differs from PostgreSQL.
`docs/internals.rst` describes the architecture; read it before changing the
wrapper layer.

## Layout

| Path | What lives there |
| --- | --- |
| `driver/main/java/io/crate/client/jdbc/Crate*.java` | The wrappers: CrateDB-specific behavior, over a folded block forwarding the rest to pgJDBC. |
| `driver/test/java/…/*Test.java` | Unit tests. No server, no Docker. |
| `driver/test/java/…/integrationtests/*IT.java` | Integration tests against a real CrateDB. |
| `devtools/` | Artifact-verification and release tools, run from the build. |
| `docs/` | Sphinx documentation published to cratedb.com. |

## The testing layer

Tests are split by what they need, so the fast ones stay usable:

```sh
./gradlew test              # unit tests only — no Docker, runs in about a second
./gradlew integrationTest   # boots CrateDB in Docker via Testcontainers
./gradlew check             # unit tests + Spotless + the artifact checks
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
test JVM and connects through the `crate://` scheme, so through this driver
rather than through pgJDBC directly. Point them elsewhere with environment
variables:

| Variable | Effect |
| --- | --- |
| `CRATEDB_VERSION` | Tag of the `crate` image to boot. |
| `CRATEDB_IMAGE` | Full image reference, for `crate/crate:nightly`. |
| `CRATEDB_NODES` | Boot a cluster of that many nodes instead of one. |
| `CRATE_URL` | Use an already-running server and start no container. |

`-PtestTimeZone=Europe/Berlin` is the other axis of the substrate, and the one
easiest to forget. The test tasks otherwise pin `user.timezone=UTC`, where a
conversion through the JVM's default calendar and one without it agree. Every
path that reads or writes a moment through an array goes through that calendar,
so a fault in one is invisible from inside UTC. A test about a moment also has
to read what the server stored (`select column::bigint`) instead of what the
driver reads back, or an error on the way in and its mirror on the way out
cancel and the round trip hides both.

`CRATEDB_NODES` is the topology axis, and where several of the driver's
decisions stop being inert. `loadBalanceHosts` is on unless a caller turns it
off, and has nothing to balance over on one node. A query timeout is handed to
the server as `statement_timeout` because pgJDBC delivers one as a cancel
request on a second connection, which a URL naming three nodes points wherever
the balancing lands. `CrateDBCluster` starts the nodes on a Docker network of
their own and waits for `sys.nodes` to report them all: each container answers
HTTP long before it has found the others, so its own wait strategy proves
nothing. `ClusterIT` starts a cluster of its own instead of reading that
variable, so the cases only several nodes can reach run wherever the integration
tests do. Its first test asserts the cluster formed at all, since three nodes
that came up as three clusters of one would let everything after it pass while
measuring nothing.

Each integration test class covers one area (connections, statements, types,
metadata, batching, cursors, transactions, pooling, concurrency, vector search)
and pins behavior a user could depend on. The driver serves a range of
servers, so a test of behavior a later CrateDB introduced guards itself with
`serverAtLeast(major, minor)` rather than describing only the newest release.
Beyond the JUnit suites, `check` also runs:

- `verifyArtifacts` — inspects the shaded jar's entries, then loads each
  artifact on the classpaths it lands on. Three things vary and each decides
  what registers with the `DriverManager`: the artifact, the arrangement
  (system classpath or a plugin class loader of its own), and which class an
  application touches first (a URL lookup, a data source, the driver class by
  name). The two artifacts owe opposite things: the standalone's bundled pgJDBC
  must never answer a `jdbc:postgresql://` URL, while the thin artifact's
  belongs to the application and must always keep answering it. The plugin
  loader is the arrangement the service entry cannot carry, the `DriverManager`
  scanning for services with the thread context class loader, which does not
  reach into a plugin loader.
- `WrapperCompletenessTest` — reflects over the JDBC interfaces and fails if
  any method would hand out a raw pgJDBC object instead of a wrapper. A future
  JDBC release adding methods surfaces here.

Pin what the driver decides, not what pgJDBC does. A metadata answer or a
protocol behavior the driver forwards untouched can only change with pgJDBC,
so a test holding it fails the suite for reasons that have nothing to do with
CrateDB.

### The derived suites

Five integration suites assert nothing anyone wrote down in advance. Each
computes the answer it expects, which is how they cover a surface — hundreds
of methods across eight JDBC interfaces plus pgJDBC's three — that no amount
of hand-written cases would reach. They are a layer of their own: `JdbcSurface`,
`Invocation`, `Fixture`, `Posture`, `Sweep`, `Outcome`, `Verb`, `Program`,
`ProgramRun`, `ValueCatalogue`, `DeviceSeed`, `PgJdbcDelta`, `Disposition`,
`Refusals` and `ControlCell` serve nothing else, and no suite outside the five
reaches into them.

- `DifferentialIT` points **both** drivers at the same CrateDB and asks each of
  them every method of every interface an application reaches, then compares
  the answers. The expected answer is whatever stock pgJDBC gave; the driver's
  own claim — pgJDBC, plus a small set of deliberate differences — is what
  makes that an oracle. `JdbcSurface` enumerates the calls and generates their
  arguments, `Fixture` builds the objects to call them on, and `Outcome`
  renders what came back so two drivers can be compared on it. Each driver is
  swept twice, so an answer that merely changes from moment to moment is told
  apart from one that differs between drivers.

  Every call is made in each state of `Posture` that its interface has one for
  — closed, executed, exhausted, carrying a batch — because a JDBC object is a
  state machine and most of what a driver owes its caller is what a method
  answers in a state that is not the first one. The state is part of a call's
  name, `ResultSet.getString(int)@closed`, and a delta entry naming no state
  covers a call in every state it answers alike in.

  A reading call taking a column index is pointed at every column of the probe
  table rather than only the first, which holds an integer — asked only about
  that one, the sweep asks thirty-odd getters what they make of an integer and
  never asks any of them what they make of a timestamp, a json object or an
  array. Reading one type through the getter for another is where a driver
  layered over another driver's conversions has somewhere to go wrong, and it
  is the whole of what `CrateResultSet` and the json gate in
  `CrateResultSetMetaData` do. The column is part of the name too,
  `ResultSet.getString(int)#stamp`, with the first column left unsuffixed so
  that a call reading it is named the way every delta entry names it. A sweep
  is some 3,458 calls; each run prints how many it reached and which methods it
  left out for want of an argument to pass.

  It also carries the rules a comparison cannot reach, where two drivers can
  agree and both be wrong: `ClosedObjectContract` for what a closed object owes
  (JDBC has one refuse everything but the few methods about its closedness),
  and `SqlStateContract` for the part of an error a program branches on — five
  characters, a defined condition class, and agreeing with the `SQLException`
  subclass carrying it.

- `SelfConsistencyIT` holds the driver against itself: a column read as the
  class its metadata names, `wasNull` agreeing with the value, an array read
  whole and by range describing one value, a setting in effect being the one
  the metadata claims, a metadata result set carrying the columns JDBC
  specifies. None of these needs a reference implementation, which is why they
  catch what the differential cannot — a driver can be wrong in exactly the way
  the one it is compared against is wrong.

- `ValueRoundTripIT` writes each value of a catalogue by every route a value
  reaches a column through — a column of its own, an element of a series, a
  member of an object, an array nested in one — and holds the readings of one
  value against each other rather than against a stored expectation. Routes of
  a kind have to agree: what a typed column gives back and what json gives
  back are different answers to different questions, and only the second is
  allowed to widen.

  A value no route can carry is the server's decision rather than a gap in the
  driver, and the catalogue reaches for that edge on purpose, so those are
  written down in `refusals.txt` and checked both ways: a value the server
  starts refusing fails the build until it is listed, and one it stops refusing
  fails until its entry goes. Stepping over them silently is what would leave
  CrateDB's narrowing of `bigint` by two — both endpoints spent as null
  sentinels in doc values — unremarked in a suite that stores `Long.MIN_VALUE`
  on every run.

- `SequenceIT` generates programs — orders of calls drawn from a seed — runs
  each against both drivers and compares them step for step. The sweep asks
  every method once, on an object built for that one call; this asks what
  happens next, which is where a wrapper that keeps something is caught
  handing it back after the thing it describes has gone. A divergence is
  shrunk to the shortest program that still shows it and filed under the chain
  of calls that produced the object it happened on, so an entry outlives the
  seed that found it. Generation never executes: a program is text, and a
  failure is reproducible from the report rather than from the seed alone.

- `RoundTripCostIT` counts what reaches the server, from `sys.jobs_log`, for
  each of a set of ordinary operations. It is a ratchet rather than a
  measurement: a call that starts asking the server one more question than it
  used to fails, whether the question came from this driver or from a pgJDBC
  upgrade underneath it.

`SequenceIT` keeps its own `sequence-delta.txt`, because what it names is a
chain of calls rather than a single one.

`driver/test/resources/.../pgjdbc-delta.txt` is where the first two write their
findings down. It is the executable specification of the delta: every call
that answers differently is named there with a reason, and a call named there
must still answer that way. A new difference fails the build, and so does one
that quietly went away. Adding an entry is a decision that the difference is
meant — a failing run prints the line to add, along with what each driver
answered, so the choice between "deliberate" and "bug" needs nothing but the
output.

An entry reads `<kind> {disposition} [scope] <call> :: <why>`. The kind says
what the machine saw — `differs`, `unstable`, `unchecked`, `permissive`,
`malformed`, `inconsistent`. The disposition says what is to be done about it,
which is what separates a difference this driver chose from a fault it
inherited and has not reported: `by-design` is this driver's own choice,
`for-crate` an accommodation of something CrateDB does, `spec` a reading JDBC
leaves open where neither driver is wrong, `inherent` a property of what is
being measured rather than anyone's decision, `wont-file` a fault of pgJDBC's
recorded rather than reported, `jdk` a default method `java.sql` supplies to
every driver that does not override it, so the behavior is the platform's and
there is nobody to report it to. The open ones name a project and either nothing
yet — `pgjdbc-todo`, `crate-todo` — or the ticket, `pgjdbc#3141`. So
`grep -- '-todo'` is the queue of what upstream has not been told, and a
ticketed entry going stale is upstream shipping the fix, which is the one thing
a driver bump should announce rather than leave reading as drift. A scope in
square brackets holds an entry to the runs it belongs to: a connection property
the suite is running under, `[!property]` for its absence, or `[>=6.4]` for the
CrateDB releases that have the behavior, checked against `serverAtLeast`.
`sequence-delta.txt` and `refusals.txt` are written in the same grammar.

Findings are reported worst first, and within `differs` by which way the
difference runs. `laxer` — this driver answered where pgJDBC refused — comes
first: it is the one direction that can hide a fault of this driver's, and the
one no contract covers, since `ClosedObjectContract` governs the objects an
application closes rather than `DatabaseMetaData`. Then two answers that merely
differ, which a caller acts on either way; then a refusal where pgJDBC
answered, which stops an application rather than misleading it; then two
refusals differing only in their wording.

A run that passes prints a census of the ledger instead — entries by kind, by
disposition, what is owed to whom, and the pgJDBC version it was measured
against — so that a file of decisions teaches something on the runs where none
of them breaks. It also names any `by-design` entry whose method this driver
does not override: a call pgJDBC reaches through untouched cannot answer
differently because of a decision made here, so the disposition is describing
the server or a layer below. That one is said rather than failed, the mapping
from an interface to the class implementing it being a convention.

`ControlCell` sweeps the same surface with stock pgJDBC against a stock
PostgreSQL, and `everyInheritedFaultReproducesAgainstStockPostgres` is the one
question it is there to answer. Handing a fault to pgJDBC is a claim about a
driver the rest of the suite only ever watches through a server it was not
written for; what makes the claim
reportable upstream is that the fault is still there against the database it
was written for. Both directions are checked: a fault reaching the control and
named nowhere is one this driver has not noticed inheriting, and an entry
claiming a fault the control no longer meets has outlived it. Renderings are
never compared — two servers differ in their catalogs, their column types and
their name for json in ways that say nothing about either driver — so the only
questions put to it are the two rules a change of substrate cannot move:
`ClosedObjectContract` and `SqlStateContract`. It runs under
`-PtestControl=true`, for the second server it starts.

`FaultIT` is the one suite that does not assume a server which answers. It
puts a Toxiproxy between the driver and a CrateDB of its own and breaks it —
a connection cut mid-query, a socket that stops carrying bytes, writes split
across packets — and holds the driver to what it owes then: a `SQLException`
in class 08, a return inside the socket timeout the URL asked for, a
connection that reports itself invalid and refuses everything afterwards, and
values that read back whole however the bytes arrived. It starts a server of
its own because a killed connection leaves jobs behind and `RoundTripCostIT`
counts what reaches the shared one, and it runs only under `-PtestFaults=true`
because that pair of containers costs more than the rest of the suite.

The delta hangs off `integrationTest` rather than `check`, which keeps `check`
Docker-free and runs the comparison against every server in the CI matrix —
where it belongs, since some of the delta is the server's doing rather than
the driver's.

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

- **A wrapper answers its whole interface.** Adapted methods come first, then
  a folded `editor-fold` block forwarding the rest. An adapted method carries
  `@Adapted`, which is how `WrapperCompletenessTest` holds every JDBC object
  handed to an application to being one of this driver's own; add the marker
  when you add an override. `Connection`'s four `enquote*`/`isSimpleIdentifier`
  methods are absent from the delegation on purpose, Java 11 having no such
  methods to forward.
- **The wrappers nest as `java.sql` does.** `CrateStatement` →
  `CratePreparedStatement` → `CrateCallableStatement`. Each inherits everything
  the one below it does and adds only what its own interface brings, so
  behavior such as bracketing an execution with the query timeout is written
  once and holds for all three.
- **Two artifacts.** `crate-jdbc` is thin, with pgJDBC as a visible transitive
  dependency. `crate-jdbc-standalone` bundles everything relocated under
  `io.crate.shade`, carries the bundled licenses, and deliberately keeps its
  pgJDBC out of the `DriverManager`. Code reached from the DataSource must not
  route through `DriverManager`, or it breaks in that artifact only.
- **CrateDB is not PostgreSQL.** It has no transactions, `ROLLBACK`, or
  savepoints, and its `pg_catalog` is partial. Never reason from PostgreSQL
  behavior, and never raise a finding from reading the code alone: write,
  compile and run a test that proves the failure first. The container makes
  that cheap.
- **Minimum server is CrateDB 6.0**, which is where `current_catalog` arrived;
  pgJDBC's metadata queries need it.
- Documentation is built with `cd docs && make html`, checked with `make check`.
- `CHANGES.txt` is the one place that narrates change. Entries stay under
  `Unreleased` until a release dates the heading.

Everything left in the tree, comments and names and docstrings and test titles
alike, should read as though the code had always been that way, describing what
it is instead of what was changed.
