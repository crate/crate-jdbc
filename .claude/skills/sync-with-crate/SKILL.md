---
name: sync-with-crate
description: Keep the CrateDB JDBC driver in sync with the CrateDB server. Finds what changed in crate/crate (types, pg_catalog, wire protocol, SQL surface, deprecations), decides what the driver owes each change, then implements, tests and documents it. Use when a new CrateDB release lands, when auditing driver/server drift, or when someone reports a type or metadata gap.
---

# Keeping the driver in sync with CrateDB

## The model that makes decisions obvious

This driver is a thin adaptation layer over stock pgJDBC. **Anything CrateDB
sends with an ordinary PostgreSQL wire representation already works, with no
driver change at all.** Most new server types need nothing from this repo.

The driver only owes a change when one of these is true:

1. **Binding needs a CrateDB name that PostgreSQL spells differently** — users
   write `createArrayOf("object", …)` using CrateDB's type name. A name the
   two spell alike resolves against `pg_type` on its own; only a name that
   differs needs an entry in `ARRAY_TYPE_ALIASES`.
2. **The value needs conversion** — CrateDB sends it as `json` but users expect
   a Java type (`OBJECT`/`geo_shape` → `Map`).
3. **Metadata cannot be answered** — a `DatabaseMetaData` call depends on a
   catalog object CrateDB lacks, and should fail with an explanation.
4. **The documentation would otherwise be wrong or silent** — this is the most
   common obligation by far.

Resist adding driver code for a type stock pgJDBC already handles. A doc row
and a test are usually the whole job.

## Where to look in CrateDB

Expects a `crate/crate` checkout beside this repo (`../crate`). Clone it if
missing. Work from a release tag or `master` as the question requires.

**Ranked by signal:**

| Path in `crate/crate` | Why it matters |
| --- | --- |
| `server/src/main/java/io/crate/protocols/postgres/types/PGTypes.java` | **The authoritative map** of CrateDB type → PostgreSQL wire type/OID. A type absent here is invisible to JDBC. Diffing this file between two releases is the single highest-signal sync check. |
| `server/src/main/java/io/crate/protocols/postgres/types/` | One class per wire type. A new file here is a new type on the wire. |
| `docs/appendices/release-notes/*.rst` | Breaking changes, deprecations, new features, per release. |
| `server/src/main/java/io/crate/metadata/pgcatalog/` | Which `pg_catalog` tables and columns exist. Governs which `DatabaseMetaData` calls can work. |
| `docs/general/ddl/data-types.rst` | User-facing type semantics and literal syntax. |
| `docs/interfaces/postgres.rst` | Wire-protocol compatibility and its documented limits. |
| `docs/general/builtins/scalar-functions.rst` | Functions users reach through the driver (`knn_match`, `vector_similarity`). |
| `docs/sql/statements/` | Statement-level support — e.g. `declare.rst`/`fetch.rst`/`close.rst` back cursor fetching. |

Useful commands:

```sh
git -C ../crate log --oneline <old-tag>..<new-tag> -- server/src/main/java/io/crate/protocols/postgres/types/
git -C ../crate diff <old-tag>..<new-tag> -- server/src/main/java/io/crate/protocols/postgres/types/PGTypes.java
git -C ../crate diff <old-tag>..<new-tag> -- server/src/main/java/io/crate/metadata/pgcatalog/
```

## Where to look in the driver

| Path | Holds |
| --- | --- |
| `driver/main/java/io/crate/client/jdbc/CrateConnection.java` | `ARRAY_TYPE_ALIASES` — the CrateDB type names `createArrayOf` has to translate. |
| `driver/main/java/io/crate/client/jdbc/CrateResultSet.java` | `fromPg` / `asType` — value conversion on read. |
| `driver/main/java/io/crate/client/jdbc/CrateArray.java` | Element conversion inside arrays. |
| `driver/main/java/io/crate/client/jdbc/CrateParameters.java` | Value conversion on bind. |
| `driver/main/java/io/crate/client/jdbc/CrateDatabaseMetaData.java` | Metadata surface and old-server error translation. |
| `docs/data-types.rst` | The user-facing type table and per-type prose. **Keep this honest.** |
| `docs/limitations.rst` | What the driver cannot answer for, and why. |
| `driver/test/…/integrationtests/TypesIT.java` | Per-type read/bind behavior. |
| `driver/test/…/integrationtests/VectorSearchIT.java` | Vector search end to end. |
| `driver/test/…/integrationtests/MetaDataIT.java` | The `DatabaseMetaData` contract. |
| `driver/test/…/integrationtests/BaseIntegrationTest.java` | `DEFAULT_CRATEDB_VERSION` — the server tests boot. |
| `.github/workflows/tests.yml` | The CI server matrix. |

## Procedure

### 1. Establish the delta

Identify the CrateDB versions to compare (the driver's pinned default versus
the new release). Read that range's release notes and diff `PGTypes.java` and
`pgcatalog/`. Produce a candidate list of changes.

### 2. Measure, do not assume

**Never conclude from documentation alone** — CrateDB's docs and its wire
behavior can disagree, and pgJDBC adds its own mapping on top. Write a
throwaway probe as an integration test, run it against the real server, read
the output, then delete it.

```java
// driver/test/java/io/crate/client/jdbc/integrationtests/ProbeIT.java
public class ProbeIT extends BaseIntegrationTest {

    private static void probe(Connection conn, String label, String sql) {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            rs.next();
            Object value = rs.getObject(1);
            System.out.printf("PROBE %-18s | pgType=%-14s jdbcType=%-5d class=%-38s | getObject=%s%n",
                label, md.getColumnTypeName(1), md.getColumnType(1), md.getColumnClassName(1),
                value == null ? "null" : value.getClass().getName() + " -> " + value);
        } catch (Exception e) {
            System.out.printf("PROBE %-18s | FAILED %s: %s%n",
                label, e.getClass().getSimpleName(), String.valueOf(e.getMessage()).replace('\n', ' '));
        }
    }

    @Test
    public void probe() throws Exception {
        try (Connection conn = connect()) {
            probe(conn, "<type>", "select <literal or cast>");
        }
    }
}
```

Run it with `./gradlew integrationTest --tests '*ProbeIT*'` — the class name
must end in `IT` or the filter skips it.

Watch for three things in the output:

- **The value's class** — is it what `docs/data-types.rst` promises?
- **`getColumnClassName()` versus the actual `getObject()` class.** When they
  disagree, that belongs in the type-description table in `data-types.rst`;
  mappers that resolve by declared type break on it.
- **Failures.** A type the docs describe may not exist on the wire at all.

### 3. Classify each finding

| Finding | Driver action |
| --- | --- |
| New type, ordinary PG representation | Doc row + a `TypesIT` case. No driver code. |
| New type users must name in `createArrayOf` | `ARRAY_TYPE_ALIASES` entry + test + doc. |
| Type arrives as `json` but should be a Java type | Convert in `CrateResultSet`/`CrateArray` + test + doc. |
| New `pg_catalog` coverage | A previously failing `DatabaseMetaData` call may work — retest and drop any stale limitation from the docs. |
| Removed/renamed catalog object | Check `CrateDatabaseMetaData` and its error translation; update the supported-server range if the floor moved. |
| Deprecation | Grep the driver and docs for it; if the driver depends on it, plan the replacement before it is removed. |
| New capability (functions, statements) | Usually docs plus an integration test proving it works through the driver. |

### 4. Implement, test, document

Every accepted change lands as one coherent set:

- code, if the classification calls for it;
- a test pinning the invariant — extend a table-driven test rather than adding
  a near-duplicate;
- the docs row or prose, in the same commit as the behavior;
- a `CHANGES.txt` entry under `Unreleased`.

### 5. Routine version bump

When a new CrateDB release ships and nothing else is required:

- `DEFAULT_CRATEDB_VERSION` in `BaseIntegrationTest`;
- the `cratedb-image` matrix in `.github/workflows/tests.yml`;
- re-run the suite against both the old and new server.

Raise the **minimum** supported version only when a metadata query genuinely
needs a newer server, and say why in the docs — the current floor is CrateDB
6.0, where `current_catalog` arrived for pgJDBC's benefit.

### 6. Verify

```sh
make verify
CRATEDB_IMAGE=crate/crate:nightly ./gradlew integrationTest
cd docs && make check
```

## Done means

- Probes deleted; nothing left behind but real tests.
- Every claim in `docs/data-types.rst` matches measured behavior.
- Suite green against the pinned server and against nightly.
- `CHANGES.txt` describes the change under `Unreleased`.
- Nothing in the tree narrates the sync itself — no "new in 6.x", no
  "previously". Write what the driver *is*, per `AGENTS.md`.
