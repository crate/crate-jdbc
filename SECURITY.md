# Security policy

## Reporting a vulnerability

Please report security issues in the CrateDB JDBC driver privately, so that a
fix can be released before the details are public.

- Use [GitHub's private vulnerability reporting][advisories] for this
  repository, or
- email <office@crate.io>.

Please do not open a public issue for a security problem.

Include what you have: the driver and CrateDB versions, whether the
`crate-jdbc` or the `crate-jdbc-standalone` artifact is affected, and the
steps that reproduce the problem.

## Supported versions

Fixes go into the current minor release. Older ones receive none, so an
affected deployment upgrades rather than waiting for a patch.

## Bundled dependencies

The driver builds on the [PostgreSQL JDBC driver][pgjdbc], and the
`crate-jdbc-standalone` artifact bundles it, relocated under `io.crate.shade`
where dependency scanners that read a POM cannot see it. Both artifacts
therefore publish a CycloneDX SBOM under the `cyclonedx` classifier, and the
standalone jar names the pgJDBC release it bundles in its manifest as
`Bundled-PgJdbc-Version`.

A vulnerability in pgJDBC itself is best reported to [that project][pgjdbc];
please also let us know, so that the bundled version can be updated.

[advisories]: https://github.com/crate/crate-jdbc/security/advisories/new
[pgjdbc]: https://github.com/pgjdbc/pgjdbc
