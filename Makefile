# Entry points for the workflows described in DEVELOP.rst and AGENTS.md.
# Everything here delegates to Gradle; nothing configures the build.

GRADLE ?= ./gradlew

# The oldest CrateDB the driver supports. `make itest-floor` runs the
# integration suite against it on the oldest JRE too — the far corner of what
# the driver claims to serve, and where a type this release describes
# differently from a later one is held to this release's description. The CI
# matrix in .github/workflows/tests.yml boots the same version.
FLOOR_CRATEDB_VERSION ?= 6.0.8

# The oldest JRE the driver runs on. Gradle itself needs a newer JDK, so the
# baseline is a toolchain the test tasks launch on rather than the JVM the
# build runs on.
BASELINE_JAVA_VERSION ?= 11

# How many nodes `make itest-cluster` brings up. Several, so that connections
# have somewhere to spread and the connection pgJDBC sends a cancel over can
# reach a node other than the one running the query.
CLUSTER_NODES ?= 3

# The zone `make itest-zoned` runs the suites in. Anything but UTC: at offset
# zero a conversion that goes through the JVM's default calendar and one that
# does not give the same answer.
TEST_TIME_ZONE ?= Europe/Berlin

# How `make itest-wire` has pgJDBC send statements and decode values. Text
# rather than binary is the arrangement the driver reads json arrays under,
# which is the assumption most of its conversions rest on.
TEST_CONNECTION_PROPERTIES ?= binaryTransfer=false

# The JRE `make coverage` measures on. Pinned rather than left to the JVM
# running the build, because the coverage agent supports a narrower range of
# JDKs than the driver does, and a number from one JRE answers for all of them.
COVERAGE_JAVA_VERSION ?= 21

.DEFAULT_GOAL := help
.PHONY: help build test test-baseline itest itest-floor itest-cluster itest-zoned \
        itest-standalone itest-wire itest-faults itest-control coverage mutation \
        check verify format docs docs-check \
        publish-local sbom version tag clean

help:  ## Show this help
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) \
		| awk -F':.*## ' '{printf "  \033[1m%-16s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  Server selection for the integration suite:"
	@echo "    CRATEDB_VERSION=6.2.2   tag of the crate image to boot"
	@echo "    CRATEDB_IMAGE=...       full image reference, e.g. crate/crate:nightly"
	@echo "    CRATE_URL=crate://...   use a running server, start no container"
	@echo "    CRATEDB_NODES=3         run against a cluster of that many nodes"
	@echo "    -PtestTimeZone=...      run the JVM in that zone instead of UTC"
	@echo "    -PtestConnectionProperties=...  add them to the JDBC URL"
	@echo "    -PtestSeed=...          draw other generated values and programs"

build:  ## Build both jars into build/libs
	$(GRADLE) jar standaloneJar

test:  ## Run the unit tests (no Docker)
	$(GRADLE) test

test-baseline:  ## Run the unit tests on the oldest supported JRE
	$(GRADLE) test -PtestJavaVersion=$(BASELINE_JAVA_VERSION)

itest:  ## Run the integration tests against a CrateDB container
	$(GRADLE) integrationTest

itest-floor:  ## Run the integration tests on the oldest supported CrateDB and JRE
	CRATEDB_VERSION=$(FLOOR_CRATEDB_VERSION) $(GRADLE) integrationTest \
		-PtestJavaVersion=$(BASELINE_JAVA_VERSION)

itest-cluster:  ## Run the integration tests against a CrateDB cluster
	CRATEDB_NODES=$(CLUSTER_NODES) $(GRADLE) integrationTest

itest-zoned:  ## Run the integration tests in a JVM zone away from UTC
	$(GRADLE) integrationTest -PtestTimeZone=$(TEST_TIME_ZONE)

itest-standalone:  ## Run the integration tests against the standalone artifact
	$(GRADLE) standaloneTest

itest-wire:  ## Run the integration tests over a different arrangement of the wire
	$(GRADLE) integrationTest -PtestConnectionProperties=$(TEST_CONNECTION_PROPERTIES)

itest-faults:  ## Run the suite that breaks the network under the driver
	$(GRADLE) integrationTest -PtestFaults=true --tests '*FaultIT'

itest-control:  ## Hold the faults inherited from pgJDBC against stock PostgreSQL
	$(GRADLE) integrationTest -PtestControl=true --tests '*DifferentialIT'

mutation:  ## Report the lines no test objects to being changed (needs CRATE_URL)
	$(GRADLE) mutationTest

coverage:  ## Measure what the suites reach of the hand-written classes
	$(GRADLE) jacocoTestReport -Pcoverage -PtestJavaVersion=$(COVERAGE_JAVA_VERSION)

check:  ## Unit tests, code style and artifact checks
	$(GRADLE) check

# Not docs-check: its link checker reaches the open internet and fails on
# rate limits rather than on anything in the tree.
verify: check test-baseline itest itest-floor itest-cluster itest-zoned itest-standalone itest-wire itest-faults itest-control  ## Tests and checks, across the supported ranges

format:  ## Apply the code style
	$(GRADLE) spotlessApply

docs:  ## Build the documentation
	$(MAKE) -C docs html

docs-check:  ## Build the documentation and check its links and prose
	$(MAKE) -C docs check

publish-local:  ## Publish both artifacts to the local Maven repository
	$(GRADLE) publishToMavenLocal

sbom:  ## Write the CycloneDX SBOM to build/reports
	$(GRADLE) cyclonedxBom

version:  ## Print the version being built
	@$(GRADLE) -q getVersion

tag:  ## Tag the current version and push it, which starts the release
	./devtools/create_tag.sh

clean:  ## Remove the build directory
	$(GRADLE) clean
