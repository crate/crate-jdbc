/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What only several nodes can show.
 *
 * <p>Every other suite runs against a cluster of one, where a whole set of the
 * driver's decisions cannot be wrong: {@code loadBalanceHosts} is on by
 * default and has nothing to balance over, and the reason a query timeout is
 * given to the server directly, namely that pgJDBC delivers one as a cancel
 * request on a second connection which a load balancer may point at a node
 * knowing nothing of the session, needs a second node to exist at all.</p>
 *
 * <p>The cluster is this suite's own rather than the one
 * {@code BaseIntegrationTest} hands out, so that these run wherever the
 * integration tests run rather than only when someone asks for several
 * nodes.</p>
 */
public class ClusterIT extends BaseIntegrationTest {

    /** Enough nodes that losing one leaves the rest with a quorum. */
    private static final int NODES = 3;

    /** How many connections it takes before landing twice on one node stops being luck. */
    private static final int CONNECTION_ATTEMPTS = 30;

    /**
     * How many times the timeout is tried. Each attempt opens its own
     * connection, and pgJDBC opens a second one to cancel with, so a few
     * attempts are enough for the cancel to reach a node other than the one
     * running the query at least once.
     */
    private static final int TIMEOUT_ATTEMPTS = 5;

    private static final int TIMEOUT_SECONDS = 3;

    private static final int ROWS = 50_000;

    /**
     * A cross join over {@link #ROWS} rows per side, which no machine finishes
     * within {@link #TIMEOUT_SECONDS}. It reads a real table on purpose: a
     * query over {@code sys} tables is answered from memory on the thread
     * serving the connection, and is bounded by neither the cancel request a
     * timeout sends nor the server's {@code statement_timeout}.
     */
    private static final String LONG_RUNNING = "select count(*) from numbers a, numbers b";

    private static CrateDBCluster cluster;

    @BeforeAll
    static void startCluster() throws Exception {
        cluster = CrateDBCluster.start(serverImage(), NODES);
        try (Connection conn = DriverManager.getConnection(cluster.url());
             Statement statement = conn.createStatement()) {
            statement.execute(
                "create table numbers (x integer) clustered into 4 shards " +
                "with (number_of_replicas = 1)");
            statement.execute(
                "insert into numbers (x) (select g from generate_series(1, " + ROWS + ") as g)");
            statement.execute("refresh table numbers");
        }
        awaitAllShardsStarted();
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private static void awaitAllShardsStarted() throws Exception {
        for (int attempt = 0; attempt < 600; attempt++) {
            try (Connection conn = DriverManager.getConnection(cluster.url());
                 Statement statement = conn.createStatement();
                 ResultSet rows = statement.executeQuery(
                     "select count(*) from sys.shards where state != 'STARTED'")) {
                rows.next();
                if (rows.getLong(1) == 0) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Shards were still unassigned");
    }

    /**
     * The fixture is worth nothing if the nodes did not actually find each
     * other: a cluster of three that formed as three clusters of one would let
     * every test below pass while measuring nothing.
     */
    @Test
    public void theClusterHasEveryNodeTheFixtureStarted() throws Exception {
        for (int node = 0; node < NODES; node++) {
            try (Connection conn = DriverManager.getConnection(cluster.urlFor(node));
                 Statement statement = conn.createStatement();
                 ResultSet rows = statement.executeQuery(
                     "select count(*) from sys.nodes")) {
                rows.next();
                assertThat("node " + node + " sees the whole cluster",
                    rows.getInt(1), is(NODES));
            }
        }
    }

    /**
     * A URL naming several hosts spreads its connections over them. The driver
     * turns {@code loadBalanceHosts} on for exactly this, and a caller that
     * opens a pool against a cluster gets the spread without asking.
     */
    @Test
    public void connectionsOpenedThroughOneUrlLandOnMoreThanOneNode() throws Exception {
        Set<String> reached = new HashSet<>();
        for (int attempt = 0; attempt < CONNECTION_ATTEMPTS; attempt++) {
            try (Connection conn = DriverManager.getConnection(cluster.url())) {
                reached.add(nodeNameOf(conn));
            }
        }
        assertThat("connections opened through " + cluster.url() + " reached " + reached,
            reached.size(), greaterThan(1));
    }

    /**
     * Which node answered. A running job is held by the node that took it, and
     * the only job running is the one asking.
     */
    private static String nodeNameOf(Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rows = statement.executeQuery("select node['name'] from sys.jobs limit 1")) {
            rows.next();
            return rows.getString(1);
        }
    }

    /**
     * A statement's timeout holds on a cluster, where pgJDBC's own delivery of
     * one does not: it opens a second connection to send the cancel request,
     * and a URL naming three nodes will point that connection at whichever one
     * the load balancing picks — not necessarily the one running the query.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void aQueryTimeoutEndsAQueryWhicheverNodeItRunsOn() throws Exception {
        for (int attempt = 0; attempt < TIMEOUT_ATTEMPTS; attempt++) {
            try (Connection conn = DriverManager.getConnection(cluster.url());
                 Statement statement = conn.createStatement()) {
                statement.setQueryTimeout(TIMEOUT_SECONDS);
                long started = System.nanoTime();
                assertThrows(SQLException.class, () -> statement.executeQuery(LONG_RUNNING),
                    "the query ran to completion instead of being stopped, on " + nodeNameOf(conn));
                long elapsed = (System.nanoTime() - started) / 1_000_000;
                assertThat("a " + TIMEOUT_SECONDS + " second timeout took " + elapsed + "ms to hold",
                    elapsed, is(lessThan(60_000L)));
                // A cancel the driver mishandled would leave the protocol
                // stream out of step, and this would fail or hang.
                try (ResultSet rows = conn.createStatement().executeQuery("select 1")) {
                    assertThat(rows.next(), is(true));
                }
            }
        }
    }

}
