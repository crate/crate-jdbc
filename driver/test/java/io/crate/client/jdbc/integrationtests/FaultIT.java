package io.crate.client.jdbc.integrationtests;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.cratedb.CrateDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What the driver does when the network under it misbehaves.
 *
 * <p>Every other suite asks a server that answers. This one puts a proxy in
 * between and breaks it on purpose: a connection cut while a query is running,
 * a socket that stops carrying bytes, writes split across packets. Those are
 * the conditions an application meets in production and never in a test, and
 * what a driver owes under them is narrow — a {@link SQLException} a caller can
 * catch, a state it can branch on, a return before the timeout it asked for,
 * and nothing readable from an object the failure has ruined.</p>
 *
 * <p>It starts a server of its own. A toxic that kills a connection mid-query
 * leaves jobs behind on the server it was talking to, and {@code RoundTripCostIT}
 * counts what reaches the one the rest of the suite shares.</p>
 */
public class FaultIT extends BaseIntegrationTest {

    /** What the proxy is called, and where CrateDB listens behind it. */
    private static final String PROXY = "cratedb";
    private static final int PGSQL_PORT = 5432;

    /**
     * How long a call may take once the bytes have stopped. The URL asks for a
     * two-second socket timeout; anything past this is the driver waiting on
     * something it was told not to wait for.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(15);

    private static Network network;
    private static CrateDBContainer server;
    private static ToxiproxyContainer toxiproxy;
    private static Proxy proxy;
    private static String url;

    @BeforeAll
    static void startTheProxiedServer() throws Exception {
        assumeTrue(Boolean.getBoolean("test.faults"),
            "fault injection starts a server of its own; -PtestFaults=true asks for it");
        network = Network.newNetwork();
        server = new CrateDBContainer(serverImage()).withNetwork(network).withNetworkAliases(PROXY);
        toxiproxy = new ToxiproxyContainer(
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0")
                .asCompatibleSubstituteFor("shopify/toxiproxy")).withNetwork(network);
        server.start();
        toxiproxy.start();
        proxy = new eu.rekawek.toxiproxy.ToxiproxyClient(
            toxiproxy.getHost(), toxiproxy.getControlPort())
            .createProxy(PROXY, "0.0.0.0:8666", PROXY + ":" + PGSQL_PORT);
        url = String.format("crate://%s:%d/doc?user=crate&socketTimeout=2",
            toxiproxy.getHost(), toxiproxy.getMappedPort(8666));
        try (Connection conn = DriverManager.getConnection(url);
             Statement statement = conn.createStatement()) {
            statement.execute("create table faults (id integer primary key, name text)"
                + " clustered into 1 shards with (number_of_replicas = 0)");
            statement.execute("insert into faults (id, name) values (1, 'one'), (2, 'two')");
            statement.execute("refresh table faults");
        }
    }

    @AfterEach
    void mendTheNetwork() throws Exception {
        if (proxy == null) {
            return;
        }
        for (eu.rekawek.toxiproxy.model.Toxic toxic : proxy.toxics().getAll()) {
            toxic.remove();
        }
        proxy.enable();
    }

    @AfterAll
    static void stopTheProxiedServer() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (server != null) {
            server.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    /**
     * A failure on the wire reaches a caller as something it can catch, with a
     * state it can branch on. Class 08 is the one the standard gives to a
     * connection that is no longer there.
     */
    @Test
    public void aCutConnectionFailsAsASqlException() throws Exception {
        try (Connection conn = DriverManager.getConnection(url);
             Statement statement = conn.createStatement()) {
            proxy.disable();
            SQLException raised = assertThrows(SQLException.class,
                () -> statement.executeQuery("select id, name from faults order by id"));
            assertThat(raised.getSQLState(), notNullValue());
            assertThat(raised.getSQLState(), startsWith("08"));
        }
    }

    /**
     * A socket that has stopped carrying bytes is given up on. This is the
     * hang: without a bound, a read from a connection that will never answer
     * waits for as long as the application lives.
     */
    @Test
    public void aStalledConnectionDoesNotOutlastTheSocketTimeout() throws Exception {
        try (Connection conn = DriverManager.getConnection(url);
             Statement statement = conn.createStatement()) {
            proxy.toxics().bandwidth("stalled", ToxicDirection.DOWNSTREAM, 0);
            SQLException raised = assertTimeoutPreemptively(PATIENCE,
                () -> assertThrows(SQLException.class,
                    () -> statement.executeQuery("select id, name from faults order by id")));
            // Class 08 rather than any refusal, so that a query answered by a
            // proxy that never stalled cannot pass this by failing some other
            // way.
            assertThat(raised.getSQLState(), startsWith("08"));
        }
    }

    /**
     * A connection the network took away says so, and refuses everything a
     * closed one would refuse. An application that keeps using it — or a pool
     * that hands it out again — has no other way to find out.
     */
    @Test
    public void aConnectionThatFailedReportsItself() throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement statement = conn.createStatement()) {
                proxy.disable();
                assertThrows(SQLException.class,
                    () -> statement.executeQuery("select id from faults order by id"));
            } catch (SQLException closingAFailedStatement) {
                // Closing what the failure already took is not the question.
            }
            assertThat(conn.isValid(1), is(false));
            List<String> answered = new ArrayList<>();
            refuses(answered, "createStatement", conn::createStatement);
            refuses(answered, "getMetaData", conn::getMetaData);
            refuses(answered, "prepareStatement", () -> conn.prepareStatement("select 1"));
            assertThat("A connection the network took away still answered:\n  "
                + String.join("\n  ", answered), answered, is(empty()));
        }
    }

    /**
     * Bytes arriving in pieces are the same bytes. A driver that reads a value
     * out of one packet at a time has to put it back together, and a message
     * split at the wrong byte is where one that does not comes apart.
     */
    @Test
    public void bytesArrivingInPiecesReadBackWhole() throws Exception {
        proxy.toxics().slicer("sliced", ToxicDirection.DOWNSTREAM, 4, 0);
        proxy.toxics().latency("slow", ToxicDirection.DOWNSTREAM, 5).setJitter(5);
        try (Connection conn = DriverManager.getConnection(url);
             Statement statement = conn.createStatement();
             ResultSet rows = statement.executeQuery(
                 "select id, name from faults order by id")) {
            List<String> read = new ArrayList<>();
            while (rows.next()) {
                read.add(rows.getInt("id") + "=" + rows.getString("name"));
            }
            assertThat(read, is(List.of("1=one", "2=two")));
        }
    }

    /**
     * A batch that fails partway reports what it managed, as a
     * {@link BatchUpdateException} carrying one count per entry attempted.
     * Anything else leaves a caller unable to tell which rows are there.
     */
    @Test
    public void aFailedBatchFailsAsABatchUpdateException() throws Exception {
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement insert = conn.prepareStatement(
                 "insert into faults (id, name) values (?, ?)")) {
            for (int id = 10; id < 14; id++) {
                insert.setInt(1, id);
                insert.setString(2, "row " + id);
                insert.addBatch();
            }
            proxy.disable();
            SQLException raised = assertThrows(SQLException.class, insert::executeBatch);
            assertThat("A batch cut off partway has to say how far it got, so that a caller "
                    + "knows which rows to write again — it raised " + raised.getClass().getName(),
                raised instanceof BatchUpdateException, is(true));
        }
    }

    private static void refuses(List<String> answered, String call, Answering answering) {
        try {
            answering.answer();
            answered.add(call + " answered");
        } catch (SQLException refused) {
            // What a connection that is gone owes its caller.
        }
    }

    @FunctionalInterface
    private interface Answering {
        Object answer() throws SQLException;
    }
}
