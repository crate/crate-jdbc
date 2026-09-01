package io.crate.client.jdbc.integrationtests;

import io.crate.client.jdbc.CrateConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * What each thing an application asks of the driver costs the server, counted
 * rather than reasoned about.
 *
 * <p>A driver's round trips are invisible from the outside: a metadata call
 * that quietly asks the catalog one extra question, or a setting held around
 * an execution that grows from three statements to five, changes nothing a
 * test of behavior would notice and everything about what the driver costs
 * under load. CrateDB records every statement it ran in {@code sys.jobs_log},
 * so the cost is a number, and a number can be held.</p>
 *
 * <p>The counts here are what the driver does, not what pgJDBC does with a
 * query it composed: a catalog query is one statement whatever it says.</p>
 */
public class RoundTripCostIT extends BaseIntegrationTest {

    private static final String TABLE = "round_trip_cost";

    /**
     * Names the observer's own statements so they can be told from the ones
     * being counted. It has to appear in the statement text, which is all
     * {@code sys.jobs_log} keeps.
     */
    private static final String OBSERVER = "sys.jobs_log";

    /**
     * How long the log is given to catch up. Entries land when a statement
     * ends, and the reading of them is a statement of its own.
     */
    private static final long SETTLE_MILLIS = 300;

    private static Connection observer;

    @BeforeAll
    static void setUpTable() throws Exception {
        dropAllUserTables();
        observer = connect();
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute("create table " + TABLE + " (id integer primary key, name string) "
                + "clustered into 1 shards with (number_of_replicas = 0)");
            statement.execute("insert into " + TABLE + " (id, name) values (1, 'first')");
            statement.execute("refresh table " + TABLE);
        }
        ensureYellow();
    }

    @AfterAll
    static void dropTable() throws Exception {
        if (observer != null) {
            observer.close();
        }
        dropAllUserTables();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("operations")
    public void anOperationCostsTheStatementsItAlwaysHas(
            String description, int statements, Operation operation) throws Exception {
        List<String> ran = statementsRunBy(operation);
        assertThat(description + " ran " + ran.size() + " statements:" + numbered(ran),
            ran.size(), is(statements));
    }

    /**
     * A query timeout is CrateDB's {@code statement_timeout}, which belongs to
     * the session while the timeout belongs to one statement — so the driver
     * reads what the session holds, puts its own value in, and gives the old
     * one back. Three statements around each execution, and this is where that
     * price is stated as a number rather than as prose.
     */
    @Test
    public void aQueryTimeoutIsThreeStatementsAroundTheExecution() throws Exception {
        List<String> ran = statementsRunBy(conn -> {
            try (Statement statement = conn.createStatement()) {
                statement.setQueryTimeout(30);
                try (ResultSet rows = statement.executeQuery("select 1")) {
                    rows.next();
                }
            }
        });
        assertThat("the bracket:" + numbered(ran), ran.size(), is(4));
        assertThat(ran.get(0), containsString("statement_timeout"));
        assertThat(ran.get(1), is("set statement_timeout = '30000ms'"));
        assertThat(ran.get(2), is("select 1"));
        assertThat(ran.get(3), is("set statement_timeout = '0ms'"));
    }

    /** A statement that sets no timeout pays none of the bracket. */
    @Test
    public void aStatementWithNoTimeoutCostsOnlyItself() throws Exception {
        List<String> ran = statementsRunBy(conn -> {
            try (Statement statement = conn.createStatement();
                 ResultSet rows = statement.executeQuery("select 1")) {
                rows.next();
            }
        });
        assertThat("what ran:" + numbered(ran), ran, is(List.of("select 1")));
    }

    /**
     * The statements the server ran while the operation did, in order. The
     * connection is opened and warmed first, so that what is counted is the
     * operation rather than the opening of a connection to run it on.
     */
    private static List<String> statementsRunBy(Operation operation) throws Exception {
        try (Connection conn = connect()) {
            try (Statement statement = conn.createStatement();
                 ResultSet rows = statement.executeQuery("select 1")) {
                rows.next();
            }
            Thread.sleep(SETTLE_MILLIS);
            Timestamp start = serverTime();
            operation.on(conn);
            Thread.sleep(SETTLE_MILLIS);
            return statementsSince(start);
        }
    }

    /**
     * The server's own clock, since the log is stamped with it. Taken from a
     * statement that names the log, so that the reading of the clock is left
     * out of what follows it.
     */
    private static Timestamp serverTime() throws SQLException {
        try (Statement statement = observer.createStatement();
             ResultSet rows = statement.executeQuery(
                 "select current_timestamp from " + OBSERVER + " limit 1")) {
            rows.next();
            return rows.getTimestamp(1);
        }
    }

    private static List<String> statementsSince(Timestamp start) throws SQLException {
        List<String> ran = new ArrayList<>();
        try (PreparedStatement statement = observer.prepareStatement(
                 "select stmt from " + OBSERVER + " where started >= ? "
                 + "and stmt not like '%" + OBSERVER + "%' order by started, ended")) {
            statement.setTimestamp(1, start);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    ran.add(rows.getString(1));
                }
            }
        }
        return ran;
    }

    private static String numbered(List<String> statements) {
        StringBuilder listing = new StringBuilder();
        for (int i = 0; i < statements.size(); i++) {
            String statement = statements.get(i);
            listing.append("\n  ").append(i + 1).append(". ")
                .append(statement.length() > 120 ? statement.substring(0, 120) + " …" : statement);
        }
        return listing.toString();
    }

    @FunctionalInterface
    interface Operation {
        void on(Connection connection) throws Exception;
    }

    static Stream<Arguments> operations() {
        return Stream.of(
            Arguments.of("a query", 1, (Operation) conn -> {
                try (Statement statement = conn.createStatement();
                     ResultSet rows = statement.executeQuery("select id, name from " + TABLE)) {
                    rows.next();
                }
            }),
            Arguments.of("a prepared query", 1, (Operation) conn -> {
                try (PreparedStatement statement = conn.prepareStatement(
                         "select name from " + TABLE + " where id = ?")) {
                    statement.setInt(1, 1);
                    try (ResultSet rows = statement.executeQuery()) {
                        rows.next();
                    }
                }
            }),
            Arguments.of("an insert", 1, (Operation) conn -> {
                try (PreparedStatement statement = conn.prepareStatement(
                         "insert into " + TABLE + " (id, name) values (?, ?)")) {
                    statement.setInt(1, 2);
                    statement.setString(2, "second");
                    statement.execute();
                }
            }),
            Arguments.of("asking for the metadata", 0, (Operation) Connection::getMetaData),
            Arguments.of("listing tables", 1, (Operation) conn -> {
                try (ResultSet rows = conn.getMetaData().getTables(null, "doc", TABLE, null)) {
                    while (rows.next()) {
                        rows.getString("TABLE_NAME");
                    }
                }
            }),
            // Two, where listing tables is one: pgJDBC asks the server for the
            // current catalog before it runs the query that reads the columns.
            Arguments.of("listing columns", 2, (Operation) conn -> {
                try (ResultSet rows = conn.getMetaData().getColumns(null, "doc", TABLE, "%")) {
                    while (rows.next()) {
                        rows.getString("COLUMN_NAME");
                    }
                }
            }),
            Arguments.of("the CrateDB version", 1,
                (Operation) conn -> conn.unwrap(CrateConnection.class).getCrateVersion()),
            Arguments.of("the CrateDB version a second time", 1, (Operation) conn -> {
                CrateConnection crate = conn.unwrap(CrateConnection.class);
                crate.getCrateVersion();
                crate.getCrateVersion();
            }),
            Arguments.of("building an array", 0,
                (Operation) conn -> conn.createArrayOf("integer", new Integer[]{1, 2})));
    }
}
