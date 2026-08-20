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

import io.crate.client.jdbc.CratePreparedStatement;
import io.crate.client.jdbc.CrateStatement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A statement against a real server: what a query timeout costs the session
 * it ran in, how many result sets one statement hands back, and what a closed
 * statement and its result set still answer.
 *
 * <p>A timeout is the driver's own. CrateDB never receives the cancel request
 * pgJDBC sends over a second connection in time to end a query with it, so the
 * driver arms the session's {@code statement_timeout} as well, and owes the
 * session the setting it found.</p>
 */
public class StatementIT extends BaseIntegrationTest {

    /**
     * A cross join over {@link #ROWS} rows per side: 2.5 billion row
     * combinations, so no machine finishes it within {@link #TIMEOUT_SECONDS}
     * and the timeout is the reason it ends. A tenth of it answers well inside
     * the timeout.
     *
     * <p>A user table is what makes the query bounded at all. CrateDB answers a
     * query over {@code sys} tables inline instead of dispatching it, and never
     * arms {@code statement_timeout} for one, so such a query would end only if
     * the cancel request happened to land.</p>
     */
    private static final String LONG_RUNNING = "select count(*) from numbers a, numbers b";

    private static final int ROWS = 50_000;

    private static final int TIMEOUT_SECONDS = 3;

    @BeforeAll
    static void createNumbers() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("create table numbers (x int) clustered into 4 shards"
                         + " with (number_of_replicas = 0)");
            stmt.execute("insert into numbers (x) (select g from generate_series(1, " + ROWS + ") as g)");
            stmt.execute("refresh table numbers");
        }
        ensureYellow();
    }

    @AfterAll
    static void dropNumbers() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("drop table numbers");
        }
    }

    /**
     * A timeout ends a query that outruns it, and the timeout belongs to the
     * statement that set it: the value the session already held is what it
     * holds again afterwards, whether that is nothing or one the application
     * set for its own connection.
     *
     * <p>{@code 90s} is there because CrateDB prints it as {@code 1.5m} and
     * rejects {@code 1.5m}, so a setting given back in the spelling it was read
     * in would not survive.</p>
     */
    @ParameterizedTest
    @ValueSource(strings = {"0s", "90s"})
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void aQueryTimeoutLeavesTheSessionSettingAsItFoundIt(String sessionTimeout) throws Exception {
        try (Connection conn = connect()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("set statement_timeout = '" + sessionTimeout + "'");
            }
            String held = statementTimeout(conn);

            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(TIMEOUT_SECONDS);
                assertThat(stmt.getQueryTimeout(), is(TIMEOUT_SECONDS));
                assertThrows(SQLException.class, () -> stmt.executeQuery(LONG_RUNNING));
            }

            assertThat(statementTimeout(conn), is(held));
            // The connection is still in step with the server: a driver that
            // mishandled the cancel would leave the protocol stream
            // desynchronized and this would fail or hang.
            try (ResultSet rs = conn.createStatement().executeQuery("select 1")) {
                assertThat(rs.next(), is(true));
                assertThat(rs.getInt(1), is(1));
            }
        }
    }

    private static String statementTimeout(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "select setting from pg_settings where name = 'statement_timeout'")) {
            assertThat(rs.next(), is(true));
            return rs.getString(1);
        }
    }

    /**
     * Every call that runs something on the server is bracketed by the
     * statement's query timeout, and each of them answers for what it ran:
     * a query says it has rows, an insert says it has none, and a count of
     * the rows written fits either width. The generated keys these calls also
     * ask for are {@link CrudBatchIT}'s.
     */
    @Test
    public void everyExecutionAnswersForWhatItRan() throws Exception {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("create table keyed (id integer primary key)"
                + " clustered into 1 shards with (number_of_replicas=0)");
            try {
                String insert = "insert into keyed (id) values (";
                assertThat(stmt.execute(insert + "1)", Statement.RETURN_GENERATED_KEYS), is(false));
                assertThat(stmt.execute(insert + "2)", new String[]{"id"}), is(false));
                assertThat(stmt.executeUpdate(insert + "3)", Statement.RETURN_GENERATED_KEYS), is(1));
                assertThat(stmt.executeUpdate(insert + "4)", new String[]{"id"}), is(1));
                assertThat(stmt.executeLargeUpdate(insert + "5)", Statement.RETURN_GENERATED_KEYS), is(1L));
                assertThat(stmt.executeLargeUpdate(insert + "6)", new String[]{"id"}), is(1L));

                stmt.execute("refresh table keyed");
                assertThat(stmt.execute("select id from keyed", Statement.NO_GENERATED_KEYS), is(true));
                assertThat(stmt.execute("select id from keyed", new String[0]), is(true));
            } finally {
                stmt.execute("drop table keyed");
            }
        }
    }

    /**
     * Naming generated keys by column position is refused rather than run:
     * pgJDBC names them in the SQL it rewrites, and a position is not a name.
     * The refusal arrives through the same three calls, which is where a
     * caller meets it.
     */
    @Test
    public void namingGeneratedKeysByPositionIsRefused() throws Exception {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            String insert = "insert into keyed (id) values (1)";
            int[] byPosition = {1};

            assertThat(assertThrows(SQLException.class, () -> stmt.execute(insert, byPosition))
                .getSQLState(), is("0A000"));
            assertThrows(SQLException.class, () -> stmt.executeUpdate(insert, byPosition));
            assertThrows(SQLException.class, () -> stmt.executeLargeUpdate(insert, byPosition));
        }
    }

    /**
     * A statement produces one result set per query it was given, and
     * {@code getMoreResults()} walks them. CrateDB answers a query text
     * holding several statements with several result sets, the way
     * {@code DatabaseMetaData.supportsMultipleResultSets()} announces.
     */
    @Test
    public void resultSetsFollowTheQueriesTheStatementWasGiven() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            assertThat(stmt.execute("select 1"), is(true));
            assertThat(stmt.getMoreResults(), is(false));

            List<Integer> read = new ArrayList<>();
            for (boolean results = stmt.execute("select 1; select 2"); results; results = stmt.getMoreResults()) {
                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next()) {
                        read.add(rs.getInt(1));
                    }
                }
            }
            assertThat(read, contains(1, 2));
        }
    }

    /**
     * Closing a statement closes what it handed out, and both then refuse to
     * be used — the wrappers hold no state of their own that would answer
     * after the object underneath is gone.
     */
    @Test
    public void aClosedStatementAndItsResultSetRefuseToBeUsed() throws Exception {
        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select 1");
            stmt.close();

            assertThat(stmt.isClosed(), is(true));
            assertThat(rs.isClosed(), is(true));

            assertThrows(SQLException.class, () -> stmt.executeQuery("select 1"));
            assertThrows(SQLException.class, () -> stmt.execute("select 1"));
            assertThrows(SQLException.class, rs::next);
            assertThrows(SQLException.class, () -> rs.getInt(1));
            assertThrows(SQLException.class, () -> rs.getObject(1));
            assertThrows(SQLException.class, () -> rs.getArray(1));
        }
    }

    /**
     * A statement asked for with a scroll type, a concurrency or a holdability
     * is still one of this driver's, so the CrateDB behavior does not depend on
     * how a caller asked for it. Most frameworks configure their statements,
     * and would otherwise be handed pgJDBC's.
     */
    @Test
    public void statementsAskedForWithOptionsAreStillThisDriversOwn() throws Exception {
        try (Connection conn = connect()) {
            assertThat(conn.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE),
                is(instanceOf(CrateStatement.class)));
            assertThat(conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT),
                is(instanceOf(CrateStatement.class)));
            assertThat(conn.prepareStatement("select 1",
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY),
                is(instanceOf(CratePreparedStatement.class)));
            assertThat(conn.prepareStatement("select 1",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                    ResultSet.CLOSE_CURSORS_AT_COMMIT),
                is(instanceOf(CratePreparedStatement.class)));
            assertThat(conn.prepareStatement("select 1", Statement.RETURN_GENERATED_KEYS),
                is(instanceOf(CratePreparedStatement.class)));
            assertThat(conn.prepareStatement("select 1", new String[]{"x"}),
                is(instanceOf(CratePreparedStatement.class)));
        }
    }

}
