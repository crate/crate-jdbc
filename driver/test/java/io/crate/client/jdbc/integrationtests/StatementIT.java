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
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The controls JDBC puts on a statement — a timeout, a row cap, escape
 * syntax, and the closing rules — against a real server. They are stock
 * pgJDBC behavior, which means they hold only as far as CrateDB answers the
 * protocol pgJDBC uses for them: a timeout, for instance, is a cancel request
 * sent over a second connection rather than anything the driver decides on
 * its own.
 */
public class StatementIT extends BaseIntegrationTest {

    /**
     * A cross join over {@link #ROWS} rows per side: 2.5 billion row
     * combinations, so no machine finishes it within {@link #TIMEOUT_SECONDS}.
     * The size is what makes the timeout the reason the query ends — a tenth
     * of it is answered well inside the timeout. The table matters as much: a
     * query over {@code sys} tables, which CrateDB answers from memory on the
     * thread serving the connection, holds that thread for its whole duration
     * and is bounded by neither the cancel request a timeout sends nor the
     * server's {@code statement_timeout}.
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
     * The abort reaches the caller as an ordinary {@code SQLException}, and the
     * connection stays in step with the server afterwards — a driver that
     * mishandled the cancel would leave the protocol stream desynchronized and
     * the next statement would fail or hang.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void queryTimeoutAbortsALongRunningQuery() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(TIMEOUT_SECONDS);
            assertThat(stmt.getQueryTimeout(), is(TIMEOUT_SECONDS));

            assertThrows(SQLException.class, () -> stmt.executeQuery(LONG_RUNNING));

            ResultSet rs = conn.createStatement().executeQuery("select 1");
            assertThat(rs.next(), is(true));
            assertThat(rs.getInt(1), is(1));
        }
    }

    /**
     * The timeout belongs to the statement that set it. The driver carries it
     * to the server as the session's {@code statement_timeout}, so the value
     * the session already held — nothing, or one the application set for its
     * own connection — is what it holds again afterwards.
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
                assertThrows(SQLException.class, () -> stmt.executeQuery(LONG_RUNNING));
            }

            assertThat(statementTimeout(conn), is(held));
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
     * A statement produces one result set per query it was given, which is
     * what {@code getMoreResults()} walks. CrateDB answers a query text
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

    @Test
    public void maxRowsLimitsTheRowsRead() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.setMaxRows(2);
            assertThat(stmt.getMaxRows(), is(2));

            int rows = 0;
            try (ResultSet rs = stmt.executeQuery("select unnest([1, 2, 3, 4, 5])")) {
                while (rs.next()) {
                    rows++;
                }
            }
            assertThat(rows, is(2));
        }
    }

    /**
     * The escape syntax JDBC defines for portable SQL, which reporting tools
     * emit. pgJDBC rewrites it into the server's own dialect before sending.
     */
    @Test
    public void jdbcEscapesAreTranslatedBeforeTheServerSeesThem() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select {fn ucase('abc')}, {ts '2024-01-02 03:04:05'}")) {
            assertThat(rs.next(), is(true));
            assertThat(rs.getString(1), is("ABC"));
            assertThat(rs.getTimestamp(2).toString(), is("2024-01-02 03:04:05.0"));
        }
    }

    @Test
    public void escapesReachTheServerVerbatimWhenTranslationIsOff() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.setEscapeProcessing(false);
            assertThrows(SQLException.class, () -> stmt.executeQuery("select {fn ucase('abc')}"));
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

    @Test
    public void closeOnCompletionClosesTheStatementWithItsResultSet() throws Exception {
        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            stmt.closeOnCompletion();
            assertThat(stmt.isCloseOnCompletion(), is(true));

            ResultSet rs = stmt.executeQuery("select 1");
            assertThat(rs.next(), is(true));
            rs.close();

            assertThat(stmt.isClosed(), is(true));
        }
    }
}
