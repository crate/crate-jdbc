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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

/**
 * Pins cursor-based fetching through the crate:// driver. A fetch size is
 * honored only while autoCommit is disabled: with manual commit the rows
 * stay on the server and arrive one batch at a time, with autoCommit the
 * whole result set is read into the client at once.
 *
 * <p>Which of the two happened is read off the rows themselves: once the
 * connection is aborted, only what the client already holds can still be
 * iterated — one batch for a cursor, everything for a materialized result
 * set.</p>
 */
public class FetchSizeIT extends BaseIntegrationTest {

    private static final int FETCH_SIZE = 10;

    private static final String SUMMITS = "select * from sys.summits";

    /**
     * Cursor-based fetching holds for a prepared statement as it does for a
     * plain one: what brackets an execution is written once, on the statement
     * wrapper both are built on.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("statements")
    public void fetchSizeBatchesRowsUnderManualCommit(String description, Query query) throws Exception {
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try (Statement statement = query.prepare(connection)) {
                statement.setFetchSize(FETCH_SIZE);
                ResultSet rs = query.run(statement);
                assertThat(rs.getFetchSize(), is(FETCH_SIZE));
                assertThat(rowsHeldByTheClient(connection, rs), is(FETCH_SIZE));
            }
        }
    }

    static Stream<Arguments> statements() {
        return Stream.of(
            Arguments.of("Statement", new Query() {
                @Override
                public Statement prepare(Connection connection) throws SQLException {
                    return connection.createStatement();
                }

                @Override
                public ResultSet run(Statement statement) throws SQLException {
                    return statement.executeQuery(SUMMITS);
                }
            }),
            Arguments.of("PreparedStatement", new Query() {
                @Override
                public Statement prepare(Connection connection) throws SQLException {
                    return connection.prepareStatement(SUMMITS);
                }

                @Override
                public ResultSet run(Statement statement) throws SQLException {
                    return ((PreparedStatement) statement).executeQuery();
                }
            })
        );
    }

    interface Query {
        Statement prepare(Connection connection) throws SQLException;

        ResultSet run(Statement statement) throws SQLException;
    }

    @Test
    public void fetchSizeIgnoredWithAutoCommit() throws Exception {
        int summits;
        try (Connection counting = connect()) {
            summits = countSummits(counting);
        }
        try (Connection connection = connect();
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            statement.setFetchSize(FETCH_SIZE);
            statement.execute(SUMMITS);
            ResultSet rs = statement.getResultSet();
            assertThat(rs.getFetchSize(), is(FETCH_SIZE));
            assertThat(rowsHeldByTheClient(connection, rs), is(summits));
        }
    }

    /**
     * Batches follow one another until the rows run out: what a cursor is for
     * is reading a result set larger than the client wants to hold.
     */
    @Test
    public void everyBatchIsFetchedUntilTheRowsRunOut() throws Exception {
        int summits;
        try (Connection counting = connect()) {
            summits = countSummits(counting);
        }
        try (Connection connection = connect();
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.setFetchSize(FETCH_SIZE);
            try (ResultSet rs = statement.executeQuery(SUMMITS)) {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                assertThat(rows, is(summits));
                assertThat(summits, greaterThan(FETCH_SIZE));
            }
        }
    }

    /**
     * A query timeout is applied around the execution and taken off again
     * afterwards, which happens while a cursor is open on the same
     * connection. The rows keep coming.
     */
    @Test
    public void aQueryTimeoutLeavesTheCursorIntact() throws Exception {
        int summits;
        try (Connection counting = connect()) {
            summits = countSummits(counting);
        }
        try (Connection connection = connect();
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.setFetchSize(FETCH_SIZE);
            statement.setQueryTimeout(30);
            try (ResultSet rs = statement.executeQuery(SUMMITS)) {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                assertThat(rows, is(summits));
            }
        }
    }

    /**
     * The timeout bounds the execution that opens the cursor, and no more: it
     * is off the session again by the time the first batch is in the client's
     * hands, so the fetches that bring the rest of the rows run unbounded.
     */
    @Test
    public void aCursorFetchesItsRowsOutsideTheQueryTimeout() throws Exception {
        try (Connection connection = connect();
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.setFetchSize(FETCH_SIZE);
            statement.setQueryTimeout(30);
            String ownTimeout = sessionStatementTimeout(connection);
            try (ResultSet rs = statement.executeQuery(SUMMITS)) {
                assertThat(rs.next(), is(true));
                assertThat(sessionStatementTimeout(connection), is(ownTimeout));
            }
        }
    }

    private static String sessionStatementTimeout(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                 "select setting from pg_settings where name = 'statement_timeout'")) {
            rs.next();
            return rs.getString(1);
        }
    }

    /**
     * How many rows the result set can still produce with the server out of
     * reach, which is exactly how many it had read ahead.
     */
    private static int rowsHeldByTheClient(Connection connection, ResultSet rs) throws SQLException {
        connection.abort(Runnable::run);
        int rows = 0;
        try {
            while (rs.next()) {
                rows++;
            }
        } catch (SQLException endOfWhatWasBuffered) {
            return rows;
        }
        return rows;
    }

    private static int countSummits(Connection connection) throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select count(*) from sys.summits");
        rs.next();
        return rs.getInt(1);
    }
}
