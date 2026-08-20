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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * What a query timeout and a cursor do to each other. A fetch size under
 * manual commit leaves the rows on the server, to arrive one batch at a time,
 * and the timeout the driver applies is the session's {@code statement_timeout}
 * — a setting that would bound the fetches as readily as the execution that
 * opened the cursor.
 *
 * <p>How many rows the client holds is read off the rows themselves: once the
 * connection is aborted, only what was already fetched can still be
 * iterated.</p>
 */
public class FetchSizeIT extends BaseIntegrationTest {

    private static final int FETCH_SIZE = 10;

    private static final String SUMMITS = "select * from sys.summits";

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
     *
     * <p>That the client holds a batch and not the whole result set is what
     * makes this and the test above it say anything: both would pass on a
     * driver that never batched at all.</p>
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
                assertThat(rs.getFetchSize(), is(FETCH_SIZE));
                assertThat(sessionStatementTimeout(connection), is(ownTimeout));
                assertThat(rowsHeldByTheClient(connection, rs), is(FETCH_SIZE));
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
     * reach, exactly how many it had read ahead.
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
