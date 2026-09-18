/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FetchSizeITest extends BaseIntegrationTest {

    private static final int FETCH_SIZE = 10;

    private static final String SUMMITS = "select * from sys.summits";

    @Test
    public void testFetchSizeWithQueryTimeout() throws Exception {
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

    @Test
    public void testCursorFetchesOutsideQueryTimeout() throws Exception {
        try (Connection connection = connect();
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.setFetchSize(FETCH_SIZE);
            statement.setQueryTimeout(30);
            String ownTimeout = sessionStatementTimeout(connection);
            try (ResultSet rs = statement.executeQuery(SUMMITS)) {
                assertThat(rs.getFetchSize(), is(FETCH_SIZE));
                assertThat(sessionStatementTimeout(connection), is(ownTimeout));
                assertThat(countBufferedRows(connection, rs), is(FETCH_SIZE));
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

    // With the connection aborted, only the rows already fetched can still be read.
    private static int countBufferedRows(Connection connection, ResultSet rs) throws SQLException {
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
