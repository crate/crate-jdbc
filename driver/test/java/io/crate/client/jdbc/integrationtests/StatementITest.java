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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StatementITest extends BaseIntegrationTest {

    // Neither a cancel request nor statement_timeout ends a query over sys tables,
    // which CrateDB answers inline.
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

    @ParameterizedTest
    @ValueSource(strings = {"0s", "90s"})
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testQueryTimeoutRestoresSessionSetting(String sessionTimeout) throws Exception {
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
            // A mishandled cancel leaves the protocol stream out of step here.
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

    @Test
    public void testGetMoreResults() throws Exception {
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
    public void testClosedStatement() throws Exception {
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

}
