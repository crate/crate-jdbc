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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Pins the row count a statement reports: a statement that changes rows
 * reports how many, and CrateDB counts a DDL statement as one.
 *
 * <p>JDBC has two ways to ask, one answering an {@code int} and one a
 * {@code long} for counts that would not fit in one. Each brackets the
 * execution with the statement's query timeout on its own, so they are two
 * paths rather than one calling the other, and every case here is asserted
 * through both.</p>
 */
public class RowCountIT extends BaseIntegrationTest {

    @FunctionalInterface
    interface UpdateCount {
        long of(Statement statement, String sql) throws SQLException;
    }

    static Stream<Arguments> updateForms() {
        return Stream.of(
            Arguments.of("executeUpdate", (UpdateCount) Statement::executeUpdate),
            Arguments.of("executeLargeUpdate", (UpdateCount) Statement::executeLargeUpdate)
        );
    }

    @BeforeEach
    void setUpTables() {
        dropAllUserTables();
    }

    @AfterEach
    void tearDownTables() {
        dropAllUserTables();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("updateForms")
    public void ddlReportsOneAffectedRow(String form, UpdateCount count) throws Exception {
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            assertThat(count.of(statement, "create table t (id int)"), is(1L));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("updateForms")
    public void aCountedDeleteReportsTheRowsItRemoved(String form, UpdateCount count) throws Exception {
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute("create table t (id int)");
            statement.execute("insert into t (id) values (1), (2), (3)");
            statement.execute("refresh table t");

            assertThat(count.of(statement, "delete from t where id < 3"), is(2L));
        }
    }

    /** A prepared statement answers the same, at either width. */
    @Test
    public void aPreparedStatementReportsTheSameCountAtEitherWidth() throws Exception {
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute("create table t (id int)");

            try (PreparedStatement insert = conn.prepareStatement("insert into t (id) values (?)")) {
                insert.setInt(1, 1);
                assertThat(insert.executeUpdate(), is(1));
                insert.setInt(1, 2);
                assertThat(insert.executeLargeUpdate(), is(1L));
            }
        }
    }
}
