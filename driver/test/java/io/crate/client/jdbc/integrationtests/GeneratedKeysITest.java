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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GeneratedKeysITest extends BaseIntegrationTest {

    private static final String INSERT = "insert into generated_keys (value) values (1)";

    private Connection connection;

    @BeforeEach
    public void setUpTable() throws SQLException {
        connection = DriverManager.getConnection(getConnectionString());
        connection.createStatement().execute(
            "create table generated_keys (id text default gen_random_text_uuid() primary key, value int)");
    }

    @Test
    public void testSupportsGetGeneratedKeys() throws SQLException {
        assertThat(connection.getMetaData().supportsGetGeneratedKeys(), is(true));
    }

    @Test
    public void testPreparedStatementReturnsGeneratedKeys() throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS)) {
            assertThat(stmt.executeUpdate(), is(1));
            assertGeneratedId(stmt.getGeneratedKeys());
        }
    }

    @ParameterizedTest
    @MethodSource("statementInserts")
    public void testStatementReturnsGeneratedKeys(String description, StatementInsert insert) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            insert.run(stmt);
            assertGeneratedId(stmt.getGeneratedKeys());
        }
    }

    static Stream<Arguments> statementInserts() {
        return Stream.of(
            Arguments.of("executeUpdate with a flag", insert(s -> s.executeUpdate(INSERT, Statement.RETURN_GENERATED_KEYS))),
            Arguments.of("executeUpdate with column names", insert(s -> s.executeUpdate(INSERT, new String[]{"id"}))),
            Arguments.of("executeLargeUpdate with a flag",
                insert(s -> s.executeLargeUpdate(INSERT, Statement.RETURN_GENERATED_KEYS))),
            Arguments.of("execute with a flag", insert(s -> s.execute(INSERT, Statement.RETURN_GENERATED_KEYS))));
    }

    @Test
    public void testColumnIndexesAreRefusedBeforeTheInsert() throws SQLException {
        SQLException e = assertThrows(SQLException.class, () -> connection.prepareStatement(INSERT, new int[]{1}));
        assertThat(e.getSQLState(), is("0A000"));
        e = assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(INSERT, new int[]{1}));
        assertThat(e.getSQLState(), is("0A000"));

        connection.createStatement().execute("refresh table generated_keys");
        ResultSet rs = connection.createStatement().executeQuery("select count(*) from generated_keys");
        assertThat(rs.next(), is(true));
        assertThat(rs.getLong(1), is(0L));
    }

    private static void assertGeneratedId(ResultSet keys) throws SQLException {
        assertThat(keys.next(), is(true));
        assertThat(keys.getString("id"), notNullValue());
    }

    @FunctionalInterface
    interface StatementInsert {
        void run(Statement statement) throws SQLException;
    }

    private static StatementInsert insert(StatementInsert insert) {
        return insert;
    }
}
