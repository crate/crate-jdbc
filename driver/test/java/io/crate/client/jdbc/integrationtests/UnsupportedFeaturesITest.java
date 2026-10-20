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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UnsupportedFeaturesITest extends BaseIntegrationTest {

    private static Connection connection;

    @BeforeAll
    public static void beforeClass() throws SQLException, InterruptedException {
        connection = DriverManager.getConnection(getConnectionString());
    }

    @Test
    public void testPrepareCall() throws SQLException {
        expectUnsupportedFeature("Connection: prepareCall(String sql, int resultSetType, int resultSetConcurrency) not supported",
            () -> connection.prepareCall("select *", 0, 0));
    }

    @Test
    public void testPrepareStatementWithColumnNames() throws SQLException {
        expectUnsupportedFeature("Connection: prepareStatement(String sql, String[] columnNames) not supported",
            () -> connection.prepareStatement("select *", new String[]{"id"}));
    }

    @Test
    public void testExecuteUpdateNotSupported() throws SQLException {
        expectUnsupportedFeature("Statement: executeUpdate(String sql, String[] columnNames) not supported",
            () -> connection.createStatement().executeUpdate("insert into test (id, name) values (3, 'hello')", new String[]{"id", "name"}));
    }

    @Test
    public void testPrepareCallSQLState() {
        expectNotImplemented(
            () -> connection.prepareCall("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    }

    @Test
    public void testPrepareStatementOverloadsRefused() {
        String insert = "insert into test (id) values (1)";
        expectNotImplemented(() -> connection.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS));
        expectNotImplemented(() -> connection.prepareStatement(insert, new int[]{1}));
        expectNotImplemented(() -> connection.prepareStatement(insert, new String[]{"id"}));
    }

    @Test
    public void testStatementOverloadsRefused() throws SQLException {
        String insert = "insert into test (id) values (1)";
        try (Statement stmt = connection.createStatement()) {
            expectNotImplemented(() -> stmt.execute(insert, Statement.RETURN_GENERATED_KEYS));
            expectNotImplemented(() -> stmt.execute(insert, new int[]{1}));
            expectNotImplemented(() -> stmt.execute(insert, new String[]{"id"}));
            expectNotImplemented(() -> stmt.executeUpdate(insert, Statement.RETURN_GENERATED_KEYS));
            expectNotImplemented(() -> stmt.executeUpdate(insert, new int[]{1}));
            expectNotImplemented(() -> stmt.executeUpdate(insert, new String[]{"id"}));
            expectNotImplemented(stmt::getGeneratedKeys);
        }
    }

    private void expectNotImplemented(Executable executable) {
        SQLFeatureNotSupportedException e = assertThrows(SQLFeatureNotSupportedException.class, executable);
        assertThat(e.getSQLState(), is("0A000"));
    }

    private void expectUnsupportedFeature(String errorMessage, Executable executable) {
        SQLFeatureNotSupportedException e = assertThrows(SQLFeatureNotSupportedException.class, executable);
        assertThat(e.getMessage(), containsString(errorMessage));
    }
}
