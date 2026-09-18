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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UnsupportedFeaturesITest extends BaseIntegrationTest {

    private static final String INSERT = "insert into nowhere (id) values (1)";

    private static Connection conn;

    @BeforeAll
    static void openConnection() throws SQLException {
        conn = connect();
    }

    @AfterAll
    static void closeConnection() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void testPrepareCall() {
        expectUnsupportedFeature(
            () -> conn.prepareCall("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    }

    @Test
    public void testPrepareStatementWithColumnNames() {
        expectUnsupportedFeature(() -> conn.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS));
        expectUnsupportedFeature(() -> conn.prepareStatement(INSERT, new int[]{1}));
        expectUnsupportedFeature(() -> conn.prepareStatement(INSERT, new String[]{"id"}));
    }

    @Test
    public void testExecuteUpdateNotSupported() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            expectUnsupportedFeature(() -> stmt.execute(INSERT, Statement.RETURN_GENERATED_KEYS));
            expectUnsupportedFeature(() -> stmt.execute(INSERT, new int[]{1}));
            expectUnsupportedFeature(() -> stmt.execute(INSERT, new String[]{"id"}));
            expectUnsupportedFeature(() -> stmt.executeUpdate(INSERT, Statement.RETURN_GENERATED_KEYS));
            expectUnsupportedFeature(() -> stmt.executeUpdate(INSERT, new int[]{1}));
            expectUnsupportedFeature(() -> stmt.executeUpdate(INSERT, new String[]{"id"}));
            expectUnsupportedFeature(stmt::getGeneratedKeys);
        }
    }

    private static void expectUnsupportedFeature(Executable call) {
        SQLFeatureNotSupportedException refused =
            assertThrows(SQLFeatureNotSupportedException.class, call);
        assertThat(refused.getSQLState(), is("0A000"));
    }
}
