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
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * CrateDB has no transactions: BEGIN and COMMIT are accepted and ignored, and
 * ROLLBACK is not in its grammar.
 */
public class ByPassSpecSettingITest extends BaseIntegrationTest {

    @BeforeEach
    void setUpTables() throws Exception {
        dropAllUserTables();
        setUpTestTable();
    }

    @AfterEach
    void tearDownTables() {
        dropAllUserTables();
    }

    @Test
    public void testRollbackStrictFalse() throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.execute("insert into test (id, string_field) values (50, 'committed')");
            conn.commit();
            stmt.execute("insert into test (id, string_field) values (51, 'rolled-back')");
            conn.rollback();
            conn.setAutoCommit(true);
            conn.createStatement().execute("refresh table test");
            ResultSet rs = conn.createStatement().executeQuery(
                "select count(*) from test where id in (50, 51)");
            assertThat(rs.next(), is(true));
            assertThat(rs.getLong(1), is(2L));
        }
    }

    @Test
    public void testSavepointStrictFalse() throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            Savepoint savepoint = conn.setSavepoint("savepoint");
            conn.releaseSavepoint(savepoint);
            conn.setAutoCommit(true);

            ResultSet rs = conn.createStatement().executeQuery("select 1");
            assertThat(rs.next(), is(true));
        }
    }

    @ParameterizedTest(name = "{0} after {1}")
    @MethodSource("blockEnds")
    public void testConnectionReusableAfterBlockEnds(String end, String description, String sql)
            throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            try {
                stmt.execute(sql);
            } catch (SQLException refused) {
                // A refused statement leaves the block failed, which is one of the cases.
            }
            if (end.equals("commit")) {
                conn.commit();
            } else {
                conn.rollback();
            }

            conn.setReadOnly(true);
            conn.setReadOnly(false);
            ResultSet rs = conn.createStatement().executeQuery("select 1");
            assertThat(rs.next(), is(true));
        }
    }

    static Stream<Arguments> blockEnds() {
        return Stream.of("commit", "rollback").flatMap(end -> Stream.of(
            Arguments.of(end, "a write", "insert into test (id, string_field) values (70, 'written')"),
            Arguments.of(end, "a statement the server refused", "select * from no_such_table")));
    }

    @Test
    public void testRollbackOnClosedConnection() throws Exception {
        Connection conn = connect();
        conn.setAutoCommit(false);
        conn.close();

        SQLException rejected = assertThrows(SQLException.class, conn::rollback);
        assertThat(rejected.getSQLState(), is("08003"));
    }

    @Test
    public void testTransactionMetaDataStrictFalse() throws Exception {
        try (Connection conn = connect()) {
            DatabaseMetaData metaData = conn.getMetaData();
            assertThat(metaData.supportsTransactions(), is(true));
            assertThat(metaData.supportsDataDefinitionAndDataManipulationTransactions(), is(true));
            assertThat(metaData.getDefaultTransactionIsolation(), is(Connection.TRANSACTION_READ_COMMITTED));
            assertThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE), is(false));
            assertThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED), is(true));
        }
    }

    @Test
    public void testTransactionMetaDataStrictTrue() throws Exception {
        try (Connection conn = connectWith("strict", "true")) {
            DatabaseMetaData metaData = conn.getMetaData();
            assertThat(metaData.supportsTransactions(), is(false));
            assertThat(metaData.supportsDataDefinitionAndDataManipulationTransactions(), is(false));
            assertThat(metaData.getDefaultTransactionIsolation(), is(Connection.TRANSACTION_NONE));
            assertThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE), is(true));
            assertThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED), is(false));
        }
    }

    @Test
    public void testTransactionCallsStrictTrue() throws Exception {
        try (Connection conn = connectWith("strict", "true")) {
            List<Executable> calls = List.of(
                () -> conn.setAutoCommit(false),
                conn::commit,
                conn::rollback,
                () -> conn.rollback((Savepoint) null),
                () -> conn.setSavepoint("sp"),
                () -> conn.releaseSavepoint(null),
                () -> conn.setReadOnly(true));
            for (Executable call : calls) {
                SQLFeatureNotSupportedException refused =
                    assertThrows(SQLFeatureNotSupportedException.class, call);
                assertThat(refused.getSQLState(), is("0A000"));
            }
            conn.setAutoCommit(true);
            assertThat(conn.getAutoCommit(), is(true));
        }
    }

    @Test
    public void testGetConnectionStrictTrueReadOnlyTrue() {
        assertThrows(SQLException.class, () -> connectWith("strict", "true", "readOnly", "true").close());
    }
}
