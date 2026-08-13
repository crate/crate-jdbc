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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the transaction surface of the crate:// driver. CrateDB has no
 * transactions: BEGIN and COMMIT parse as server-side no-ops, ROLLBACK is
 * not in its grammar at all. {@link Connection#rollback()} therefore undoes
 * nothing, so frameworks that call it during routine cleanup keep working —
 * while still rejecting the states JDBC forbids a rollback in, and still
 * leaving the connection in the state a rollback owes its caller. There is
 * one isolation level, and transactions and savepoints are reported as
 * unsupported.
 */
public class TransactionsIT extends BaseIntegrationTest {

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
    public void rollbackIsNoOpAndAllWritesPersist() throws Exception {
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
    public void commitSucceedsWithAutoCommitDisabled() throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            conn.createStatement().execute("insert into test (id, string_field) values (60, 'committed')");
            conn.commit();
            conn.setAutoCommit(true);
            conn.createStatement().execute("refresh table test");
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from test where id = 60");
            assertThat(rs.next(), is(true));
            assertThat(rs.getLong(1), is(1L));
        }
    }

    @Test
    public void rollbackToSavepointIsUnsupported() throws Exception {
        try (Connection conn = connect()) {
            Savepoint savepoint = new Savepoint() {
                @Override
                public int getSavepointId() {
                    return 1;
                }

                @Override
                public String getSavepointName() {
                    return "sp";
                }
            };
            assertThrows(SQLFeatureNotSupportedException.class, () -> conn.rollback(savepoint));
        }
    }

    @Test
    public void savepointsAreUnsupported() throws Exception {
        try (Connection conn = connect()) {
            SQLFeatureNotSupportedException unsupported = assertThrows(
                SQLFeatureNotSupportedException.class, conn::setSavepoint);
            assertThat(unsupported.getSQLState(), is("0A000"));

            assertThrows(SQLFeatureNotSupportedException.class, () -> conn.setSavepoint("sp"));
            assertThrows(SQLFeatureNotSupportedException.class, () -> conn.releaseSavepoint(null));
        }
    }

    @Test
    public void rollbackWithAutoCommitEnabledIsRejected() throws Exception {
        try (Connection conn = connect()) {
            SQLException rejected = assertThrows(SQLException.class, conn::rollback);
            assertThat(rejected.getSQLState(), is("25P01"));
        }
    }

    @Test
    public void rollbackOnAClosedConnectionIsRejected() throws Exception {
        Connection conn = connect();
        conn.setAutoCommit(false);
        conn.close();

        SQLException rejected = assertThrows(SQLException.class, conn::rollback);
        assertThat(rejected.getSQLState(), is("08003"));
    }

    @Test
    public void transactionsAreReportedAsUnsupported() throws Exception {
        try (Connection conn = connect()) {
            DatabaseMetaData metaData = conn.getMetaData();
            assertThat(metaData.supportsTransactions(), is(false));
            assertThat(metaData.supportsMultipleTransactions(), is(false));
            assertThat(metaData.supportsSavepoints(), is(false));
            assertThat(metaData.getDefaultTransactionIsolation(), is(Connection.TRANSACTION_NONE));
            // Nor either of the mixtures of schema changes and transactions
            // that a database supporting transactions would have to choose
            // between. Neither is on offer when neither half exists.
            assertThat(metaData.supportsDataDefinitionAndDataManipulationTransactions(), is(false));
            assertThat(metaData.supportsDataManipulationTransactionsOnly(), is(false));
        }
    }

    /**
     * The level {@code DatabaseMetaData} announces as the only supported one
     * is the level the connection is in and stays in. A framework that asks
     * for one of PostgreSQL's levels is not refused — there is nothing for the
     * server to do differently — but a level JDBC does not define is.
     *
     * <p>{@code TRANSACTION_NONE} is taken as well, which JDBC reserves for a
     * connection to report rather than for a caller to name: it is the level
     * this connection reports, and refusing it would refuse a framework its
     * own answer read back.</p>
     */
    @Test
    public void theOnlyIsolationLevelIsTheOneReportedAsSupported() throws Exception {
        try (Connection conn = connect()) {
            DatabaseMetaData metaData = conn.getMetaData();
            assertThat(metaData.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_NONE), is(true));
            assertThat(conn.getTransactionIsolation(), is(Connection.TRANSACTION_NONE));

            for (int level : new int[]{
                    Connection.TRANSACTION_READ_UNCOMMITTED,
                    Connection.TRANSACTION_READ_COMMITTED,
                    Connection.TRANSACTION_REPEATABLE_READ,
                    Connection.TRANSACTION_SERIALIZABLE}) {
                assertThat(metaData.supportsTransactionIsolationLevel(level), is(false));
                conn.setTransactionIsolation(level);
                assertThat(conn.getTransactionIsolation(), is(Connection.TRANSACTION_NONE));
            }

            conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
            assertThat(conn.getTransactionIsolation(), is(Connection.TRANSACTION_NONE));
            assertThrows(SQLException.class, () -> conn.setTransactionIsolation(999));
        }
    }

    /**
     * Under manual commit mode pgJDBC opens a transaction block, and until it
     * is ended the connection refuses to change its read-only flag or its
     * isolation level. A rollback has to end it, the way a commit does —
     * whether the block holds a write or a statement the server refused,
     * which is the state a caller reaches for a rollback in the first place.
     */
    @ParameterizedTest(name = "after {0}")
    @MethodSource("blockContents")
    public void rollbackLeavesTheConnectionReconfigurable(String description, String sql) throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            try {
                stmt.execute(sql);
            } catch (SQLException refused) {
                // The block is now failed, which is the case being covered.
            }
            conn.rollback();

            conn.setReadOnly(true);
            conn.setReadOnly(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_NONE);

            ResultSet rs = conn.createStatement().executeQuery("select 1");
            assertThat(rs.next(), is(true));
        }
    }

    static Stream<Arguments> blockContents() {
        return Stream.of(
            Arguments.of("a write", "insert into test (id, string_field) values (70, 'written')"),
            Arguments.of("a statement the server refused", "select * from no_such_table")
        );
    }
}
