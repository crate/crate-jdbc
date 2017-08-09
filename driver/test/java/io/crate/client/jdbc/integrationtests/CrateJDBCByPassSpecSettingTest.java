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

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.hamcrest.Matchers.is;

import org.postgresql.util.PSQLException;

public class CrateJDBCByPassSpecSettingTest extends CrateJDBCIntegrationTest {

    private static Properties strictProperties = new Properties();
    private static String connectionString;

    @BeforeClass
    public static void beforeClass() {
        strictProperties.put("strict", "true");
        connectionString = getConnectionString();
    }

    @Test
    public void testSetAutoCommitStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);

        connection.setAutoCommit(false);
        assertThat(connection.getAutoCommit(), is(false));

        connection.setAutoCommit(true);
        assertThat(connection.getAutoCommit(), is(true));
        connection.close();
    }

    @Test
    public void testCommitStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        connection.commit();
        assertThat(connection.getAutoCommit(), is(true));
        connection.close();
    }

    @Test
    public void testSavepointStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        Savepoint savepoint = connection.setSavepoint("savepoint");
        connection.releaseSavepoint(savepoint);
        connection.close();
    }

    @Test
    public void testRollbackStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        connection.rollback();
        connection.rollback(null);
        connection.close();
    }

    @Test
    public void testSetAutoToTrueCommitStrictTrue() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString, strictProperties);
        connection.setAutoCommit(true);
        assertThat(connection.getAutoCommit(), is(true));
        connection.close();
    }

    @Test
    public void testSetAutoCommitToFalseStrictTrue() throws SQLException {
        try(Connection connection = DriverManager.getConnection(connectionString, strictProperties)) {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("The auto-commit mode cannot be disabled in strict mode. The Crate JDBC driver does not support manual commit.");
            connection.setAutoCommit(false);
        }
    }

    @Test
    public void testCommitWhenAutoCommitIsTrueStrictTrue() throws SQLException {
        try(Connection connection = DriverManager.getConnection(connectionString, strictProperties)) {
            connection.setAutoCommit(true);
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("The commit operation is not allowed. The Crate JDBC driver does not support manual commit.");
            connection.commit(); // cannot commit in a strict mode if auto-commit is set to true
        }
    }

    @Test
    public void testSetSavepointStrictTrue() throws SQLException {
        try(Connection connection = DriverManager.getConnection(connectionString, strictProperties)) {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("Savepoint is not supported.");
            connection.setSavepoint("savepoint");
        }
    }

    @Test
    public void testRollbackSavepointStrictTrue() throws SQLException {
        try(Connection connection = DriverManager.getConnection(connectionString, strictProperties)) {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("Rollback is not supported.");
            connection.rollback(null);
        }
    }

    @Test
    public void testReleaseSavepointStrictTrue() throws SQLException {
        try(Connection connection = DriverManager.getConnection(connectionString, strictProperties)) {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("Savepoint is not supported.");
            connection.releaseSavepoint(null);
        }
    }

    @Test
    public void testRollbackStrictTrue() throws SQLException {
        try(Connection connection = DriverManager.getConnection(connectionString, strictProperties)) {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("Rollback is not supported.");
            connection.rollback();
        }
    }

    @Test
    public void testSupportsTransactionsStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        DatabaseMetaData metadata = connection.getMetaData();
        assertThat(metadata.supportsTransactions(), is (true));
        connection.close();
    }

    @Test
    public void testSupportsTransactionsStrictTrue() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString, strictProperties);
        DatabaseMetaData metadata = connection.getMetaData();
        assertThat(metadata.supportsTransactions(), is (false));
        connection.close();
    }

    @Test
    public void testGetDefaultTransactionIsolationReadCommittedStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        DatabaseMetaData metadata = connection.getMetaData();
        assertThat(metadata.getDefaultTransactionIsolation(), is (Connection.TRANSACTION_READ_COMMITTED));
        connection.close();
    }

    @Test
    public void testGetDefaultTransactionIsolationStrictTrue() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString, strictProperties);
        DatabaseMetaData metadata = connection.getMetaData();
        assertThat(metadata.getDefaultTransactionIsolation(), is (Connection.TRANSACTION_NONE));
        connection.close();
    }

    @Test
    public void testSupportsTransactionIsolationLevelStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        DatabaseMetaData metadata = connection.getMetaData();
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE), is (false));
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED), is (true));
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED), is (true));
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ), is (true));
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE), is (true));
        connection.close();
    }

    @Test
    public void testSupportsTransactionIsolationLevelStrictTrue() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString, strictProperties);
        DatabaseMetaData metadata = connection.getMetaData();
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE), is (true));
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED), is (false));
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED), is (false));
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ), is (false));
        assertThat(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE), is (false));
        connection.close();
    }

    @Test
    public void testSupportsDataDefinitionAndDataManipulationTransactionsStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        DatabaseMetaData metadata = connection.getMetaData();
        assertThat(metadata.supportsDataDefinitionAndDataManipulationTransactions(), is (true));
        connection.close();
    }

    @Test
    public void testSupportsDataDefinitionAndDataManipulationTransactionsStrictTrue() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString, strictProperties);
        DatabaseMetaData metadata = connection.getMetaData();
        assertThat(metadata.supportsDataDefinitionAndDataManipulationTransactions(), is (false));
        connection.close();
    }

    @Test
    public void testSetReadOnlyStrictTrue() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString,
            strictProperties)) {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage(
                "Setting transaction isolation READ ONLY not supported.");
            connection.setReadOnly(true);
        }
    }

    @Test
    public void testGetConnectionStrictTrueReadOnlyTrue() throws Exception {
        Properties readOnlyStrictProperties = new Properties();
        readOnlyStrictProperties.setProperty("strict", "true");
        readOnlyStrictProperties.setProperty("readOnly", "true");

        expectedException.expect(PSQLException.class);
        expectedException.expectMessage("Read-only connections are not supported.");
        DriverManager.getConnection(connectionString, readOnlyStrictProperties);
    }

}

