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

import io.crate.testing.CrateTestServer;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.util.Locale;
import java.util.Properties;

import static org.hamcrest.Matchers.is;

@Ignore("not implemented")
public class CrateJDBCByPassSpecSettingTest extends CrateJDBCIntegrationTest {

    private static String connectionString;
    private static Properties strictProperties = new Properties();

    @BeforeClass
    public static void beforeClass() throws Exception {
        CrateTestServer server = testCluster.randomServer();
        connectionString = String.format(Locale.ENGLISH, "crate://%s:%d", server.crateHost(), server.transportPort());
        strictProperties.put("strict", "true");
    }

    @Test
    public void testSetAutoCommitStrictFalse() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);

        connection.setAutoCommit(false);
        assertThat(connection.getAutoCommit(), is(true));

        connection.setAutoCommit(true);
        assertThat(connection.getAutoCommit(), is(true));
        connection.close();
    }

    @Test
    public void tesCommitStrictFalse() throws SQLException {
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
            expectedException.expectMessage("The auto-commit mode cannot be disabled. The Crate JDBC driver does not support manual commit.");
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
}
