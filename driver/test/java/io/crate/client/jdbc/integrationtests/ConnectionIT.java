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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins connection-level behavior of the crate:// driver: schema selection,
 * prepared statements, batch execution, multi-host URLs and single result
 * set semantics.
 */
public class ConnectionIT extends BaseIntegrationTest {

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
    public void customSchemaAppliesToStatements() throws SQLException, InterruptedException {
        try (Connection conn = connect()) {
            conn.setSchema("foo");
            assertThat(conn.getSchema(), is("foo"));

            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE t (name STRING) WITH (number_of_replicas=0)");
            ensureYellow();

            ResultSet rs = stmt.executeQuery(
                "SELECT table_schema " +
                "FROM information_schema.TABLES " +
                "WHERE table_name = 't'"
            );

            assertThat(rs.next(), is(true));
            assertThat(rs.getObject(1), is("foo"));
            assertThat(rs.next(), is(false));
        }
    }

    @Test
    public void customSchemaAppliesToPreparedStatements() throws Exception {
        try (Connection conn = connect()) {
            conn.setSchema("bar");

            PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE t (id INTEGER) WITH (number_of_replicas=0)");
            assertThat(stmt.execute(), is(false));
            ensureYellow();

            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT table_schema " +
                "FROM information_schema.TABLES " +
                "WHERE table_name = 't'"
            );

            assertThat(rs.next(), is(true));
            assertThat(rs.getObject(1), is("bar"));
            assertThat(rs.next(), is(false));
        }
    }

    @Test
    public void preparedStatementWithoutMatchesReturnsEmptyResultSet() throws Exception {
        try (Connection conn = connect()) {
            PreparedStatement preparedStatement = conn.prepareStatement("select * from test where id = ?");
            preparedStatement.setInt(1, 2);
            ResultSet resultSet = preparedStatement.executeQuery();

            assertThat(resultSet, notNullValue());
            assertThat(resultSet.isBeforeFirst(), is(false));
        }
    }

    @Test
    public void preparedStatementReadsAllScalarTypes() throws Exception {
        insertIntoTestTable();
        try (Connection conn = connect()) {
            PreparedStatement preparedStatement = conn.prepareStatement("select * from test where id = ?");
            preparedStatement.setInt(1, 1);
            ResultSet resultSet = preparedStatement.executeQuery();

            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt("id"), is(1));
            assertThat(resultSet.getString("string_field"), is("Youri"));
            assertThat(resultSet.getBoolean("boolean_field"), is(true));
            assertThat(resultSet.getByte("byte_field"), is((byte) 120));
            assertThat(resultSet.getShort("short_field"), is((short) 1000));
            assertThat(resultSet.getInt("integer_field"), is(1200000));
            assertThat(resultSet.getLong("long_field"), is(120000000000L));
            assertThat(resultSet.getFloat("float_field"), is(1.4f));
            assertThat(resultSet.getDouble("double_field"), is(3.456789d));
            assertThat(resultSet.getTimestamp("timestamp_field"), is(new Timestamp(1000L)));
            assertThat(resultSet.getString("ip_field"), is("127.0.0.1"));
            assertThat(resultSet.next(), is(false));
        }
    }

    @Test
    public void syntaxErrorRaisesSQLException() throws Exception {
        try (Connection conn = connect()) {
            SQLException e = assertThrows(SQLException.class,
                () -> conn.createStatement().execute("ERROR"));
            // The parser message wording varies across CrateDB versions.
            assertThat(e.getMessage(), anyOf(
                containsString("line 1:1: no viable alternative at input 'ERROR'"),
                containsString("line 1:1: mismatched input 'ERROR' expecting {'SELECT', '"),
                containsString("line 1:1: extraneous input 'ERROR' expecting {")
            ));
        }
    }

    @Test
    public void statementBatchExecutes() throws Exception {
        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            stmt.addBatch("insert into test (id) values (3)");
            stmt.addBatch("insert into test (id) values (4)");

            int[] results = stmt.executeBatch();
            assertArrayEquals(new int[]{1, 1}, results);
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(2L));
        }
    }

    @Test
    @Disabled("validate batch behaviour")
    public void statementBatchMixedWithQueryFails() throws Exception {
        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            stmt.addBatch("insert into test (id) values (3)");
            stmt.addBatch("insert into test (id) values (5)");
            stmt.addBatch("select * from sys.cluster");

            BatchUpdateException e = assertThrows(BatchUpdateException.class, stmt::executeBatch);
            assertArrayEquals(new int[]{1, 1, Statement.EXECUTE_FAILED}, e.getUpdateCounts());
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(3L));
        }
    }

    @Test
    public void preparedStatementBatchExecutes() throws Exception {
        try (Connection conn = connect()) {
            PreparedStatement stmt = conn.prepareStatement("insert into test (id) values (?)");
            stmt.setInt(1, 2);
            stmt.addBatch();

            stmt.setInt(1, 4);
            stmt.addBatch();

            int[] results = stmt.executeBatch();
            assertArrayEquals(new int[]{1, 1}, results);
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(2L));
        }
    }

    @Test
    public void batchWithIncompatibleParameterTypeFails() throws Exception {
        try (Connection conn = connect()) {
            PreparedStatement stmt = conn.prepareStatement("insert into test (id) values (?)");
            stmt.setObject(1, new HashMap<>());
            stmt.addBatch();

            BatchUpdateException e = assertThrows(BatchUpdateException.class, stmt::executeBatch);
            assertArrayEquals(new int[]{Statement.EXECUTE_FAILED}, e.getUpdateCounts());

            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(0L));
        }
    }

    @Test
    @Disabled
    public void preparedStatementBatchReportsPerRowFailures() throws Exception {
        try (Connection conn = connect()) {
            PreparedStatement stmt = conn.prepareStatement("insert into test (id, string_field) values (?, ?)");
            stmt.setInt(1, 2);
            stmt.setString(2, "foo");
            stmt.addBatch();

            stmt.setInt(1, 1);
            stmt.setObject(2, "baz");
            stmt.addBatch();

            int[] results = stmt.executeBatch();
            assertArrayEquals(new int[]{1, Statement.EXECUTE_FAILED}, results);
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(2L));
        }
    }

    @Test
    public void batchWithSyntaxErrorFails() throws Exception {
        try (Connection conn = connect()) {
            PreparedStatement stmt = conn.prepareStatement("insert test (id) values (?)");
            stmt.setInt(1, 2);
            stmt.addBatch();

            BatchUpdateException e = assertThrows(BatchUpdateException.class, stmt::executeBatch);
            assertArrayEquals(new int[]{Statement.EXECUTE_FAILED}, e.getUpdateCounts());
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(0L));
        }
    }

    @Test
    public void selectWithoutMatchesStillProducesResultSet() throws Exception {
        try (Connection conn = connect()) {
            assertTrue(conn.createStatement().execute("select * from test where id = 1000000"));
        }
    }

    @Test
    public void executeUpdateWithoutMatchesReturnsZero() throws Exception {
        try (Connection conn = connect()) {
            assertThat(conn.createStatement().executeUpdate("update test set string_field = 'new_value' " +
                                                            "where string_field = 'nothing_matches_this'"), is(0));
        }
    }

    @Test
    public void multipleHostsInUrlConnect() throws Exception {
        String url = connectionUrl();
        int schemeEnd = url.indexOf("://") + 3;
        int pathStart = url.indexOf('/', schemeEnd);
        String hostPort = url.substring(schemeEnd, pathStart);
        String multiHostUrl = url.substring(0, schemeEnd) + hostPort + "," + hostPort + url.substring(pathStart);
        try (Connection conn = DriverManager.getConnection(multiHostUrl)) {
            assertThat(conn.createStatement().execute("select 1 from sys.cluster"), is(true));
        }
    }

    /**
     * getMoreResults() always returns false: CrateDB never produces multiple
     * result sets, which in PostgreSQL may occur when executing multiple
     * statements (separated by ;) or when calling stored procedures.
     */
    @Test
    public void getMoreResultsReturnsFalse() throws Exception {
        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            assertTrue(stmt.execute("select name from sys.nodes"));
            assertFalse(stmt.getMoreResults());
        }
    }
}
