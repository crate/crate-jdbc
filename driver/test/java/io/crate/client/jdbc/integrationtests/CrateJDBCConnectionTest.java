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

import io.crate.testing.CrateTestServer;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.Arrays;
import java.util.Date;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

public class CrateJDBCConnectionTest extends CrateJDBCIntegrationTest {

    private static String connectionString;

    @BeforeClass
    public static void beforeClass() throws SQLException, InterruptedException {
        connectionString = getConnectionString();
        setUpTestTable();
    }

    @After
    public void after() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            conn.createStatement().execute("delete from test");
            conn.createStatement().execute("refresh table test");
        }
    }

    @Test
    @Ignore("set/get schema is not implemented")
    public void testConnectionWithCustomSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            conn.setSchema("foo");
            assertThat(conn.getSchema(), is("foo"));
            Statement statement = conn.createStatement();
            statement.execute("create table t (x string) with (number_of_replicas=0)");
            ensureYellow();
            statement.execute("insert into t (x) values ('a')");
            statement.execute("refresh table t");
            ResultSet resultSet = statement.executeQuery("select count(*) from t");
            resultSet.next();
            assertThat(resultSet.getLong(1), is(1L));
            conn.close();

            Connection barConnection = DriverManager.getConnection(connectionString);
            conn.setSchema("bar");
            assertThat(barConnection.getSchema(), is("bar"));
            statement = barConnection.createStatement();
            statement.execute("create table t (x string) with (number_of_replicas=0)");
            ensureYellow();
            statement.execute("insert into t (x) values ('a')");
            statement.execute("refresh table t");
            resultSet = conn.createStatement().executeQuery("select count(*) from t)");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(1L));

            resultSet = statement.executeQuery(
                "select collect_set(schema_name) from information_schema.tables where table_name = 't'");
            resultSet.next();

            Object[] objects = (Object[]) resultSet.getObject(1);
            String[] schemas = Arrays.copyOf(objects, objects.length, String[].class);
            assertThat(schemas, Matchers.arrayContainingInAnyOrder("foo", "bar"));
            barConnection.close();
            conn.createStatement().execute("drop table foo.t");
            conn.createStatement().execute("drop table bar.t");
        }
    }

    @Test
    @Ignore("set/get schema is not implemented")
    public void testConnectionWithCustomSchemaPrepareStatement() throws Exception {
        String schemaName = "my";
        String tableName = "test_a";
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            conn.setSchema(schemaName);
            PreparedStatement pstmt = conn.prepareStatement(
                String.format("create table %s (first_column integer, second_column string) with (number_of_replicas=0)", tableName));
            assertThat(pstmt.execute(), is(false));
            ensureYellow();
            pstmt = conn.prepareStatement(
                String.format("insert into %s (first_column, second_column) values (?, ?)", tableName));
            pstmt.setInt(1, 42);
            pstmt.setString(2, "testing");
            assertThat(pstmt.execute(), is(false));
            pstmt = conn.prepareStatement(
                String.format("refresh table %s", tableName));
            pstmt.execute();
            pstmt = conn.prepareStatement(
                String.format("select * from %s", tableName));
            assertThat(pstmt.execute(), is(true)); // there should be a return value
            ResultSet rSet2 = pstmt.getResultSet();
            assertThat(rSet2.next(), is(true)); // there should be a result
            assertThat(rSet2.getInt(1), is(42));
            assertThat(rSet2.getString(2), is("testing"));
            pstmt = conn.prepareStatement("select schema_name, table_name from information_schema.tables " +
                                          "where schema_name = ? and table_name = ?");
            pstmt.setString(1, schemaName);
            pstmt.setString(2, tableName);
            assertThat(pstmt.execute(), is(true)); // there should be a return value
            ResultSet rSet = pstmt.getResultSet();
            assertThat(rSet.next(), is(true)); // there should be a result
            assertThat(rSet.getString(1), is(schemaName));
            assertThat(rSet.getString(2), is(tableName));
            conn.prepareStatement(String.format("drop table %s", tableName)).execute();
            conn.close();
        }
    }

    @Test
    @Ignore("set/get schema is not implemented")
    public void testConnectionWithCustomSchemaBatchPrepareStatement() throws Exception {
        String schemaName = "my";
        String tableName = "test_b";
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            conn.setSchema(schemaName);
            PreparedStatement stmt = conn.prepareStatement(
                String.format("create table %s (id long, ts timestamp, info string) with (number_of_replicas=0)", tableName));
            stmt.execute();
            ensureYellow();
            stmt = conn.prepareStatement(
                String.format("INSERT INTO %s (id, ts, info) values (?, ?, ?)", tableName));
            String text = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor.";
            for (long i = 1; i < 10 + 1; i++) {
                stmt.setLong(1, i);
                stmt.setTimestamp(2, new Timestamp(new Date().getTime()));
                stmt.setString(3, text);
                stmt.addBatch();
                if (i % 5 == 0) {
                    assertThat(stmt.executeBatch(), is(new int[]{1, 1, 1, 1, 1}));
                }
            }
            conn.prepareStatement(String.format("drop table %s", tableName)).execute();
            conn.close();
        }
    }

    @Test
    public void testSelectWithoutResultUsingPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            PreparedStatement preparedStatement = conn.prepareStatement("select * from test where id = ?");
            preparedStatement.setInt(1, 2);
            ResultSet resultSet = preparedStatement.executeQuery();

            assertThat(resultSet, notNullValue());
            assertThat(resultSet.isBeforeFirst(), is(false));
        }
    }

    @Test
    public void testSelectUsingPreparedStatement() throws Exception {
        insertIntoTestTable();
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            PreparedStatement preparedStatement = conn.prepareStatement("select * from test where id = ?");
            preparedStatement.setInt(1, 1);
            ResultSet resultSet = preparedStatement.executeQuery();

            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt("id"), is(1));
            assertThat(resultSet.getString("string_field"), is("Youri"));
            assertThat(resultSet.getBoolean("boolean_field"), is(true));
            assertThat(resultSet.getByte("byte_field"), is(new Byte("120")));
            assertThat(resultSet.getShort("short_field"), is(new Short("1000")));
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
    public void testException() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            expectedException.expect(anyOf(instanceOf(SQLException.class), instanceOf(PSQLException.class)));
            expectedException.expectMessage(anyOf(
                    containsString("line 1:1: no viable alternative at input 'ERROR'"),
                    containsString("line 1:1: mismatched input 'ERROR' expecting {'SELECT', '"))
            );
            conn.createStatement().execute("ERROR");
        }
    }

    @Test
    public void testExecuteBatchStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            Statement stmt = conn.createStatement();
            stmt.addBatch("insert into test (id) values (3)");
            stmt.addBatch("insert into test (id) values (4)");

            int[] results = stmt.executeBatch();
            assertArrayEquals(results, new int[]{1, 1});
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(2L));
        }
    }

    @Test
    @Ignore("validate batch behaviour")
    public void testExecuteBatchStatementFail() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            Statement stmt = conn.createStatement();
            stmt.addBatch("insert into test (id) values (3)");
            stmt.addBatch("insert into test (id) values (5)");
            stmt.addBatch("select * from sys.cluster");

            try {
                stmt.executeBatch();
                fail("BatchUpdateException not thrown");
            } catch (BatchUpdateException e) {
                assertArrayEquals(e.getUpdateCounts(), new int[]{1, 1, Statement.EXECUTE_FAILED});
            }
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(3L));
        }
    }

    @Test
    public void testExecuteBatchPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
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
    public void testExecuteBatchPreparedStatementFailBulkTypes() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            PreparedStatement stmt = conn.prepareStatement("insert into test (id) values (?)");
            stmt.setObject(1, newHashMap());
            stmt.addBatch();

            try {
                stmt.executeBatch();
                fail("BatchUpdateException not thrown");
            } catch (BatchUpdateException e) {
                assertThat(
                    e.getMessage(),
                    containsString("Validation failed for id: Cannot cast {} to type integer")
                );
                assertArrayEquals(new int[]{Statement.EXECUTE_FAILED}, e.getUpdateCounts());
            }
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(0L));
        }
    }

    @Test
    @Ignore
    public void testExecuteBatchPreparedStatementFailOne() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
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
    public void testExecuteBatchPreparedStatementFailSyntax() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            PreparedStatement stmt = conn.prepareStatement("insert test (id) values (?)");
            stmt.setInt(1, 2);
            stmt.addBatch();

            try {
                stmt.executeBatch();
                fail("BatchUpdateException not thrown");
            } catch (BatchUpdateException e) {
                assertArrayEquals(new int[]{Statement.EXECUTE_FAILED}, e.getUpdateCounts());
            }
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(0L));
        }
    }

    @Test
    public void testSelectWhenNothingMatches() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            assertTrue(conn.createStatement().execute("select * from test where id = 1000000"));
        }
    }

    @Test
    public void testExecuteUpdateWhenNothingMatches() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            assertThat(conn.createStatement().executeUpdate("update test set string_field = 'new_value' " +
                                                            "where string_field = 'nothing_matches_this'"), is(0));
        }
    }

    @Test
    public void testMultipleHostsConnectionString() throws Exception {
        CrateTestServer server = testCluster.randomServer();
        String connectionStr = String.format(
            "crate://%s:%s,%s:%s/doc?user=crate", server.crateHost(), server.psqlPort(), server.crateHost(), server.psqlPort()
        );
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            assertThat(conn.createStatement().execute("select 1 from sys.cluster"), is(true));
        }
    }

    @Test
    public void testGetMoreResults() throws Exception {
        /**
         * getMoreResults() always returns false, because CrateDB does not support multiple result sets.
         * In Postgres multiple result sets may occur when executing multiple statements (separated by ;) or when
         * calling stored procedures.
         */
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            Statement stmt = conn.createStatement();
            assertTrue(stmt.execute("select name from sys.nodes"));
            assertFalse(stmt.getMoreResults());
        }
    }
}