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
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.jdbc.CrateVersion;
import org.postgresql.jdbc.PgDatabaseMetaData;
import org.postgresql.util.PSQLException;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConnectionITest extends BaseIntegrationTest {

    @Test
    public void testConnectionWithCustomSchema() throws SQLException, InterruptedException {
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
    public void testConnectionWithCustomSchemaPrepareStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
    public void testSelectWithoutResultUsingPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
            PreparedStatement stmt = conn.prepareStatement("insert into test (id) values (?)");
            stmt.setObject(1, newHashMap());
            stmt.addBatch();
            PgDatabaseMetaData metaData = (PgDatabaseMetaData) conn.getMetaData();

            try {
                stmt.executeBatch();
                fail("BatchUpdateException not thrown");
            } catch (BatchUpdateException e) {
                String msg = e.getMessage();

                CrateVersion version = metaData.getCrateVersion(); // Don't query version several times.
                if (version.compareTo("4.7.0") >= 0) {
                    assertThat(msg, containsString("Cannot cast value `{}` to type `integer`"));
                } else if (version.compareTo("4.2.0") >= 0) {
                    assertThat(msg, Matchers.allOf(
                            containsString("The type 'object' of the insert source "),
                            containsString("is not convertible to the type 'integer' of target column 'id'"))
                    );
                } else if (metaData.getCrateVersion().before("2.3.4")) {
                    assertThat(msg, containsString("Validation failed for id: {} cannot be cast to type integer"));
                } else {
                    assertThat(msg, containsString("Validation failed for id: Cannot cast {} to type integer"));
                }
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
            assertTrue(conn.createStatement().execute("select * from test where id = 1000000"));
        }
    }

    @Test
    public void testExecuteUpdateWhenNothingMatches() throws Exception {
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
            assertThat(conn.createStatement().executeUpdate("update test set string_field = 'new_value' " +
                                                            "where string_field = 'nothing_matches_this'"), is(0));
        }
    }

    @Test
    public void testMultipleHostsConnectionString() throws Exception {
        CrateTestServer server = TEST_CLUSTER.randomServer();
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
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
            Statement stmt = conn.createStatement();
            assertTrue(stmt.execute("select name from sys.nodes"));
            assertFalse(stmt.getMoreResults());
        }
    }
}