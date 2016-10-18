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
import org.junit.*;
import org.postgresql.jdbc.PgResultSet;

import java.sql.*;
import java.util.*;
import java.util.Date;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class CrateJDBCConnectionTest extends CrateJDBCIntegrationTest {

    private static Connection connection;
    private static String connectionStr;

    @BeforeClass
    public static void beforeClass() throws Exception {
        CrateTestServer server = testCluster.randomServer();
        connectionStr = String.format("crate://%s:%s/", server.crateHost(), server.psqlPort());
        connection = DriverManager.getConnection(connectionStr);
        setUpTables();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        tearDownTables();
    }

    private static void tearDownTables() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select schema_name, table_name " +
            "from information_schema.tables where schema_name " +
            "not in ('pg_catalog', 'sys', 'information_schema', 'blob')");
        while (rs.next()) {
            connection.createStatement().execute(String.format("drop table if exists \"%s\".\"%s\"",
                rs.getString("schema_name"), rs.getString("table_name")));
        }
    }

    public static void setUpTables() throws InterruptedException, SQLException {
        connection.createStatement().execute("create table if not exists test (" +
            " id integer primary key," +
            " string_field string," +
            " boolean_field boolean," +
            " byte_field byte," +
            " short_field short," +
            " integer_field integer," +
            " long_field long," +
            " float_field float," +
            " double_field double," +
            " timestamp_field timestamp," +
            " object_field object as (\"inner\" string)," +
            " ip_field ip," +
            " array1 array(string)," +
            " obj_array array(object)" +
            ") clustered by (id) into 1 shards with (number_of_replicas=0)");
        waitForShards();
    }

    private static void waitForShards() throws InterruptedException, SQLException {
        while (countUnassigned() > 0) {
            Thread.sleep(100);
        }
    }

    private static Long countUnassigned() throws SQLException {
        ResultSet rs = connection.createStatement()
            .executeQuery("SELECT count(*) FROM sys.shards WHERE state != 'STARTED'");
        rs.next();
        return rs.getLong(1);
    }

    @Before
    public void before() throws SQLException {
        insertIntoTable();
    }

    @After
    public void after() throws SQLException {
        deleteFromTable();
    }

    private static void insertIntoTable() throws SQLException {
        Map<String, Object> objectField = new HashMap<String, Object>() {{
            put("inner", "Zoon");
        }};
        PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test (id, string_field, boolean_field, byte_field, " +
                "short_field, integer_field, long_field, float_field, double_field, object_field, " +
                "timestamp_field, ip_field, array1, obj_array) values " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        preparedStatement.setInt(1, 1);
        preparedStatement.setString(2, "Youri");
        preparedStatement.setBoolean(3, true);
        preparedStatement.setByte(4, (byte) 120);
        preparedStatement.setShort(5, (short) 1000);
        preparedStatement.setInt(6, 1200000);
        preparedStatement.setLong(7, 120000000000L);
        preparedStatement.setFloat(8, 1.4f);
        preparedStatement.setDouble(9, 3.456789);
        preparedStatement.setObject(10, objectField);
        preparedStatement.setString(11, "1970-01-01");
        preparedStatement.setString(12, "127.0.0.1");
        preparedStatement.setArray(13, connection.createArrayOf("varchar", new String[]{"a", "b", "c", "d"})); // TODO array support
        preparedStatement.setArray(14, null);
        preparedStatement.execute();

        connection.createStatement().execute("refresh table test");
    }

    public static void deleteFromTable() throws SQLException {
        connection.createStatement().execute("delete from test");
        connection.createStatement().execute("refresh table test");
    }

    @Test
    @Ignore("set/get schema is not implemented")
    public void testConnectionWithCustomSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            conn.setSchema("foo");
            assertThat(conn.getSchema(), is("foo"));
            Statement statement = conn.createStatement();
            statement.execute("create table t (x string) with (number_of_replicas=0)");
            waitForShards();
            statement.execute("insert into t (x) values ('a')");
            statement.execute("refresh table t");
            ResultSet resultSet = statement.executeQuery("select count(*) from t");
            resultSet.next();
            assertThat(resultSet.getLong(1), is(1L));
            conn.close();

            Connection barConnection = DriverManager.getConnection(connectionStr);
            conn.setSchema("bar");
            assertThat(barConnection.getSchema(), is("bar"));
            statement = barConnection.createStatement();
            statement.execute("create table t (x string) with (number_of_replicas=0)");
            waitForShards();
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
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            conn.setSchema(schemaName);
            PreparedStatement pstmt = conn.prepareStatement(
                String.format("create table %s (first_column integer, second_column string) with (number_of_replicas=0)", tableName));
            assertThat(pstmt.execute(), is(false));
            waitForShards();
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
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            conn.setSchema(schemaName);
            PreparedStatement stmt = conn.prepareStatement(
                String.format("create table %s (id long, ts timestamp, info string) with (number_of_replicas=0)", tableName));
            stmt.execute();
            waitForShards();
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
    public void testSelectAllTypes() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select * from test");

            assertThat(resultSet, instanceOf(PgResultSet.class));
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
            assertThat(resultSet.getTimestamp("timestamp_field"), is(new Timestamp(0L)));
            assertThat(resultSet.getString("ip_field"), is("127.0.0.1"));

            Map<String, Object> objectField = new HashMap<String, Object>() {{
                put("inner", "Zoon");
            }};
            //noinspection unchecked
            assertThat((Map<String, Object>) resultSet.getObject("object_field"), is(objectField));
            Array array1 = resultSet.getArray("array1");
            assertThat(array1.getArray().getClass().isArray(), is(true));
            Assert.assertThat(array1.getBaseType(), is(Types.VARCHAR));
            assertThat((Object[]) array1.getArray(), Matchers.<Object>arrayContaining("a", "b", "c", "d"));
            // TODO Support set array
//            Array objArray = resultSet.getArray("obj_array");
//            assertThat(objArray.getArray().getClass().isArray(), is(true));
//            Assert.assertThat(objArray.getBaseType(), is(Types.JAVA_OBJECT));
//            Object firstObject = ((Object[]) objArray.getArray())[0];
//            Assert.assertThat(firstObject, instanceOf(Map.class));
//            assertThat(resultSet.next(), is(false));
        }
    }

    @Test
    public void testSelectWithoutResultUsingPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            PreparedStatement preparedStatement = conn.prepareStatement("select * from test where id = ?");
            preparedStatement.setInt(1, 2);
            ResultSet resultSet = preparedStatement.executeQuery();

            assertThat(resultSet, notNullValue());
            assertThat(resultSet.isBeforeFirst(), is(false));
        }
    }

    @Test
    public void testSelectUsingPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
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
            assertThat(resultSet.getTimestamp("timestamp_field"), is(new Timestamp(0L)));
            assertThat(resultSet.getString("ip_field"), is("127.0.0.1"));
            assertThat(resultSet.next(), is(false));
        }
    }

    @Test
    public void testExcludeNestedColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.getMetaData().getColumns(null, "sys", "nodes", null);
            int counter = 0;
            while (resultSet.next()) {
                assertFalse(resultSet.getString(4).contains("."));
                assertFalse(resultSet.getString(4).contains("["));
                counter++;
            }
            assertThat(counter, is(15));
        }
    }

    @Test
    public void testException() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            expectedException.expect(SQLException.class);
            expectedException.expectMessage("line 1:1: no viable alternative at input 'ERROR'");
            conn.createStatement().execute("ERROR");
        }
    }

    @Test
    public void testExecuteBatchStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            Statement stmt = conn.createStatement();
            stmt.addBatch("insert into test (id) values (3)");
            stmt.addBatch("insert into test (id) values (4)");
            stmt.addBatch("insert into test (id) values (5)");

            int[] results = stmt.executeBatch();
            assertArrayEquals(results, new int[]{1, 1, 1});
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(4L));
        }
    }

    @Test
    @Ignore("validate batch behaviour")
    public void testExecuteBatchStatementFail() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
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
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            PreparedStatement stmt = conn.prepareStatement("insert into test (id, string_field) values (?, ?)");
            stmt.setInt(1, 2);
            stmt.setString(2, "foo");
            stmt.addBatch();

            stmt.setInt(1, 3);
            stmt.setString(2, "bar");
            stmt.addBatch();

            stmt.setInt(1, 4);
            stmt.setString(2, "baz");
            stmt.addBatch();

            int[] results = stmt.executeBatch();
            assertArrayEquals(new int[]{1, 1, 1}, results);
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(4L));
        }
    }

    @Test
    public void testExecuteBatchPreparedStatementFailBulkTypes() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            PreparedStatement stmt = conn.prepareStatement("insert into test (id, string_field) values (?, ?)");
            stmt.setInt(1, 2);
            stmt.setString(2, "foo");
            stmt.addBatch();

            stmt.setInt(1, 3);
            stmt.setString(2, "bar");
            stmt.addBatch();

            stmt.setInt(1, 1);
            stmt.setObject(2, newHashMap());
            stmt.addBatch();

            try {
                stmt.executeBatch();
                fail("BatchUpdateException not thrown");
            } catch (BatchUpdateException e) {
                assertThat(
                    e.getMessage(),
                    containsString("Validation failed for string_field: {} cannot be cast to type string")
                );
                assertArrayEquals(
                    new int[]{Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
                    e.getUpdateCounts()
                );
            }
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));

            assertThat(resultSet.getLong(1), is(1L));
        }
    }

    @Test
    public void testExecuteBatchPreparedStatementFailOne() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            PreparedStatement stmt = conn.prepareStatement("insert into test (id, string_field) values (?, ?)");
            stmt.setInt(1, 2);
            stmt.setString(2, "foo");
            stmt.addBatch();

            stmt.setInt(1, 3);
            stmt.setString(2, "bar");
            stmt.addBatch();

            stmt.setInt(1, 1);
            stmt.setObject(2, "baz");
            stmt.addBatch();


            int[] results = stmt.executeBatch();
            assertArrayEquals(new int[]{1, 1, Statement.EXECUTE_FAILED}, results);
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(3L));
        }
    }

    @Test
    public void testExecuteBatchPreparedStatementFailSyntax() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            PreparedStatement stmt = conn.prepareStatement("insert test (id, string_field) values (?, ?)");
            stmt.setInt(1, 2);
            stmt.setString(2, "foo");
            stmt.addBatch();

            stmt.setInt(1, 3);
            stmt.setString(2, "bar");
            stmt.addBatch();

            stmt.setInt(1, 4);
            stmt.setString(2, "baz");
            stmt.addBatch();

            try {
                stmt.executeBatch();
                fail("BatchUpdateException not thrown");
            } catch (BatchUpdateException e) {
                assertArrayEquals(
                    new int[]{Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
                    e.getUpdateCounts()
                );
            }
            conn.createStatement().execute("refresh table test");
            ResultSet resultSet = conn.createStatement().executeQuery("select count(*) from test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(1L));
        }
    }

    @Test
    public void testTypesResponseNoResult() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet result = conn.createStatement().executeQuery("select * from test where 1=0");
            ResultSetMetaData metaData = result.getMetaData();
            assertThat(metaData.getColumnCount(), is(14));
            for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                // test that we can get the types, whatever they are
                assertThat(metaData.getColumnType(i), instanceOf(Integer.class));
            }
        }
    }

    @Test
    public void testSelectWhenNothingMatches() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            assertTrue(conn.createStatement().execute("select * from test where string_field = 'nothing_matches_this'"));
        }
    }

    @Test
    public void testExecuteUpdateWhenNothingMatches() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            assertThat(conn.createStatement().executeUpdate("update test set string_field = 'new_value'" +
                " where string_field = 'nothing_matches_this'"), is(0));
        }
    }


    @Test
    public void testMultipleHostsConnectionString() throws Exception {
        CrateTestServer server = testCluster.randomServer();
        String connectionStr = String.format(
                "crate://%s:%s,%s:%s/", server.crateHost(), server.psqlPort(), server.crateHost(), server.psqlPort()
        );
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            assertThat(conn.createStatement().execute("select 1 from sys.cluster"), is(true));
        }
    }

    @Test
    @Ignore("getSchemas is not implemented")
    public void testGetSchemas() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet result = metaData.getSchemas();
            result.next();
            assertThat(result.getString(1), is("blob"));
            result.next();
            assertThat(result.getString(1), is("doc"));
            result.next();
            assertThat(result.getString(1), is("information_schema"));
            result.next();
            assertThat(result.getString(1), is("sys"));
        }
    }

    @Test
    public void testSetGetObject() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            Map<String, Integer> expected = new HashMap<>();
            expected.put("n", 1);

            conn.createStatement().executeUpdate("create table test_obj (obj object as (n int))");
            PreparedStatement statement = conn.prepareStatement("insert into test_obj (obj) values (?)");
            statement.setObject(1, expected);
            statement.execute();

            conn.createStatement().execute("refresh table test_obj");
            ResultSet resultSet = conn.createStatement().executeQuery("select obj from test_obj");
            assertThat(resultSet.next(), is(true));
            conn.createStatement().execute("drop table test_obj");
            
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) resultSet.getObject(1);
            assertEquals(expected, map);
        }
    }
}
