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

import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.client.CrateClient;
import io.crate.client.jdbc.CrateResultSet;
import io.crate.testing.CrateTestCluster;
import io.crate.testing.CrateTestServer;
import org.hamcrest.Matchers;
import org.junit.*;

import java.sql.*;
import java.util.*;
import java.util.Date;

import static io.crate.shade.com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.Matchers.*;

public class CrateJDBCConnectionTest extends CrateJDBCIntegrationTest {

    @ClassRule
    public static CrateTestCluster testCluster = CrateTestCluster
            .fromVersion(CRATE_SERVER_VERSION)
            .keepWorkingDir(false)
            .build();

    private static Connection connection;
    private static String hostAndPort;
    private static String connectionString;
    private static CrateClient client;

    @BeforeClass
    public static void beforeClass() throws Exception {
        CrateTestServer server = testCluster.randomServer();
        hostAndPort = String.format(Locale.ENGLISH, "%s:%d",
                server.crateHost(),
                server.transportPort()
        );
        connectionString = "crate://" + hostAndPort;
        connection = DriverManager.getConnection(connectionString);
        client = new CrateClient(hostAndPort);
        setUpTables();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        tearDownTables();
        connection.close();
        connection = null;
        client.close();
        client = null;
    }

    @Before
    public void setUp() throws Exception {
        insertIntoTable();
    }

    public static void setUpTables() throws InterruptedException {
        String stmt = "create table if not exists test (" +
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
                ") clustered by (id) into 1 shards with (number_of_replicas=0)";
        client.sql(stmt).actionGet();
        waitForShards();
    }

    private static void insertIntoTable() {
        Map<String, Object> objectField = new HashMap<String, Object>() {{
            put("inner", "Zoon");
        }};
        SQLRequest sqlRequest = new SQLRequest("insert into test (id, string_field, boolean_field, byte_field, short_field, integer_field," +
                "long_field, float_field, double_field, object_field," +
                "timestamp_field, ip_field, array1, obj_array) values " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", new Object[]{
                1, "Youri", true, 120, 1000, 1200000,
                120000000000L, 1.4, 3.456789, objectField,
                "1970-01-01", "127.0.0.1",
                new Object[]{"a", "b", "c", "d"},
                new Object[]{new HashMap<String, Object>() {{
                    put("bla", "blubb");
                }}}
        });
        client.sql(sqlRequest).actionGet();
        client.sql("refresh table test").actionGet();
    }

    private static void waitForShards() throws InterruptedException {
        while (countUnassigned() > 0) {
            Thread.sleep(100);
        }
    }

    private static Long countUnassigned() {
        SQLResponse res = client.sql("SELECT count(*) FROM sys.shards WHERE state != 'STARTED'").actionGet();
        return (Long) res.rows()[0][0];
    }

    @After
    public void tearDown() {
        deleteFromTable();
    }

    public static void deleteFromTable() {
        client.sql("delete from test").actionGet();
        client.sql("refresh table test").actionGet();
    }

    private static void tearDownTables() {
        SQLResponse response = client.sql("select schema_name, table_name from information_schema.tables where schema_name not in ('sys', 'information_schema', 'blob')").actionGet();
        for (Object[] row : response.rows()) {
            try {
                client.sql(String.format("drop table if exists \"%s\".\"%s\"", row[0], row[1]));
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testConnectionWithCustomSchema() throws Exception {
        Connection fooConn = DriverManager.getConnection(String.format("crate://%s/foo", hostAndPort));
        assertThat(fooConn.getSchema(), is("foo"));
        Statement statement = fooConn.createStatement();
        statement.execute("create table t (x string) with (number_of_replicas=0)");
        waitForShards();
        statement.execute("insert into t (x) values ('a')");
        statement.execute("refresh table t");
        ResultSet resultSet = statement.executeQuery("select count(*) from t");
        resultSet.next();
        assertThat(resultSet.getLong(1), is(1L));
        fooConn.close();

        Connection barConnection = DriverManager.getConnection(String.format("crate://%s/bar", hostAndPort));
        assertThat(barConnection.getSchema(), is("bar"));
        statement = barConnection.createStatement();
        statement.execute("create table t (x string) with (number_of_replicas=0)");
        waitForShards();
        statement.execute("insert into t (x) values ('a')");
        statement.execute("refresh table t");
        resultSet = statement.executeQuery("select count(*) from t");
        resultSet.next();
        assertThat(resultSet.getLong(1), is(1L));

        resultSet = statement.executeQuery(
                "select collect_set(schema_name) from information_schema.tables where table_name = 't'");
        resultSet.next();

        Object[] objects = (Object[]) resultSet.getObject(1);
        String[] schemas = Arrays.copyOf(objects, objects.length, String[].class);
        assertThat(schemas, Matchers.arrayContainingInAnyOrder("foo", "bar"));
        barConnection.close();
        connection.prepareStatement("drop table foo.t").execute();
        connection.prepareStatement("drop table bar.t").execute();
    }

    @Test
    public void testConnectionWithCustomSchemaPrepareStatement() throws Exception {
        String schemaName = "my";
        String tableName = "test_a";
        Connection conn = DriverManager.getConnection(String.format("jdbc:crate://%s/%s", hostAndPort, schemaName));

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

    @Test
    public void testConnectionWithCustomSchemaBatchPrepareStatement() throws Exception {
        String schemaName = "my";
        String tableName = "test_b";
        Connection conn = DriverManager.getConnection(String.format("jdbc:crate://%s/%s", hostAndPort, schemaName));

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

    @Test
    @SuppressWarnings("unchecked")
    public void testSelectAllTypes() throws Exception {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test");

        assertThat(resultSet, instanceOf(CrateResultSet.class));
        assertThat(((CrateResultSet) resultSet).getCount(), is(1L));
        resultSet.next();
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
        assertThat((Map<String, Object>) resultSet.getObject("object_field"), is(objectField));

        Array array1 = resultSet.getArray("array1");
        assertThat(array1.getArray().getClass().isArray(), is(true));
        Assert.assertThat(array1.getBaseType(), is(Types.VARCHAR));
        assertThat((Object[]) array1.getArray(), Matchers.<Object>arrayContaining("a", "b", "c", "d"));

        Array objArray = resultSet.getArray("obj_array");
        assertThat(objArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(objArray.getBaseType(), is(Types.JAVA_OBJECT));
        Object firstObject = ((Object[]) objArray.getArray())[0];
        Assert.assertThat(firstObject, instanceOf(Map.class));
    }

    @Test
    public void testSelectWithoutResultUsingPreparedStatement() throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("select * from test where id = ?");
        preparedStatement.setInt(1, 2);

        ResultSet resultSet = preparedStatement.executeQuery();

        assertThat(resultSet, notNullValue());
        assertThat(resultSet.isBeforeFirst(), is(false));
    }

    @Test
    public void testSelectUsingPreparedStatement() throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("select * from test where id = ?");
        preparedStatement.setInt(1, 1);

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();

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
    }

    @Test
    public void testExcludeNestedColumns() throws Exception {
        ResultSet resultSet = connection.getMetaData().getColumns(null, "sys", "nodes", null);
        int counter = 0;
        while (resultSet.next()) {
            assertFalse(resultSet.getString(4).contains("."));
            assertFalse(resultSet.getString(4).contains("["));
            counter++;
        }
        assertThat(counter, is(15));

    }

    @Test
    public void testIncludeNestedColumns() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("showsubcolumns", "true");
        Connection connection = DriverManager.getConnection(connectionString, properties);
        ResultSet resultSet = connection.getMetaData().getColumns(null, "sys", "nodes", null);
        int counter = 0;
        while (resultSet.next()) {
            counter++;
        }
        assertThat(counter, is(99));
        connection.close();
    }

    /**
     * test that SQLActionException is correctly wrapped in a SQLException
     */
    @Test
    public void testException() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("line 1:1: no viable alternative at input 'ERROR'");
        expectedException.expectCause(Matchers.is(Matchers.<Throwable>instanceOf(SQLActionException.class)));

        Statement stmt = connection.createStatement();
        stmt.executeQuery("ERROR");
    }

    @Test
    public void testExecuteBatchStatement() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.addBatch("insert into test (id) values (3)");
        stmt.addBatch("insert into test (id) values (4)");
        stmt.addBatch("insert into test (id) values (5)");

        int[] results = stmt.executeBatch();
        assertArrayEquals(results, new int[]{1, 1, 1});

        assertFalse(stmt.execute("refresh table test"));
        ResultSet resultSet = stmt.executeQuery("select count(*) from test");
        resultSet.first();
        assertThat(resultSet.getLong(1), is(4L));
    }

    @Test
    public void testExecuteBatchStatementFail() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.addBatch("insert into test (id) values (3)");
        stmt.addBatch("insert (id) values (4) into test");
        stmt.addBatch("insert into test (id) values (5)");
        stmt.addBatch("select * from sys.cluster");

        try {
            stmt.executeBatch();
            fail("BatchUpdateException not thrown");
        } catch (BatchUpdateException e) {
            assertArrayEquals(e.getUpdateCounts(), new int[]{1, Statement.EXECUTE_FAILED, 1, Statement.EXECUTE_FAILED});
        }
        assertFalse(stmt.execute("refresh table test"));
        ResultSet resultSet = stmt.executeQuery("select count(*) from test");
        resultSet.first();
        assertThat(resultSet.getLong(1), is(3L));
    }

    @Test
    public void testExecuteBatchPreparedStatement() throws Exception {
        PreparedStatement stmt = connection.prepareStatement("insert into test (id, string_field) values (?, ?)");
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

        assertFalse(connection.createStatement().execute("refresh table test"));
        ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from test");
        resultSet.first();
        assertThat(resultSet.getLong(1), is(4L));
    }

    @Test
    public void testExecuteBatchPreparedStatementFailBulkTypes() throws Exception {
        PreparedStatement stmt = connection.prepareStatement("insert into test (id, string_field) values ($1, $2)");
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
            assertThat(e.getMessage(), is("ColumnValidationException: Validation failed for string_field: {} cannot be cast to type string"));
            assertArrayEquals(new int[]{Statement.EXECUTE_FAILED}, e.getUpdateCounts());
        }

        assertFalse(connection.createStatement().execute("refresh table test"));
        ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from test");
        resultSet.first();
        assertThat(resultSet.getLong(1), is(1L));
    }

    @Test
    public void testExecuteBatchPreparedStatementFailOne() throws Exception {
        PreparedStatement stmt = connection.prepareStatement("insert into test (id, string_field) values ($1, $2)");
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

        assertFalse(connection.createStatement().execute("refresh table test"));
        ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from test");
        resultSet.first();
        assertThat(resultSet.getLong(1), is(3L));
    }

    @Test
    public void testExecuteBatchPreparedStatementFailSyntax() throws Exception {
        PreparedStatement stmt = connection.prepareStatement("insert test (id, string_field) values (?, ?)");
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
            assertArrayEquals(new int[]{Statement.EXECUTE_FAILED}, e.getUpdateCounts());
        }
        assertFalse(connection.createStatement().execute("refresh table test"));
        ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from test");
        resultSet.first();
        assertThat(resultSet.getLong(1), is(1L));
    }

    @Test
    public void testTypesResponseNoResult() throws Exception {
        ResultSet result = connection.createStatement().executeQuery("select * from test where 1=0");
        ResultSetMetaData metaData = result.getMetaData();
        assertThat(metaData.getColumnCount(), is(14));
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            // test that we can get the types, whatever they are
            assertThat(metaData.getColumnType(i), instanceOf(Integer.class));
        }
    }

    @Test
    public void testSelectWhenNothingMatches() throws Exception {
        Statement statement = connection.createStatement();
        assertTrue(statement.execute("select * from test where string_field = 'nothing_matches_this'"));
    }

    @Test
    public void testExecuteUpdateWhenNothingMatches() throws Exception {
        Statement statement = connection.createStatement();
        assertThat(statement.executeUpdate("update test set string_field = 'new_value' where string_field = 'nothing_matches_this'"), is(0));
    }

    @Test
    public void testGetSchemas() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet result = metaData.getSchemas();
        result.next();
        assertThat(result.getString(1), is("blob"));
        result.next();
        assertThat(result.getString(1), is("doc"));
        result.next();
        assertThat(result.getString(1), is("information_schema"));
        result.next();
        assertThat(result.getString(1), is("pg_catalog"));
        result.next();
        assertThat(result.getString(1), is("sys"));
    }

}
