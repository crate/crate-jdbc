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
import io.crate.client.CrateClient;
import io.crate.client.CrateTestServer;
import io.crate.client.jdbc.CrateResultSet;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.sql.*;
import java.util.*;

import static io.crate.shade.com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * to be run by IntegrationTestSuite only
 */
public class CrateJDBCIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static CrateTestServer testServer = new CrateTestServer("jdbc");

    private static Connection connection;
    private static String hostAndPort;
    private static CrateClient client;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Class.forName("io.crate.client.jdbc.CrateDriver");
        hostAndPort = String.format(Locale.ENGLISH, "%s:%d",
                testServer.crateHost,
                testServer.transportPort
        );
        connection = DriverManager.getConnection("crate://" + hostAndPort);
        client = new CrateClient(hostAndPort);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        connection.close();
        connection = null;
    }

    @Test
    public void testConnectionWithCustomSchema() throws Exception {
        Connection fooConn = DriverManager.getConnection(String.format("crate://%s/foo", hostAndPort));
        assertThat(fooConn.getSchema(), is("foo"));
        Statement statement = fooConn.createStatement();
        statement.execute("create table t (x string) with (number_of_replicas = 0)");
        statement.execute("insert into t (x) values ('a')");
        statement.execute("refresh table t");
        ResultSet resultSet = statement.executeQuery("select count(*) from t");
        resultSet.next();
        assertThat(resultSet.getLong(1), is(1L));

        Connection barConnection = DriverManager.getConnection(String.format("crate://%s/bar", hostAndPort));
        assertThat(barConnection.getSchema(), is("bar"));
        statement = barConnection.createStatement();
        statement.execute("create table t (x string) with (number_of_replicas = 0)");
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
    }

    @Before
    public void setUpTable() {
        String stmt = "create table test (" +
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
                ") clustered by (id) into 1 shards with(number_of_replicas=0)";
        try {
            client.sql("drop table test").actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.sql(stmt).actionGet();

        Map<String, Object> objectField = new HashMap<String, Object>(){{put("inner", "Zoon");}};

        SQLRequest sqlRequest = new SQLRequest("insert into test (id, string_field, boolean_field, byte_field, short_field, integer_field," +
                "long_field, float_field, double_field, object_field," +
                "timestamp_field, ip_field, array1, obj_array) values " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", new Object[]{
                1, "Youri", true, 120, 1000, 1200000,
                120000000000L, 1.4, 3.456789, objectField,
                "1970-01-01", "127.0.0.1",
                new Object[]{ "a", "b", "c", "d" },
                new Object[]{ new HashMap<String, Object>() {{ put("bla", "blubb"); }} }
        });
        client.sql(sqlRequest).actionGet();
        client.sql("refresh table test").actionGet();
    }

    @After
    public void tearDownTable() {
        CrateClient client = new CrateClient(hostAndPort);
        try {
            client.sql("drop table test").actionGet();
            client.sql("drop table if exists foo.t").actionGet();
            client.sql("drop table if exists bar.t").actionGet();
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSelectAllTypes() throws Exception {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test");

        assertThat(resultSet, instanceOf(CrateResultSet.class));
        assertThat(((CrateResultSet)resultSet).getCount(), is(1L));
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

        Map<String, Object> objectField = new HashMap<String, Object>(){{put("inner", "Zoon");}};
        assertThat((Map<String, Object>)resultSet.getObject("object_field"), is(objectField));

        Array array1 = resultSet.getArray("array1");
        assertThat(array1.getArray().getClass().isArray(), is(true));
        Assert.assertThat(array1.getBaseType(), is(Types.VARCHAR));
        assertThat((Object[])array1.getArray(), Matchers.<Object>arrayContaining("a", "b", "c", "d"));

        Array objArray = resultSet.getArray("obj_array");
        assertThat(objArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(objArray.getBaseType(), is(Types.JAVA_OBJECT));
        Object firstObject = ((Object[])objArray.getArray())[0];
        Assert.assertThat(firstObject, instanceOf(Map.class));
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
        while(resultSet.next()) {
            assertFalse(resultSet.getString(4).contains("."));
            assertFalse(resultSet.getString(4).contains("["));
            counter++;
        }
        assertThat(counter, is(13));

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
        assertArrayEquals(results, new int[]{1,1,1});

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
            assertThat(e.getMessage(), is("Validation failed for string_field: cannot cast {} to string"));
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
        for (int i = 1; i <= result.getMetaData().getColumnCount();i++) {
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
        assertThat(result.getString(1), is("sys"));
    }
}
