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

package io.crate.client.jdbc;

import com.google.common.base.Splitter;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLRequest;
import io.crate.client.AbstractIntegrationTest;
import io.crate.client.CrateClient;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CrateJDBCIntegrationTest extends AbstractIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static Connection connection;

    @BeforeClass
    public static void setUpClass() throws Exception {
        AbstractIntegrationTest.setUpClass();
        for (String path : Splitter.on(":").split(System.getProperty("java.class.path"))) {
            System.out.println(path);
        }
        Class.forName("io.crate.client.jdbc.CrateDriver");
        connection = DriverManager.getConnection("crate://127.0.0.1:" + transportPort);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        connection.close();
        connection = null;
        AbstractIntegrationTest.tearDownClass();
    }

    @Before
    public void setUpTable() {
        CrateClient client = new CrateClient("127.0.0.1:" +  transportPort);

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
                " ip_field ip" +
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
                "timestamp_field, ip_field) values " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", new Object[]{
                1, "Youri", true, 120, 1000, 1200000,
                120000000000L, 1.4, 3.456789, objectField,
                "1970-01-01", "127.0.0.1"
        });
        client.sql(sqlRequest).actionGet();
        client.sql("refresh table test").actionGet();
    }

    @After
    public void tearDownTable() {
        CrateClient client = new CrateClient("localhost:" +  transportPort);
        try {
            client.sql("drop table test").actionGet();
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
            assertThat(e.getMessage(), is("argument 2 of bulk arguments contains mixed data types"));
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
        assertThat(metaData.getColumnCount(), is(12));
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
}
