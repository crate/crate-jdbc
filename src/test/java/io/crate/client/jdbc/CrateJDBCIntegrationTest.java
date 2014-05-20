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
import io.crate.action.sql.SQLRequest;
import io.crate.client.AbstractIntegrationTest;
import io.crate.client.CrateClient;
import org.junit.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CrateJDBCIntegrationTest extends AbstractIntegrationTest {

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
        CrateClient client = new CrateClient("localhost:" +  transportPort);

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
}

