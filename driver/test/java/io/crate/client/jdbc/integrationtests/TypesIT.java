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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGobject;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins how CrateDB data types surface through the crate:// driver: scalar
 * types read back as their natural Java classes, OBJECT columns and OBJECT
 * array elements read back as {@code Map}, and
 * {@link Connection#createArrayOf} accepts CrateDB type names.
 */
public class TypesIT extends BaseIntegrationTest {

    private static Connection conn;

    @BeforeAll
    static void setUpTables() throws Exception {
        dropAllUserTables();
        conn = connect();
        setUpTestTable();
        insertIntoTestTable();
        setUpArrayTable();
        insertIntoArrayTable();
    }

    @AfterAll
    static void tearDownTables() throws Exception {
        if (conn != null) {
            conn.close();
        }
        dropAllUserTables();
    }

    private static void setUpArrayTable() throws SQLException, InterruptedException {
        conn.createStatement().execute(
            "create table if not exists arrayTest (" +
            " id integer primary key," +
            " str_array array(string)," +
            " bool_array array(boolean)," +
            " byte_array array(byte)," +
            " short_array array(short)," +
            " integer_array array(integer)," +
            " long_array array(long)," +
            " float_array array(float)," +
            " double_array array(double)," +
            " timestamp_array array(timestamp)," +
            " ip_array array(ip)," +
            " obj_array array(object)" +
            ") clustered by (id) into 1 shards with (number_of_replicas=0)");
        ensureYellow();
    }

    private static void insertIntoArrayTable() throws SQLException {
        PreparedStatement preparedStatement =
            conn.prepareStatement("insert into arrayTest (id, str_array, bool_array, byte_array, " +
                                  "short_array, integer_array, long_array, float_array, double_array, timestamp_array, " +
                                  "ip_array, obj_array) values " +
                                  "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        Map<String, Object> firstObj = new HashMap<>();
        firstObj.put("element1", "testing");
        Map<String, Object> secondObj = new HashMap<>();
        secondObj.put("element2", "testing2");

        preparedStatement.setInt(1, 1);
        preparedStatement.setArray(2, conn.createArrayOf("string", new String[]{"a", "b", "c", "d"}));
        preparedStatement.setArray(3, conn.createArrayOf("boolean", new Boolean[]{true, false}));
        preparedStatement.setArray(4, conn.createArrayOf("byte", new Short[]{120, 100}));
        preparedStatement.setArray(5, conn.createArrayOf("short", new Short[]{1300, 1200}));
        preparedStatement.setArray(6, conn.createArrayOf("integer", new Integer[]{2147483647, 234583}));
        preparedStatement.setArray(7, conn.createArrayOf("long", new Long[]{9223372036854775806L, 4L}));
        preparedStatement.setArray(8, conn.createArrayOf("float", new Float[]{3.402f, 3.403f, 1.4f}));
        preparedStatement.setArray(9, conn.createArrayOf("double", new Double[]{1.79769313486231570e+308, 1.69769313486231570e+308}));
        preparedStatement.setArray(10, conn.createArrayOf("timestamp", new Timestamp[]{new Timestamp(1000L), new Timestamp(2000L)}));
        preparedStatement.setArray(11, conn.createArrayOf("ip", new String[]{"127.142.132.9", "127.0.0.1"}));
        preparedStatement.setArray(12, conn.createArrayOf("object", new Object[]{firstObj, secondObj}));
        preparedStatement.execute();
        conn.createStatement().execute("refresh table arrayTest");
    }

    @Test
    public void selectStringType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select string_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getString("string_field"), is("Youri"));
    }

    @Test
    public void selectBooleanType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select boolean_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getBoolean("boolean_field"), is(true));
    }

    @Test
    public void selectByteType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select byte_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getByte("byte_field"), is((byte) 120));
    }

    @Test
    public void selectShortType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select short_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getShort("short_field"), is((short) 1000));
    }

    @Test
    public void selectIntegerType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select integer_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getInt("integer_field"), is(1200000));
    }

    @Test
    public void selectLongType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select long_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getLong("long_field"), is(120000000000L));
    }

    @Test
    public void selectFloatType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select float_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getFloat("float_field"), is(1.4f));
    }

    @Test
    public void selectDoubleType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select double_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getDouble("double_field"), is(3.456789d));
    }

    @Test
    public void selectTimestampType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select timestamp_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getTimestamp("timestamp_field"), is(new Timestamp(1000L)));
    }

    @Test
    public void selectIpType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select ip_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getString("ip_field"), is("127.0.0.1"));
    }

    @Test
    public void selectGeoPointAsPgPoint() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("SELECT geo_point_field FROM test");
        assertThat(rs.next(), is(true));
        Object geoPoint = rs.getObject("geo_point_field");
        assertThat(geoPoint, Matchers.instanceOf(PGpoint.class));
        PGpoint point = (PGpoint) geoPoint;
        assertThat(point.x, Matchers.closeTo(9.7419, 0.001));
        assertThat(point.y, Matchers.closeTo(47.4048, 0.001));
    }

    @Test
    public void selectGeoShapeAsMap() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select geo_shape_field from test");
        assertThat(resultSet.next(), is(true));

        Map<String, Object> expected = new HashMap<>();
        expected.put("coordinates", Collections.singletonList(
            Arrays.asList(
                Arrays.asList(30.0, 10.0),
                Arrays.asList(40.0, 40.0),
                Arrays.asList(20.0, 40.0),
                Arrays.asList(10.0, 20.0),
                Arrays.asList(30.0, 10.0)
            )
        ));
        expected.put("type", "Polygon");
        assertThat(resultSet.getObject("geo_shape_field"), is(expected));

        assertThat(resultSet.getObject("geo_shape_field", PGobject.class).getValue(),
            Matchers.allOf(
                Matchers.containsString("\"type\":\"Polygon\""),
                Matchers.containsString("\"coordinates\":[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]")
            )
        );
    }

    @Test
    public void selectStringArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select str_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array strArray = resultSet.getArray("str_array");
        assertThat(strArray.getArray().getClass().isArray(), is(true));
        assertThat(strArray.getBaseType(), is(Types.VARCHAR));
        assertThat((Object[]) strArray.getArray(), Matchers.<Object>arrayContaining("a", "b", "c", "d"));
    }

    @Test
    public void selectBooleanArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select bool_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array boolArray = resultSet.getArray("bool_array");
        assertThat(boolArray.getArray().getClass().isArray(), is(true));
        assertThat(boolArray.getBaseType(), is(Types.BIT));
        assertThat((Object[]) boolArray.getArray(), Matchers.<Object>arrayContaining(true, false));
    }

    @Test
    public void selectByteArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select byte_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array byteArray = resultSet.getArray("byte_array");
        assertThat(byteArray.getArray().getClass().isArray(), is(true));
        // CrateDB's byte maps to the PostgreSQL "char" type.
        assertThat(byteArray.getBaseType(), is(Types.CHAR));
        Object[] elements = (Object[]) byteArray.getArray();
        assertThat(elements.length, is(2));
        assertThat(String.valueOf(elements[0]), is("120"));
        assertThat(String.valueOf(elements[1]), is("100"));
    }

    @Test
    public void selectShortArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select short_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array shortArray = resultSet.getArray("short_array");
        assertThat(shortArray.getArray().getClass().isArray(), is(true));
        assertThat(shortArray.getBaseType(), is(Types.SMALLINT));
        assertThat((Object[]) shortArray.getArray(), Matchers.<Object>arrayContaining((short) 1300, (short) 1200));
    }

    @Test
    public void selectIntegerArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select integer_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array integerArray = resultSet.getArray("integer_array");
        assertThat(integerArray.getArray().getClass().isArray(), is(true));
        assertThat(integerArray.getBaseType(), is(Types.INTEGER));
        assertThat((Object[]) integerArray.getArray(), Matchers.<Object>arrayContaining(2147483647, 234583));
    }

    @Test
    public void selectLongArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select long_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array longArray = resultSet.getArray("long_array");
        assertThat(longArray.getArray().getClass().isArray(), is(true));
        assertThat(longArray.getBaseType(), is(Types.BIGINT));
        assertThat((Object[]) longArray.getArray(), Matchers.<Object>arrayContaining(9223372036854775806L, 4L));
    }

    @Test
    public void selectFloatArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select float_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array floatArray = resultSet.getArray("float_array");
        assertThat(floatArray.getArray().getClass().isArray(), is(true));
        assertThat(floatArray.getBaseType(), is(Types.REAL));
        assertThat((Object[]) floatArray.getArray(), Matchers.<Object>arrayContaining(3.402f, 3.403f, 1.4f));
    }

    @Test
    public void selectDoubleArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select double_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array doubleArray = resultSet.getArray("double_array");
        assertThat(doubleArray.getArray().getClass().isArray(), is(true));
        assertThat(doubleArray.getBaseType(), is(Types.DOUBLE));
        assertThat((Object[]) doubleArray.getArray(), Matchers.<Object>arrayContaining(1.79769313486231570e+308, 1.69769313486231570e+308));
    }

    @Test
    public void selectTimestampArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select timestamp_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array timestampArray = resultSet.getArray("timestamp_array");
        assertThat(timestampArray.getArray().getClass().isArray(), is(true));
        assertThat(timestampArray.getBaseType(), is(Types.TIMESTAMP));
        assertThat((Object[]) timestampArray.getArray(), Matchers.<Object>arrayContaining(new Timestamp(1000L), new Timestamp(2000L)));
    }

    @Test
    public void selectIpArrayType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select ip_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array ipArray = resultSet.getArray("ip_array");
        assertThat(ipArray.getArray().getClass().isArray(), is(true));
        assertThat(ipArray.getBaseType(), is(Types.VARCHAR));
        assertThat((Object[]) ipArray.getArray(), Matchers.<Object>arrayContaining("127.142.132.9", "127.0.0.1"));
    }

    @Test
    public void selectObjectArrayTypeElementsReadAsMaps() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("SELECT obj_array FROM arrayTest");
        assertThat(resultSet.next(), is(true));

        Array objArray = resultSet.getArray("obj_array");
        assertThat(objArray.getArray().getClass().isArray(), is(true));
        assertThat(objArray.getBaseType(), is(Types.OTHER));

        Map<String, Object> firstObj = new HashMap<>();
        firstObj.put("element1", "testing");
        Map<String, Object> secondObj = new HashMap<>();
        secondObj.put("element2", "testing2");
        assertThat((Object[]) objArray.getArray(), arrayContaining(firstObj, secondObj));
    }

    @Test
    public void objectColumnRoundTripsAsMap() throws SQLException {
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
