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

import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.CrateVersion;
import org.postgresql.jdbc.PgDatabaseMetaData;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TypesITest extends BaseIntegrationTest {

    private static final Instant INSTANT = Instant.parse("2026-08-05T10:00:00Z");

    private static Connection CONNECTION;

    @BeforeAll
    public static void beforeClass() throws Exception {
        dropAllUserTables();
        CONNECTION = connect();
        setUpTestTable();
        insertIntoTestTable();
        setUpArrayTable();
        insertIntoArrayTable();
    }

    @AfterAll
    public static void afterClass() throws SQLException {
        if (CONNECTION != null) {
            CONNECTION.close();
        }
        dropAllUserTables();
    }

    private static void setUpArrayTable() throws SQLException, InterruptedException {
        CONNECTION.createStatement().execute(
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
        Map<String, Object> firstObject = new HashMap<>();
        firstObject.put("element1", "testing");
        Map<String, Object> secondObject = new HashMap<>();
        secondObject.put("element2", "testing2");

        PreparedStatement preparedStatement =
            CONNECTION.prepareStatement("insert into arrayTest (id, str_array, bool_array, byte_array, " +
                                        "short_array, integer_array, long_array, float_array, double_array, timestamp_array, " +
                                        "ip_array, obj_array) values " +
                                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        preparedStatement.setInt(1, 1);
        preparedStatement.setArray(2, CONNECTION.createArrayOf("string", new String[]{"a", "b", "c", "d"}));
        preparedStatement.setArray(3, CONNECTION.createArrayOf("boolean", new Boolean[]{true, false}));
        preparedStatement.setArray(4, CONNECTION.createArrayOf("byte", new Byte[]{(byte) 120, (byte) 100}));
        preparedStatement.setArray(5, CONNECTION.createArrayOf("short", new Short[]{1300, 1200}));
        preparedStatement.setArray(6, CONNECTION.createArrayOf("integer", new Integer[]{2147483647, 234583}));
        preparedStatement.setArray(7, CONNECTION.createArrayOf("long", new Long[]{9223372036854775806L, 4L}));
        preparedStatement.setArray(8, CONNECTION.createArrayOf("float", new Float[]{3.402f, 3.403f, 1.4f}));
        preparedStatement.setArray(9, CONNECTION.createArrayOf("double", new Double[]{1.79769313486231570e+308, 1.69769313486231570e+308}));
        preparedStatement.setArray(10, CONNECTION.createArrayOf("timestamp", new Timestamp[]{new Timestamp(1000L), new Timestamp(2000L)}));
        preparedStatement.setArray(11, CONNECTION.createArrayOf("ip", new String[]{"127.142.132.9", "127.0.0.1"}));
        preparedStatement.setArray(12, CONNECTION.createArrayOf("object", new Object[]{firstObject, secondObject}));
        preparedStatement.execute();
        CONNECTION.createStatement().execute("refresh table arrayTest");
    }

    @Test
    public void testSelectStringType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select string_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.VARCHAR));
        assertThat(resultSet.getString("string_field"), is("Youri"));
    }

    @Test
    public void testSelectBooleanType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select boolean_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.BOOLEAN));
        assertThat(resultSet.getBoolean("boolean_field"), is(true));
    }

    @Test
    public void testSelectByteType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select byte_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(mappingForByteType()));
        assertThat(resultSet.getByte("byte_field"), is((byte) 120));
    }

    @Test
    public void testSelectShortType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select short_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.SMALLINT));
        assertThat(resultSet.getShort("short_field"), is((short) 1000));
    }

    @Test
    public void testSelectIntegerType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select integer_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.INTEGER));
        assertThat(resultSet.getInt("integer_field"), is(1200000));
    }

    @Test
    public void testSelectLongType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select long_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.BIGINT));
        assertThat(resultSet.getLong("long_field"), is(120000000000L));
    }

    @Test
    public void testSelectFloatType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select float_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.REAL));
        assertThat(resultSet.getFloat("float_field"), is(1.4f));
    }

    @Test
    public void testSelectDoubleType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select double_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.DOUBLE));
        assertThat(resultSet.getDouble("double_field"), is(3.456789d));
    }

    @Test
    public void testSelectTimestampType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select timestamp_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.TIMESTAMP));
        // The driver sends a bound timestamp as epoch milliseconds in text, so the
        // value read back follows the JVM time zone. The instant is pinned on
        // timestamptz instead, where the offset travels with the value.
        assertThat(resultSet.getTimestamp("timestamp_field"), instanceOf(Timestamp.class));
    }

    @Test
    public void testSelectIPType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select ip_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.VARCHAR));
        assertThat(resultSet.getString("ip_field"), is("127.0.0.1"));
    }

    @Test
    public void testSelectNumericType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select 1.25::numeric(10,2)");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.NUMERIC));
        assertThat(resultSet.getBigDecimal(1), is(new BigDecimal("1.25")));
    }

    @Test
    public void testSelectTimestampTzType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select '2026-08-05T10:00:00Z'::timestamptz");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.TIMESTAMP));
        assertThat(resultSet.getTimestamp(1), is(Timestamp.from(INSTANT)));
    }

    @Test
    public void testSelectDateCast() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select '2024-01-02'::date");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.DATE));
        assertThat(resultSet.getDate(1), is(Date.valueOf("2024-01-02")));
    }

    @Test
    public void testSelectTimeType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select '12:34:56+01:00'::time with time zone");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.TIME));
        assertThat(resultSet.getTime(1), instanceOf(Time.class));
    }

    @Test
    public void testSelectIntervalType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select '1 day 2 hours'::interval");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.OTHER));
        assertThat(resultSet.getObject(1), instanceOf(PGInterval.class));
    }

    @Test
    public void testSelectBitType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select B'1010'");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.BIT));
        assertThat(resultSet.getObject(1), instanceOf(PGobject.class));
    }

    @Test
    public void testSelectJsonType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select '{\"x\": 10}'::json");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getObject(1), is(Collections.singletonMap("x", 10)));
    }

    @Test
    public void testSelectObjectType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select object_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnType(1), is(Types.JAVA_OBJECT));
        assertThat(resultSet.getObject("object_field"), is(Collections.singletonMap("inner", "Zoon")));
    }

    @Test
    public void testSelectGeoPoint() throws Exception {
        ResultSet rs = CONNECTION.createStatement().executeQuery("SELECT geo_point_field FROM test");
        assertThat(rs.next(), is(true));

        Object geoPoint = rs.getObject("geo_point_field");
        assertThat(geoPoint, Matchers.instanceOf(PGpoint.class));
        PGpoint point = (PGpoint) geoPoint;
        assertThat(point.x, Matchers.closeTo(9.7419, 0.001));
        assertThat(point.y, Matchers.closeTo(47.4048, 0.001));
    }

    @Test
    public void testSelectGeoShape() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select geo_shape_field from test");
        assertThat(resultSet.next(), is(true));
        Map<String, Object> expected = new HashMap<>();
        expected.put("coordinates", Collections.singletonList(
            Arrays.asList(
                Arrays.asList(30.0, 10.0),
                Arrays.asList(40.0, 40.0),
                Arrays.asList(20.0, 40.0),
                Arrays.asList(10.0, 20.0),
                Arrays.asList(30.0, 10.0)
            )));
        expected.put("type", "Polygon");

        assertThat((Map) resultSet.getObject("geo_shape_field"), Is.<Map>is(expected));
        assertThat(resultSet.getObject("geo_shape_field", PGobject.class).getValue(),
            Matchers.allOf(
                Matchers.containsString("\"type\":\"Polygon\""),
                Matchers.containsString("\"coordinates\":[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]")
            )
        );
    }

    @Test
    public void testSelectStringArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select str_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array strArray = resultSet.getArray("str_array");
        assertThat(strArray.getArray().getClass().isArray(), is(true));
        assertThat(strArray.getBaseType(), is(Types.VARCHAR));
        assertThat((Object[]) strArray.getArray(), Matchers.<Object>arrayContaining("a", "b", "c", "d"));
    }

    @Test
    public void testSelectBooleanArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select bool_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array boolArray = resultSet.getArray("bool_array");
        assertThat(boolArray.getArray().getClass().isArray(), is(true));
        assertThat(boolArray.getBaseType(), is(Types.BOOLEAN));
        assertThat((Object[]) boolArray.getArray(), Matchers.<Object>arrayContaining(true, false));
    }

    @Test
    public void testSelectByteArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select byte_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array byteArray = resultSet.getArray("byte_array");
        assertThat(byteArray.getArray().getClass().isArray(), is(true));
        // For more information, see: https://github.com/crate/crate/pull/19578.
        assertThat(byteArray.getBaseType(), is(mappingForByteType()));
        assertThat((Object[]) byteArray.getArray(), arrayContaining(mappingForByteValues(120, 100)));
    }

    private static int mappingForByteType() throws SQLException {
        CrateVersion version = ((PgDatabaseMetaData) CONNECTION.getMetaData()).getCrateVersion();
        return version.compareTo("6.5.0") >= 0 ? Types.SMALLINT : Types.TINYINT;
    }

    private static Number[] mappingForByteValues(int... ints) throws SQLException {
        boolean tinyInt = mappingForByteType() == Types.TINYINT;
        Number[] numbers = new Number[ints.length];
        for (int i = 0; i < ints.length; i++) {
            if (tinyInt) {
                numbers[i] = (byte) ints[i];
            } else {
                numbers[i] = (short) ints[i];
            }
        }
        return numbers;
    }

    @Test
    public void testSelectShortArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select short_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array shortArray = resultSet.getArray("short_array");
        assertThat(shortArray.getArray().getClass().isArray(), is(true));
        assertThat(shortArray.getBaseType(), is(Types.SMALLINT));
        assertThat((Object[]) shortArray.getArray(), Matchers.<Object>arrayContaining((short) 1300, (short) 1200));
    }

    @Test
    public void testSelectIntegerArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select integer_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array integerArray = resultSet.getArray("integer_array");
        assertThat(integerArray.getArray().getClass().isArray(), is(true));
        assertThat(integerArray.getBaseType(), is(Types.INTEGER));
        assertThat((Object[]) integerArray.getArray(), Matchers.<Object>arrayContaining(2147483647, 234583));
    }

    @Test
    public void testSelectLongArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select long_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array longArray = resultSet.getArray("long_array");
        assertThat(longArray.getArray().getClass().isArray(), is(true));
        assertThat(longArray.getBaseType(), is(Types.BIGINT));
        assertThat((Object[]) longArray.getArray(), Matchers.<Object>arrayContaining(9223372036854775806L, 4L));
    }

    @Test
    public void testSelectFloatArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select float_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array floatArray = resultSet.getArray("float_array");
        assertThat(floatArray.getArray().getClass().isArray(), is(true));
        assertThat(floatArray.getBaseType(), is(Types.REAL));
        assertThat((Object[]) floatArray.getArray(), Matchers.<Object>arrayContaining(3.402f, 3.403f, 1.4f));
    }

    @Test
    public void testSelectDoubleArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select double_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array doubleArray = resultSet.getArray("double_array");
        assertThat(doubleArray.getArray().getClass().isArray(), is(true));
        assertThat(doubleArray.getBaseType(), is(Types.DOUBLE));
        assertThat((Object[]) doubleArray.getArray(), Matchers.<Object>arrayContaining(1.79769313486231570e+308, 1.69769313486231570e+308));
    }

    @Test
    public void testSelectTimestampArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select timestamp_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array timestampArray = resultSet.getArray("timestamp_array");
        assertThat(timestampArray.getArray().getClass().isArray(), is(true));
        assertThat(timestampArray.getBaseType(), is(Types.TIMESTAMP));
        // A bound timestamp travels as epoch milliseconds in text, so what comes back
        // follows the JVM time zone. testSetTimestamp pins the instant on timestamptz,
        // where the offset travels with the value.
        assertThat((Object[]) timestampArray.getArray(),
            Matchers.<Object>arrayContaining(instanceOf(Timestamp.class), instanceOf(Timestamp.class)));
    }

    @Test
    public void testSelectIPArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("select ip_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array ipArray = resultSet.getArray("ip_array");
        assertThat(ipArray.getArray().getClass().isArray(), is(true));
        assertThat(ipArray.getBaseType(), is(Types.VARCHAR));
        assertThat((Object[]) ipArray.getArray(), Matchers.<Object>arrayContaining("127.142.132.9", "127.0.0.1"));
    }

    @Test
    public void testSelectObjectArrayType() throws Exception {
        ResultSet resultSet = CONNECTION.createStatement().executeQuery("SELECT obj_array FROM arrayTest");
        assertThat(resultSet.next(), is(true));

        Array objArray = resultSet.getArray("obj_array");
        assertThat(objArray.getArray().getClass().isArray(), is(true));
        assertThat(objArray.getBaseType(), is(Types.JAVA_OBJECT));
        assertThat(objArray.getArray(), is(new Object[]{
            Collections.singletonMap("element1", "testing"),
            Collections.singletonMap("element2", "testing2")}));

        // getObject and getArray report the same elements.
        Object asObject = resultSet.getObject("obj_array");
        assertThat(asObject, is(instanceOf(Array.class)));
        assertThat(((Array) asObject).getArray(), is(objArray.getArray()));
    }

    @Test
    public void testSelectNestedArrayType() throws Exception {
        CONNECTION.createStatement().executeUpdate("create table test_nested_array (rows_ array(array(integer)))");
        try {
            CONNECTION.createStatement().executeUpdate("insert into test_nested_array (rows_) values ([[1, 2], [3]])");
            CONNECTION.createStatement().execute("refresh table test_nested_array");

            ResultSet resultSet = CONNECTION.createStatement().executeQuery("select rows_ from test_nested_array");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject(1), is(List.of(List.of(1, 2), List.of(3))));
            assertThat(resultSet.getString(1), is("[[1,2],[3]]"));
            assertThat((Object[]) resultSet.getArray(1).getArray(),
                arrayContaining((Object) List.of(1, 2), List.of(3)));
        } finally {
            CONNECTION.createStatement().execute("drop table test_nested_array");
        }
    }

    @Test
    public void testSelectNestedObjectArrayType() throws Exception {
        CONNECTION.createStatement().executeUpdate("create table test_nested_objects (rows_ array(array(object)))");
        try {
            CONNECTION.createStatement().executeUpdate(
                "insert into test_nested_objects (rows_) values ([[{a=1}], [{b=2}]])");
            CONNECTION.createStatement().execute("refresh table test_nested_objects");

            ResultSet resultSet = CONNECTION.createStatement().executeQuery("select rows_ from test_nested_objects");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject(1), is(List.of(List.of(Map.of("a", 1)), List.of(Map.of("b", 2)))));
        } finally {
            CONNECTION.createStatement().execute("drop table test_nested_objects");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("setObjectForms")
    public void testSetGetObject(String form, ParameterBinder binder) throws Exception {
        Map<String, Integer> expected = Collections.singletonMap("n", 1);

        CONNECTION.createStatement().executeUpdate("create table test_obj (obj object as (n int))");
        try (PreparedStatement statement = CONNECTION.prepareStatement("insert into test_obj (obj) values (?)")) {
            binder.bind(statement, 1, expected);
            statement.execute();
            CONNECTION.createStatement().execute("refresh table test_obj");

            ResultSet resultSet = CONNECTION.createStatement().executeQuery("select obj from test_obj");
            assertThat(resultSet.next(), is(true));
            assertThat(form, resultSet.getObject(1), is(expected));
        } finally {
            CONNECTION.createStatement().execute("drop table test_obj");
        }
    }

    static Stream<Arguments> setObjectForms() {
        return Stream.of(
            Arguments.of("setObject(index, value)", binder(PreparedStatement::setObject)),
            Arguments.of("setObject(index, value, int)", binder((s, i, v) -> s.setObject(i, v, Types.OTHER))),
            Arguments.of("setObject(index, value, int, scale)",
                binder((s, i, v) -> s.setObject(i, v, Types.OTHER, 0))));
    }

    @FunctionalInterface
    interface ParameterBinder {
        void bind(PreparedStatement statement, int index, Object value) throws SQLException;
    }

    private static ParameterBinder binder(ParameterBinder binder) {
        return binder;
    }

    @Test
    public void testSetArrayFromResultSet() throws SQLException {
        CONNECTION.createStatement().executeUpdate("create table test_array_copy (id integer, str_array array(string))");
        try {
            CONNECTION.createStatement().executeUpdate("insert into test_array_copy (id, str_array) values (1, ['a', 'b'])");
            CONNECTION.createStatement().execute("refresh table test_array_copy");

            ResultSet source = CONNECTION.createStatement().executeQuery("select str_array from test_array_copy where id = 1");
            assertThat(source.next(), is(true));

            PreparedStatement statement = CONNECTION.prepareStatement(
                "insert into test_array_copy (id, str_array) values (2, ?)");
            statement.setArray(1, source.getArray("str_array"));
            statement.execute();
            CONNECTION.createStatement().execute("refresh table test_array_copy");

            ResultSet copy = CONNECTION.createStatement().executeQuery("select str_array from test_array_copy where id = 2");
            assertThat(copy.next(), is(true));
            assertThat((Object[]) copy.getArray("str_array").getArray(), arrayContaining((Object) "a", "b"));
        } finally {
            CONNECTION.createStatement().execute("drop table test_array_copy");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("timestampValues")
    public void testSetTimestamp(String description, Object value) throws Exception {
        CONNECTION.createStatement().executeUpdate("create table test_instants (moment timestamptz)");
        try (PreparedStatement insert = CONNECTION.prepareStatement("insert into test_instants (moment) values (?)")) {
            insert.setObject(1, value);
            insert.executeUpdate();
            CONNECTION.createStatement().execute("refresh table test_instants");

            ResultSet resultSet = CONNECTION.createStatement().executeQuery("select moment from test_instants");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getTimestamp(1).getTime(), is(INSTANT.toEpochMilli()));
            assertThat(resultSet.getObject(1, OffsetDateTime.class).toInstant(), is(INSTANT));
        } finally {
            CONNECTION.createStatement().execute("drop table test_instants");
        }
    }

    static Stream<Arguments> timestampValues() {
        return Stream.of(
            Arguments.of("java.sql.Timestamp", Timestamp.from(INSTANT)),
            Arguments.of("OffsetDateTime at +00", INSTANT.atOffset(ZoneOffset.UTC)),
            Arguments.of("OffsetDateTime at +02", INSTANT.atOffset(ZoneOffset.ofHours(2))),
            // No offset of its own, so the server reads it as UTC.
            Arguments.of("LocalDateTime", LocalDateTime.ofInstant(INSTANT, ZoneOffset.UTC)));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("arrayElementTypeNames")
    public void testCreateArrayOfCrateTypeNames(String typeName, String pgTypeName) throws Exception {
        assertThat(CONNECTION.createArrayOf(typeName, new Object[0]).getBaseTypeName(), is(pgTypeName));
        assertThat(CONNECTION.createArrayOf(typeName.toUpperCase(Locale.ENGLISH), new Object[0]).getBaseTypeName(),
            is(pgTypeName));
    }

    static Stream<Arguments> arrayElementTypeNames() {
        return Stream.of(
            Arguments.of("string", "varchar"),
            Arguments.of("ip", "varchar"),
            Arguments.of("character", "bpchar"),
            Arguments.of("boolean", "bool"),
            Arguments.of("short", "int2"),
            Arguments.of("integer", "int4"),
            Arguments.of("long", "int8"),
            // A CrateDB float is the four-byte one; a PostgreSQL float is not.
            Arguments.of("float", "float4"),
            Arguments.of("real", "float4"),
            Arguments.of("double", "float8"),
            Arguments.of("float_vector", "float4"),
            Arguments.of("object", "json"),
            Arguments.of("geo_shape", "json"),
            Arguments.of("geo_point", "float8"),
            Arguments.of("text", "text"),
            Arguments.of("timestamp", "timestamp"),
            Arguments.of("timestamptz", "timestamptz"),
            Arguments.of("numeric", "numeric"),
            Arguments.of("date", "date"),
            Arguments.of("interval", "interval"),
            Arguments.of("uuid", "uuid"),
            Arguments.of("bit", "bit"));
    }

    @Test
    public void testCreateArrayOfUnknownType() {
        assertThrows(SQLException.class, () -> CONNECTION.createArrayOf("no_such_type", new Object[0]));
    }

    @Test
    public void testWasNull() throws Exception {
        CONNECTION.createStatement().executeUpdate(
            "create table test_nulls (id integer primary key, obj object, texts array(text), label text)");
        try {
            CONNECTION.createStatement().executeUpdate(
                "insert into test_nulls (id, obj, texts, label) values (1, null, null, null),"
                + " (2, {a=1}, ['x'], 'set')");
            CONNECTION.createStatement().execute("refresh table test_nulls");

            ResultSet resultSet = CONNECTION.createStatement().executeQuery("select obj, texts, label from test_nulls order by id");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject("obj"), is(nullValue()));
            assertThat(resultSet.wasNull(), is(true));
            assertThat(resultSet.getArray("texts"), is(nullValue()));
            assertThat(resultSet.wasNull(), is(true));

            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject("obj"), is(Map.of("a", 1)));
            assertThat(resultSet.wasNull(), is(false));
            assertThat(resultSet.getArray("texts"), is(notNullValue()));
            assertThat(resultSet.wasNull(), is(false));
        } finally {
            CONNECTION.createStatement().execute("drop table test_nulls");
        }
    }

    @Test
    public void testUuidType() throws Exception {
        assumeTrue(serverAtLeast(6, 2), "CrateDB has the uuid type since 6.2");
        UUID uuid = UUID.fromString("55d07626-4927-47c5-ba43-a015c23632ef");

        CONNECTION.createStatement().executeUpdate("create table test_uuids (id uuid, ids array(uuid))");
        try (PreparedStatement insert = CONNECTION.prepareStatement("insert into test_uuids (id, ids) values (?, ?)")) {
            insert.setObject(1, uuid);
            insert.setArray(2, CONNECTION.createArrayOf("uuid", new Object[]{uuid}));
            insert.execute();
            CONNECTION.createStatement().execute("refresh table test_uuids");

            ResultSet resultSet = CONNECTION.createStatement().executeQuery("select id, ids from test_uuids");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getMetaData().getColumnType(1), is(Types.OTHER));
            assertThat(resultSet.getObject(1), is(uuid));
            assertThat((Object[]) resultSet.getArray(2).getArray(), arrayContaining((Object) uuid));
        } finally {
            CONNECTION.createStatement().execute("drop table test_uuids");
        }
    }

    @Test
    public void testDateType() throws Exception {
        assumeTrue(serverAtLeast(6, 5), "CrateDB stores date columns since 6.5");
        LocalDate date = LocalDate.of(2026, 9, 17);

        CONNECTION.createStatement().executeUpdate("create table test_dates (d date)");
        try (PreparedStatement insert = CONNECTION.prepareStatement("insert into test_dates (d) values (?)")) {
            insert.setObject(1, date);
            insert.execute();
            CONNECTION.createStatement().execute("refresh table test_dates");

            ResultSet resultSet = CONNECTION.createStatement().executeQuery("select d from test_dates");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject(1, LocalDate.class), is(date));
            assertThat(resultSet.getDate(1), is(Date.valueOf(date)));
        } finally {
            CONNECTION.createStatement().execute("drop table test_dates");
        }
    }
}
