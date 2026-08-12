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

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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

    /**
     * Each scalar type reads back through the typed getter JDBC pairs it
     * with, at the value it was written with.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("scalarColumns")
    public void scalarColumnsReadThroughTheirTypedGetter(
            String column, ColumnReader reader, Object value) throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select " + column + " from test");
        assertThat(resultSet.next(), is(true));
        assertThat(reader.read(resultSet, column), is(value));
    }

    static Stream<Arguments> scalarColumns() {
        return Stream.of(
            Arguments.of("string_field", reader(ResultSet::getString), "Youri"),
            Arguments.of("boolean_field", reader(ResultSet::getBoolean), true),
            Arguments.of("byte_field", reader(ResultSet::getByte), (byte) 120),
            Arguments.of("short_field", reader(ResultSet::getShort), (short) 1000),
            Arguments.of("integer_field", reader(ResultSet::getInt), 1200000),
            Arguments.of("long_field", reader(ResultSet::getLong), 120000000000L),
            Arguments.of("float_field", reader(ResultSet::getFloat), 1.4f),
            Arguments.of("double_field", reader(ResultSet::getDouble), 3.456789d),
            Arguments.of("timestamp_field", reader(ResultSet::getTimestamp), new Timestamp(1000L)),
            Arguments.of("ip_field", reader(ResultSet::getString), "127.0.0.1")
        );
    }

    @FunctionalInterface
    interface ColumnReader {
        Object read(ResultSet resultSet, String column) throws SQLException;
    }

    private static ColumnReader reader(ColumnReader reader) {
        return reader;
    }

    /** CrateDB gained the {@code uuid} type in 6.2. */
    @Test
    public void selectUuidType() throws Exception {
        assumeTrue(serverAtLeast(6, 2), "uuid needs CrateDB 6.2");
        ResultSet resultSet = conn.createStatement()
            .executeQuery("select '55d07626-4927-47c5-ba43-a015c23632ef'::uuid");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getObject(1),
            is(UUID.fromString("55d07626-4927-47c5-ba43-a015c23632ef")));
    }

    /**
     * CrateDB's {@code json} type exists for PostgreSQL interoperability and
     * travels the wire the way an OBJECT does, so it reads back as a
     * {@code Map} like one.
     */
    @Test
    public void selectJsonType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select '{\"x\": 10}'::json");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getObject(1), is(Collections.singletonMap("x", 10L)));
    }

    /**
     * The types CrateDB shares with PostgreSQL outright. pgJDBC decodes them
     * with no help from the wrapper layer, which holds only for as long as
     * CrateDB keeps putting them on the wire under the PostgreSQL type name
     * pgJDBC has a decoder for. {@code docs/data-types.rst} states this
     * mapping to users.
     */
    static Stream<Arguments> postgresqlTypeMappings() {
        return Stream.of(
            Arguments.of("1.25::numeric(10,2)", "numeric", BigDecimal.class, "1.25"),
            Arguments.of("'2024-01-02'::date", "date", Date.class, "2024-01-02"),
            // A java.sql.Time is a moment rendered as a wall clock, so the
            // reading of one that names an offset is the JVM's own.
            Arguments.of("'12:34:56+01:00'::time with time zone", "timetz", Time.class,
                LocalTime.ofInstant(OffsetTime.parse("12:34:56+01:00")
                    .withOffsetSameInstant(ZoneOffset.UTC)
                    .atDate(LocalDate.ofEpochDay(0)).toInstant(), ZoneId.systemDefault()).toString()),
            Arguments.of("'ab'::char(5)", "bpchar", String.class, "ab   "),
            Arguments.of("'ab'::varchar(10)", "varchar", String.class, "ab"),
            Arguments.of("B'1010'", "bit", PGobject.class, "1010"),
            Arguments.of("'1 day 2 hours'::interval", "interval", PGInterval.class, "1 days 2 hours")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("postgresqlTypeMappings")
    public void typesSharedWithPostgresqlReadWithoutConversion(
            String expression, String pgTypeName, Class<?> javaType, String text) throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select " + expression);
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnTypeName(1), is(pgTypeName));

        Object value = resultSet.getObject(1);
        assertThat(value, instanceOf(javaType));
        assertThat(String.valueOf(value), is(text));
    }

    /**
     * A {@code numeric} column round-trips through {@code BigDecimal},
     * unlike the expression above it is stored and read back.
     */
    @Test
    public void numericColumnsRoundTripAsBigDecimal() throws Exception {
        conn.createStatement().execute(
            "create table if not exists numeric_test (id integer primary key, amount numeric(10,2))");
        try (PreparedStatement insert = conn.prepareStatement(
                "insert into numeric_test (id, amount) values (?, ?)")) {
            insert.setInt(1, 1);
            insert.setBigDecimal(2, new BigDecimal("1234.56"));
            insert.execute();
        }
        try {
            conn.createStatement().execute("refresh table numeric_test");

            ResultSet resultSet = conn.createStatement().executeQuery("select amount from numeric_test");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getBigDecimal(1), is(new BigDecimal("1234.56")));
        } finally {
            conn.createStatement().execute("drop table numeric_test");
        }
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

    /**
     * Array columns come back as {@link Array}s that report the JDBC type of
     * their elements and hand them over as Java values — OBJECT elements as
     * maps, like everywhere else in this driver.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("arrayColumns")
    public void arrayColumnsReadAsTypedArrays(
            String column, Matcher<Integer> baseType, Matcher<Object[]> elements) throws Exception {
        ResultSet resultSet = conn.createStatement()
            .executeQuery("select " + column + " from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array array = resultSet.getArray(column);
        assertThat(array.getArray().getClass().isArray(), is(true));
        assertThat(array.getBaseType(), baseType);
        assertThat((Object[]) array.getArray(), elements);
    }

    static Stream<Arguments> arrayColumns() {
        Map<String, Object> firstObject = new HashMap<>();
        firstObject.put("element1", "testing");
        Map<String, Object> secondObject = new HashMap<>();
        secondObject.put("element2", "testing2");
        return Stream.of(
            Arguments.of("str_array", is(Types.VARCHAR),
                Matchers.<Object>arrayContaining("a", "b", "c", "d")),
            Arguments.of("bool_array", is(Types.BIT),
                Matchers.<Object>arrayContaining(true, false)),
            Arguments.of("short_array", is(Types.SMALLINT),
                Matchers.<Object>arrayContaining((short) 1300, (short) 1200)),
            Arguments.of("integer_array", is(Types.INTEGER),
                Matchers.<Object>arrayContaining(2147483647, 234583)),
            Arguments.of("long_array", is(Types.BIGINT),
                Matchers.<Object>arrayContaining(9223372036854775806L, 4L)),
            Arguments.of("float_array", is(Types.REAL),
                Matchers.<Object>arrayContaining(3.402f, 3.403f, 1.4f)),
            Arguments.of("double_array", is(Types.DOUBLE),
                Matchers.<Object>arrayContaining(1.79769313486231570e+308, 1.69769313486231570e+308)),
            Arguments.of("timestamp_array", is(Types.TIMESTAMP),
                Matchers.<Object>arrayContaining(new Timestamp(1000L), new Timestamp(2000L))),
            Arguments.of("ip_array", is(Types.VARCHAR),
                Matchers.<Object>arrayContaining("127.142.132.9", "127.0.0.1")),
            Arguments.of("obj_array", is(Types.OTHER),
                Matchers.<Object>arrayContaining(firstObject, secondObject))
        );
    }

    /**
     * A {@code byte} column is described by the width the server puts it on:
     * the PostgreSQL {@code "char"} type up to CrateDB 6.4, a small integer
     * from 6.5. An untyped read follows that width, so {@code getObject}
     * answers a {@code String} on one and an {@code Integer} on the other.
     *
     * <p>{@code getByte()} reads the byte either way. That is the contract an
     * application relies on, and the reason the wire type moving underneath it
     * is a documented difference rather than a break.</p>
     */
    @Test
    public void aByteColumnIsDescribedByTheWidthTheServerSendsIt() throws SQLException {
        ResultSet resultSet = conn.createStatement().executeQuery("select byte_field from test");
        assertThat(resultSet.next(), is(true));

        assertThat(resultSet.getByte(1), is((byte) 120));
        if (serverAtLeast(6, 5)) {
            assertThat(resultSet.getMetaData().getColumnType(1), is(Types.SMALLINT));
            assertThat(resultSet.getMetaData().getColumnTypeName(1), is("int2"));
            assertThat(resultSet.getObject(1), is(120));
        } else {
            assertThat(resultSet.getMetaData().getColumnType(1), is(Types.CHAR));
            assertThat(resultSet.getMetaData().getColumnTypeName(1), is("char"));
            assertThat(resultSet.getObject(1), is("120"));
        }
    }

    /**
     * An array of {@code byte} follows the same move, and its elements arrive
     * as the narrowest box for the width they are read at — {@code Short} from
     * 6.5 on, where the scalar column reads as an {@code Integer}.
     */
    @Test
    public void aByteArrayIsDescribedByTheWidthTheServerSendsIt() throws SQLException {
        ResultSet resultSet = conn.createStatement()
            .executeQuery("select byte_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Array array = resultSet.getArray("byte_array");
        if (serverAtLeast(6, 5)) {
            assertThat(array.getBaseType(), is(Types.SMALLINT));
            assertThat(array.getBaseTypeName(), is("int2"));
            assertThat((Object[]) array.getArray(),
                Matchers.<Object>arrayContaining((short) 120, (short) 100));
        } else {
            assertThat(array.getBaseType(), is(Types.CHAR));
            assertThat(array.getBaseTypeName(), is("char"));
            assertThat((Object[]) array.getArray(),
                Matchers.<Object>arrayContaining("120", "100"));
        }
    }

    @Test
    public void objectColumnRoundTripsAsMap() throws SQLException {
        Map<String, Long> expected = new HashMap<>();
        expected.put("n", 1L);

        conn.createStatement().executeUpdate("create table test_obj (obj object as (n int))");
        try (PreparedStatement statement = conn.prepareStatement("insert into test_obj (obj) values (?)")) {
            statement.setObject(1, expected);
            statement.execute();
            conn.createStatement().execute("refresh table test_obj");

            ResultSet resultSet = conn.createStatement().executeQuery("select obj from test_obj");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject(1), is(expected));
        } finally {
            conn.createStatement().execute("drop table test_obj");
        }
    }

    @Test
    public void arrayReadFromAResultSetCanBeBoundAgain() throws SQLException {
        conn.createStatement().executeUpdate(
            "create table test_array_copy (id integer, str_array array(string))");
        try {
            conn.createStatement().executeUpdate(
                "insert into test_array_copy (id, str_array) values (1, ['a', 'b'])");
            conn.createStatement().execute("refresh table test_array_copy");

            ResultSet source = conn.createStatement().executeQuery(
                "select str_array from test_array_copy where id = 1");
            assertThat(source.next(), is(true));

            PreparedStatement statement = conn.prepareStatement(
                "insert into test_array_copy (id, str_array) values (2, ?)");
            statement.setArray(1, source.getArray("str_array"));
            statement.execute();
            conn.createStatement().execute("refresh table test_array_copy");

            ResultSet copy = conn.createStatement().executeQuery(
                "select str_array from test_array_copy where id = 2");
            assertThat(copy.next(), is(true));
            assertThat((Object[]) copy.getArray("str_array").getArray(), arrayContaining((Object) "a", "b"));
        } finally {
            conn.createStatement().execute("drop table test_array_copy");
        }
    }

    /**
     * A series of {@code Map}s binds to a column of {@code array(object)}
     * however it is written — the shapes a caller reads one back as, and the
     * shapes they build one in.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("objectArrayParameters")
    public void seriesOfMapsBindToAnObjectArrayColumn(String description, Object value) throws SQLException {
        conn.createStatement().executeUpdate("create table test_obj_list (objs array(object))");
        try (PreparedStatement statement =
                 conn.prepareStatement("insert into test_obj_list (objs) values (?)")) {
            statement.setObject(1, value);
            statement.execute();
            conn.createStatement().execute("refresh table test_obj_list");

            ResultSet resultSet = conn.createStatement().executeQuery("select objs from test_obj_list");
            assertThat(resultSet.next(), is(true));
            assertThat((Object[]) resultSet.getArray("objs").getArray(),
                arrayContaining(Map.of("element1", "testing"), Map.of("element2", "testing2")));
        } finally {
            conn.createStatement().execute("drop table test_obj_list");
        }
    }

    static Stream<Arguments> objectArrayParameters() {
        Map<String, Object> first = Map.of("element1", "testing");
        Map<String, Object> second = Map.of("element2", "testing2");
        return Stream.of(
            Arguments.of("setObject(List of Maps)", List.of(first, second)),
            Arguments.of("setObject(Set of Maps)", new LinkedHashSet<>(List.of(first, second))),
            Arguments.of("setObject(Map[])", new Map<?, ?>[]{first, second})
        );
    }

    @Test
    public void collectionMixingObjectsWithOtherValuesIsRejected() throws SQLException {
        conn.createStatement().executeUpdate("create table test_mixed_list (objs array(object))");
        try (PreparedStatement statement =
                 conn.prepareStatement("insert into test_mixed_list (objs) values (?)")) {
            assertThrows(SQLException.class,
                () -> statement.setObject(1, List.of(Map.of("a", 1), "not an object")));
        } finally {
            conn.createStatement().execute("drop table test_mixed_list");
        }
    }

    /**
     * {@code getObject} and {@code getArray} read the same column the same
     * way: an array of OBJECT is an array of {@code Map} either way.
     */
    @Test
    public void objectArraysReadTheSameThroughGetObjectAndGetArray() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select obj_array from arrayTest");
        assertThat(resultSet.next(), is(true));

        Object asObject = resultSet.getObject("obj_array");
        assertThat(asObject, is(instanceOf(Array.class)));
        assertThat(((Object[]) ((Array) asObject).getArray())[0], is(instanceOf(Map.class)));
        assertThat(((Object[]) resultSet.getArray("obj_array").getArray())[0],
            is(((Object[]) ((Array) asObject).getArray())[0]));
    }

    /**
     * Asking for a column as a {@link Array} reads the array this driver
     * reads, whichever type the column is: the elements are converted as
     * {@code getArray()} converts them, and the json columns pgJDBC has no
     * array decoder for answer here too.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("arrayQueries")
    public void arraysReadTheSameThroughGetArrayAndAsAnArrayType(String description, String query)
            throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery(query);
        assertThat(resultSet.next(), is(true));

        Array asArrayType = resultSet.getObject(1, Array.class);
        assertThat(asArrayType, is(instanceOf(Array.class)));
        assertThat((Object[]) asArrayType.getArray(),
            is((Object[]) resultSet.getArray(1).getArray()));
    }

    static Stream<Arguments> arrayQueries() {
        return Stream.of(
            Arguments.of("array(object)", "select obj_array from arrayTest"),
            Arguments.of("array(integer)", "select integer_array from arrayTest"),
            Arguments.of("array(array(integer))", "select [[1, 2], [3]]")
        );
    }

    @Test
    public void objectColumnsReadIntoTheRequestedMapType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select object_field from test");
        assertThat(resultSet.next(), is(true));

        assertThat(resultSet.getObject(1, Map.class).get("inner"), is("Zoon"));
        assertThat(resultSet.getObject(1, HashMap.class).get("inner"), is("Zoon"));
        assertThat(resultSet.getObject(1, Object.class), is(instanceOf(Map.class)));
    }

    /**
     * What a column is described as has to be something the column can
     * actually produce. The json CrateDB sends an OBJECT and a nested array
     * under is one type carrying two shapes, so the described class is the one
     * they have in common rather than the {@code PGobject} neither is.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonColumnQueries")
    public void jsonColumnsAreDescribedAsWhatTheyReadAs(String description, String query) throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery(query);
        assertThat(resultSet.next(), is(true));

        String describedClass = resultSet.getMetaData().getColumnClassName(1);
        assertThat(describedClass, is(Object.class.getName()));
        assertThat(Class.forName(describedClass).isInstance(resultSet.getObject(1)), is(true));
    }

    static Stream<Arguments> jsonColumnQueries() {
        return Stream.of(
            Arguments.of("object", "select object_field from test"),
            Arguments.of("array(array(integer))", "select [[1, 2], [3]]")
        );
    }

    /** A column pgJDBC reads itself keeps the description pgJDBC gives it. */
    @Test
    public void columnsPgjdbcReadsKeepTheirDescribedClass() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select string_field from test");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getMetaData().getColumnClassName(1), is(String.class.getName()));
    }

    /**
     * A column of {@code array(array(...))} travels as json — the PostgreSQL
     * array format cannot hold sub-arrays of differing length — and reads
     * back through every way of asking for an array or for the value itself.
     */
    @Test
    public void nestedArraysReadAsArraysOfArrays() throws Exception {
        conn.createStatement().executeUpdate(
            "create table test_nested_array (rows_ array(array(integer)))");
        try {
            conn.createStatement().executeUpdate(
                "insert into test_nested_array (rows_) values ([[1, 2], [3]])");
            conn.createStatement().execute("refresh table test_nested_array");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select rows_ from test_nested_array");
            assertThat(resultSet.next(), is(true));

            assertThat(resultSet.getObject(1), is(List.of(List.of(1L, 2L), List.of(3L))));
            assertThat(resultSet.getObject(1, List.class), is(List.of(List.of(1L, 2L), List.of(3L))));
            assertThat(resultSet.getString(1), is("[[1,2],[3]]"));

            Object[] rows = (Object[]) resultSet.getArray(1).getArray();
            assertThat(rows, arrayContaining(new Object[]{1L, 2L}, new Object[]{3L}));
            assertThat(resultSet.getArray("rows_").getArray(), is(rows));
            assertThat(resultSet.getArray(1).getBaseType(), is(Types.OTHER));
        } finally {
            conn.createStatement().execute("drop table test_nested_array");
        }
    }

    /**
     * A nested column of an OBJECT takes the CrateDB type its json form
     * implies, so what a value is written as decides what the column becomes.
     * Only the types a dynamic object infers from are pinned here; a declared
     * column takes what it was declared as.
     */
    @Test
    public void objectMembersTakeTheTypeTheirJsonFormImplies() throws Exception {
        conn.createStatement().executeUpdate("create table test_object_types (o object(dynamic))");
        try {
            Map<String, Object> value = new HashMap<>();
            value.put("whole", 5L);
            value.put("fractional", 3.5d);
            value.put("text", "a");
            value.put("moment", OffsetDateTime.parse("2026-08-05T10:00:00Z"));
            try (PreparedStatement insert =
                     conn.prepareStatement("insert into test_object_types (o) values (?)")) {
                insert.setObject(1, value);
                insert.executeUpdate();
            }
            conn.createStatement().execute("refresh table test_object_types");

            Map<String, String> types = new HashMap<>();
            ResultSet columns = conn.createStatement().executeQuery(
                "select column_name, data_type from information_schema.columns "
                + "where table_name = 'test_object_types' and column_name like 'o[%'");
            while (columns.next()) {
                types.put(columns.getString(1), columns.getString(2));
            }
            assertThat(types.get("o['whole']"), is("bigint"));
            assertThat(types.get("o['fractional']"), is("double precision"));
            assertThat(types.get("o['text']"), is("text"));
            // Json has no moment in time to infer one from: what a timestamp
            // is written as is text, and text is what the column becomes.
            assertThat(types.get("o['moment']"), is("text"));

            ResultSet read = conn.createStatement().executeQuery("select o from test_object_types");
            assertThat(read.next(), is(true));
            Map<?, ?> back = (Map<?, ?>) read.getObject(1);
            assertThat(back.get("whole"), is(5L));
            assertThat(back.get("fractional"), is(3.5d));
        } finally {
            conn.createStatement().execute("drop table test_object_types");
        }
    }

    /**
     * A nested column declared as a timestamp takes one, which is where the
     * form a temporal value is written in has to be one CrateDB reads a
     * timestamp from.
     */
    @Test
    public void aDeclaredTimestampMemberTakesATemporalValue() throws Exception {
        conn.createStatement().executeUpdate(
            "create table test_object_time (o object(strict) as (at timestamp with time zone))");
        try (PreparedStatement insert =
                 conn.prepareStatement("insert into test_object_time (o) values (?)")) {
            insert.setObject(1, Map.of("at", OffsetDateTime.parse("2026-08-05T10:00:00Z")));
            insert.executeUpdate();
            conn.createStatement().execute("refresh table test_object_time");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select o['at'] from test_object_time");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getTimestamp(1).getTime(),
                is(Instant.parse("2026-08-05T10:00:00Z").toEpochMilli()));
        } finally {
            conn.createStatement().execute("drop table test_object_time");
        }
    }

    /**
     * A whole number in an OBJECT is a {@code bigint} whatever Java box it was
     * written from, so it reads back as a {@code Long} at any magnitude.
     */
    @Test
    public void wholeNumbersInAnObjectReadBackAsLong() throws Exception {
        Map<String, Object> value = new HashMap<>();
        value.put("small", 5);
        value.put("large", 5_000_000_000L);

        try (PreparedStatement select = conn.prepareStatement("select ?::object")) {
            select.setObject(1, value);
            ResultSet resultSet = select.executeQuery();
            assertThat(resultSet.next(), is(true));

            Map<?, ?> back = (Map<?, ?>) resultSet.getObject(1);
            assertThat(back.get("small"), is(5L));
            assertThat(back.get("large"), is(5_000_000_000L));
        }
    }

    /**
     * The elements of a nested array are arrays, which the PostgreSQL
     * protocol has no column descriptor for, so the index/value view of the
     * array is the one thing it cannot offer.
     */
    @Test
    public void nestedArraysHaveNoElementResultSet() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery("select [[1, 2], [3]]");
        assertThat(resultSet.next(), is(true));
        assertThrows(SQLFeatureNotSupportedException.class,
            () -> resultSet.getArray(1).getResultSet());
    }

    /**
     * Nested arrays bind the way any other value does — as the nested Java
     * collections or arrays they are read back as, and through
     * {@code createArrayOf}.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nestedArrayParameters")
    public void nestedArraysBindAsParameters(String description, Object value) throws Exception {
        conn.createStatement().executeUpdate(
            "create table test_nested_binding (id integer primary key, rows_ array(array(integer)))");
        try (PreparedStatement insert = conn.prepareStatement(
                "insert into test_nested_binding (id, rows_) values (1, ?)")) {
            if (value instanceof Array) {
                insert.setArray(1, (Array) value);
            } else {
                insert.setObject(1, value);
            }
            insert.execute();
            conn.createStatement().execute("refresh table test_nested_binding");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select rows_ from test_nested_binding");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject(1), is(List.of(List.of(1L, 2L), List.of(3L))));
        } finally {
            conn.createStatement().execute("drop table test_nested_binding");
        }
    }

    static Stream<Arguments> nestedArrayParameters() throws SQLException {
        return Stream.of(
            Arguments.of("setObject(List of Lists)", List.of(List.of(1, 2), List.of(3))),
            Arguments.of("setObject(Object[][])", new Object[][]{{1, 2}, {3}}),
            Arguments.of("setObject(int[][])", new int[][]{{1, 2}, {3}}),
            Arguments.of("setArray(createArrayOf)",
                conn.createArrayOf("integer", new Object[][]{{1, 2}, {3}}))
        );
    }

    /**
     * OBJECT values inside a nested array are maps, as they are wherever else
     * this driver reads them.
     */
    @Test
    public void nestedArraysOfObjectsReadAsMaps() throws Exception {
        conn.createStatement().executeUpdate(
            "create table test_nested_objects (rows_ array(array(object)))");
        try (PreparedStatement insert = conn.prepareStatement(
                "insert into test_nested_objects (rows_) values (?)")) {
            insert.setObject(1, List.of(List.of(Map.of("a", 1)), List.of(Map.of("b", 2))));
            insert.execute();
            conn.createStatement().execute("refresh table test_nested_objects");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select rows_ from test_nested_objects");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject(1),
                is(List.of(List.of(Map.of("a", 1L)), List.of(Map.of("b", 2L)))));

            Object[] rows = (Object[]) resultSet.getArray(1).getArray();
            assertThat(rows, arrayContaining(
                new Object[]{Map.of("a", 1L)}, new Object[]{Map.of("b", 2L)}));
        } finally {
            conn.createStatement().execute("drop table test_nested_objects");
        }
    }

    /**
     * One instant, however it is expressed, is that instant in a
     * {@code timestamptz} column. Which of the forms below carries a zone and
     * which does not is the whole of the difference between them, so this is
     * where an offset would go astray if the driver added one.
     *
     * <p>A {@code LocalDateTime} carries none, and CrateDB reads a timestamp
     * that names no offset as UTC — not as the JVM's zone, which is the
     * assumption a local-zone PostgreSQL leaves behind. The suite runs in UTC,
     * so the values are built against a zone of their own rather than the
     * default one.</p>
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("theSameInstantExpressedFourWays")
    public void anInstantBindsToTheSameMomentHoweverItIsWritten(String description, Object value)
            throws Exception {
        conn.createStatement().executeUpdate("create table test_instants (moment timestamptz)");
        try (PreparedStatement insert =
                 conn.prepareStatement("insert into test_instants (moment) values (?)")) {
            insert.setObject(1, value);
            insert.executeUpdate();
            conn.createStatement().execute("refresh table test_instants");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select moment from test_instants");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getTimestamp(1).getTime(), is(INSTANT.toEpochMilli()));
            assertThat(resultSet.getObject(1, OffsetDateTime.class).toInstant(), is(INSTANT));
        } finally {
            conn.createStatement().execute("drop table test_instants");
        }
    }

    private static final Instant INSTANT = Instant.parse("2026-08-05T10:00:00Z");

    static Stream<Arguments> theSameInstantExpressedFourWays() {
        return Stream.of(
            Arguments.of("java.sql.Timestamp", Timestamp.from(INSTANT)),
            Arguments.of("Instant", INSTANT),
            Arguments.of("OffsetDateTime at +00", INSTANT.atOffset(ZoneOffset.UTC)),
            Arguments.of("OffsetDateTime at +02", INSTANT.atOffset(ZoneOffset.ofHours(2))),
            // No offset of its own, so the server reads it as UTC.
            Arguments.of("LocalDateTime", LocalDateTime.ofInstant(INSTANT, ZoneOffset.UTC))
        );
    }

    /**
     * A series of moments stores the instants it names, from wherever the JVM
     * stands and by whichever route it is written. pgJDBC has an array form
     * for {@link Timestamp} alone, and writes one out as a wall clock in the
     * JVM's own zone for the server to read as UTC, so a series is where an
     * application away from offset zero would otherwise store a moment it
     * never named.
     *
     * <p>What was stored has to be read back from the server as epoch
     * milliseconds: a conversion that shifts a moment on the way in and shifts
     * it back on the way out reads correctly through any round trip. The JVM
     * is stood in a zone of its own for the same reason, since at offset zero
     * a conversion that goes through the default calendar and one that does
     * not give the same answer.</p>
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("theSameInstantInSeveralZones")
    public void aSeriesOfMomentsStoresTheInstantsItNames(String description, String zone, Object moment)
            throws Exception {
        TimeZone stood = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            // Opened after the zone is set, so that nothing the connection
            // holds about one predates the change.
            try (Connection zoned = connect()) {
                zoned.createStatement().executeUpdate(
                    "create table test_moment_series (id int, moments array(timestamptz))");
                try (PreparedStatement insert = zoned.prepareStatement(
                         "insert into test_moment_series (id, moments) values (?, ?)")) {
                    insert.setInt(1, 1);
                    insert.setObject(2, Collections.singletonList(moment));
                    insert.execute();
                    insert.setInt(1, 2);
                    insert.setArray(2, zoned.createArrayOf("timestamptz", new Object[]{moment}));
                    insert.execute();
                }
                zoned.createStatement().execute("refresh table test_moment_series");

                ResultSet stored = zoned.createStatement().executeQuery(
                    "select moments[1]::bigint from test_moment_series order by id");
                assertThat(stored.next(), is(true));
                assertThat("bound as a series", stored.getLong(1), is(INSTANT.toEpochMilli()));
                assertThat(stored.next(), is(true));
                assertThat("built with createArrayOf", stored.getLong(1), is(INSTANT.toEpochMilli()));
            } finally {
                conn.createStatement().execute("drop table test_moment_series");
            }
        } finally {
            TimeZone.setDefault(stood);
        }
    }

    static Stream<Arguments> theSameInstantInSeveralZones() {
        // Zones either side of UTC, so a conversion leaning on the default
        // calendar lands on the wrong side of one of them whichever way it errs.
        return Stream.of("UTC", "Europe/Berlin", "America/Los_Angeles").flatMap(zone ->
            theSameInstantExpressedFourWays().map(moment -> {
                Object[] arguments = moment.get();
                return Arguments.of(arguments[0] + " in " + zone, zone, arguments[1]);
            }));
    }

    /**
     * A series of {@code Byte} binds as the {@code int2} array a CrateDB
     * {@code byte} column is. Java's narrowest box for a whole number is the
     * one width pgJDBC has no array form for, and the column holds the values
     * at the same width however a caller wrote them.
     */
    @Test
    public void aSeriesOfBytesBindsToAByteColumn() throws Exception {
        conn.createStatement().executeUpdate("create table test_byte_series (values_ array(byte))");
        try (PreparedStatement insert =
                 conn.prepareStatement("insert into test_byte_series (values_) values (?)")) {
            insert.setObject(1, List.of((byte) 120, (byte) 100));
            insert.execute();
            conn.createStatement().execute("refresh table test_byte_series");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select values_ from test_byte_series");
            assertThat(resultSet.next(), is(true));
            assertThat((Object[]) resultSet.getArray(1).getArray(),
                serverAtLeast(6, 5)
                    ? Matchers.<Object>arrayContaining((short) 120, (short) 100)
                    : Matchers.<Object>arrayContaining("120", "100"));
        } finally {
            conn.createStatement().execute("drop table test_byte_series");
        }
    }

    /**
     * A {@code timestamp} column holds wall-clock time: the same instant
     * written and read through calendars in different zones lands on
     * different points on the clock, exactly the offset between them apart.
     * A {@code timestamptz} column holds the instant itself, so the zone of
     * the calendar makes no difference to it.
     *
     * <p>Before CrateDB 6.4 a {@code timestamp} column arrives with a
     * {@code +00} offset appended, which pgJDBC reads as an instant and so
     * ignores the calendar for.</p>
     */
    @Test
    public void timestampsAreReadInTheCalendarsZone() throws Exception {
        assumeTrue(serverAtLeast(6, 4), "wall-clock timestamps need CrateDB 6.4");
        Calendar berlin = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Timestamp written = new Timestamp(1000L);

        conn.createStatement().executeUpdate(
            "create table test_zones (wall timestamp, instant timestamptz)");
        try (PreparedStatement insert =
                 conn.prepareStatement("insert into test_zones (wall, instant) values (?, ?)")) {
            insert.setTimestamp(1, written, berlin);
            insert.setTimestamp(2, written, berlin);
            insert.execute();
            conn.createStatement().execute("refresh table test_zones");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select wall, instant from test_zones");
            assertThat(resultSet.next(), is(true));

            assertThat(resultSet.getTimestamp("wall", berlin), is(written));
            assertThat(resultSet.getTimestamp("instant", berlin), is(written));
            assertThat(resultSet.getTimestamp("instant", utc), is(written));

            long offset = berlin.getTimeZone().getOffset(written.getTime());
            assertThat(resultSet.getTimestamp("wall", utc).getTime(), is(written.getTime() + offset));
        } finally {
            conn.createStatement().execute("drop table test_zones");
        }
    }

    /**
     * {@code createArrayOf} takes a CrateDB type name where PostgreSQL spells
     * the type differently, and any name at all the way SQL matches one —
     * however the caller capitalised it.
     */
    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("arrayElementTypeNames")
    public void createArrayOfTakesCrateDbTypeNames(String typeName, String pgTypeName) throws Exception {
        assertThat(conn.createArrayOf(typeName, new Object[0]).getBaseTypeName(), is(pgTypeName));
        assertThat(conn.createArrayOf(typeName.toUpperCase(Locale.ENGLISH), new Object[0]).getBaseTypeName(),
            is(pgTypeName));
    }

    static Stream<Arguments> arrayElementTypeNames() {
        return Stream.of(
            // Names CrateDB spells differently from pg_catalog.
            Arguments.of("string", "varchar"),
            Arguments.of("ip", "varchar"),
            Arguments.of("character", "bpchar"),
            Arguments.of("char", "bpchar"),
            Arguments.of("boolean", "bool"),
            Arguments.of("byte", "int2"),
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
            // Names the two spell alike, which reach the server as written.
            Arguments.of("text", "text"),
            Arguments.of("timestamp", "timestamp"),
            Arguments.of("timestamptz", "timestamptz"),
            Arguments.of("numeric", "numeric"),
            Arguments.of("date", "date"),
            Arguments.of("interval", "interval"),
            Arguments.of("uuid", "uuid"),
            Arguments.of("bit", "bit")
        );
    }

    @Test
    public void createArrayOfRejectsATypeTheServerDoesNotHave() {
        assertThrows(SQLException.class, () -> conn.createArrayOf("no_such_type", new Object[0]));
    }

    /**
     * A converted value still reports whether the column was null. The
     * conversions this driver adds read the value before answering, which is
     * where a wrapping driver classically loses the flag.
     */
    @Test
    public void wasNullReportsANullColumnAfterAConvertedRead() throws Exception {
        conn.createStatement().executeUpdate(
            "create table test_nulls (id integer primary key, obj object, texts array(text), label text)");
        try {
            conn.createStatement().executeUpdate(
                "insert into test_nulls (id, obj, texts, label) values (1, null, null, null),"
                + " (2, {a=1}, ['x'], 'set')");
            conn.createStatement().execute("refresh table test_nulls");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select obj, texts, label from test_nulls order by id");

            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject("obj"), is(nullValue()));
            assertThat(resultSet.wasNull(), is(true));
            assertThat(resultSet.getArray("texts"), is(nullValue()));
            assertThat(resultSet.wasNull(), is(true));
            assertThat(resultSet.getObject("texts"), is(nullValue()));
            assertThat(resultSet.wasNull(), is(true));

            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getObject("obj"), is(Map.of("a", 1L)));
            assertThat(resultSet.wasNull(), is(false));
            assertThat(resultSet.getArray("texts"), is(notNullValue()));
            assertThat(resultSet.wasNull(), is(false));
        } finally {
            conn.createStatement().execute("drop table test_nulls");
        }
    }

    /**
     * A series of values binds the same whether the caller holds a collection
     * or the array of the same values. An empty one carries no element to take
     * a type from, and is left for the server to type from the column.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("seriesParameters")
    public void aSeriesBindsFromACollectionAsFromAnArray(
            String description, Object value, List<?> expected) throws Exception {
        conn.createStatement().executeUpdate("create table test_series (texts array(text))");
        try (PreparedStatement insert =
                 conn.prepareStatement("insert into test_series (texts) values (?)")) {
            insert.setObject(1, value);
            insert.execute();
            conn.createStatement().execute("refresh table test_series");

            ResultSet resultSet = conn.createStatement().executeQuery("select texts from test_series");
            assertThat(resultSet.next(), is(true));
            assertThat(Arrays.asList((Object[]) resultSet.getArray(1).getArray()), is(expected));
        } finally {
            conn.createStatement().execute("drop table test_series");
        }
    }

    static Stream<Arguments> seriesParameters() {
        return Stream.of(
            Arguments.of("List", List.of("a", "b"), List.of("a", "b")),
            Arguments.of("Set", new LinkedHashSet<>(List.of("a", "b")), List.of("a", "b")),
            Arguments.of("String[]", new String[]{"a", "b"}, List.of("a", "b")),
            Arguments.of("List holding a null", Arrays.asList("a", null), Arrays.asList("a", null)),
            Arguments.of("empty List", List.of(), List.of()),
            Arguments.of("empty String[]", new String[0], List.of())
        );
    }

    /**
     * A series whose elements are boxed as different kinds of number binds as
     * the widest of them, which is the CrateDB array type they describe
     * between them. A series of nothing but nulls describes none, and is left
     * for the server to type from the column.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("mixedSeriesParameters")
    public void aSeriesOfMixedNumberBoxesBindsAsTheWidestOfThem(
            String description, String columnType, Object value, List<?> expected) throws Exception {
        conn.createStatement().executeUpdate(
            "create table test_mixed (values_ array(" + columnType + "))");
        try (PreparedStatement insert =
                 conn.prepareStatement("insert into test_mixed (values_) values (?)")) {
            insert.setObject(1, value);
            insert.execute();
            conn.createStatement().execute("refresh table test_mixed");

            ResultSet resultSet = conn.createStatement().executeQuery("select values_ from test_mixed");
            assertThat(resultSet.next(), is(true));
            assertThat(Arrays.asList((Object[]) resultSet.getArray(1).getArray()), is(expected));
        } finally {
            conn.createStatement().execute("drop table test_mixed");
        }
    }

    static Stream<Arguments> mixedSeriesParameters() {
        return Stream.of(
            Arguments.of("Integer with Long", "bigint", Arrays.asList(1, 2L), List.of(1L, 2L)),
            Arguments.of("Integer with Double", "double precision",
                Arrays.asList(1, 2.5d), List.of(1.0d, 2.5d)),
            Arguments.of("nothing but nulls", "text",
                Arrays.asList(null, null), Arrays.asList(null, null))
        );
    }

    /** A series that describes no CrateDB type is refused here, by name. */
    @Test
    public void aSeriesMixingUnrelatedTypesIsRefused() throws Exception {
        try (PreparedStatement select = conn.prepareStatement("select ?")) {
            SQLException raised = assertThrows(SQLException.class,
                () -> select.setObject(1, Arrays.asList(1, "a")));
            assertThat(raised.getMessage(), containsString("Integer"));
            assertThat(raised.getMessage(), containsString("String"));
        }
    }

    /**
     * JDBC reads a count of zero as the rest of the array, and both of this
     * driver's array implementations answer the same — the one pgJDBC backs
     * and the one built from json.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("arrayQueries")
    public void readingZeroElementsReadsTheRestOfTheArray(String description, String query)
            throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery(query);
        assertThat(resultSet.next(), is(true));

        Array array = resultSet.getArray(1);
        assertThat((Object[]) array.getArray(1, 0), is((Object[]) array.getArray()));
    }
}
