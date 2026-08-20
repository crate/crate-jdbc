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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.JDBCType;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

/**
 * Pins how CrateDB data types surface through the crate:// driver: scalar
 * types read back as their natural Java classes, OBJECT columns and OBJECT
 * array elements read back as {@code Map}, and
 * {@link Connection#createArrayOf} accepts CrateDB type names.
 */
@Tag("pgjdbc-types")
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
            " integer_array array(integer)," +
            " obj_array array(object)" +
            ") clustered by (id) into 1 shards with (number_of_replicas=0)");
        ensureYellow();
    }

    private static void insertIntoArrayTable() throws SQLException {
        PreparedStatement preparedStatement =
            conn.prepareStatement("insert into arrayTest (id, str_array, integer_array, obj_array)"
                                  + " values (?, ?, ?, ?)");
        Map<String, Object> firstObj = new HashMap<>();
        firstObj.put("element1", "testing");
        Map<String, Object> secondObj = new HashMap<>();
        secondObj.put("element2", "testing2");

        preparedStatement.setInt(1, 1);
        preparedStatement.setArray(2, conn.createArrayOf("string", new String[]{"a", "b", "c", "d"}));
        preparedStatement.setArray(3, conn.createArrayOf("integer", new Integer[]{2147483647, 234583}));
        preparedStatement.setArray(4, conn.createArrayOf("object", new Object[]{firstObj, secondObj}));
        preparedStatement.execute();
        conn.createStatement().execute("refresh table arrayTest");
    }

    /**
     * {@code docs/data-types.rst} publishes, for every CrateDB type, the JDBC
     * type a column of it is reported as and the {@link ResultSet} method that
     * reads it. An application picks its getter off that table, so this holds
     * the table against a server. Where the value read is pgJDBC's own
     * rendering of what arrived, the class it comes back as is all the table
     * promises and all that is pinned.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("thePublishedTypeTable")
    public void everyPublishedTypeReadsAsTheTableSaysItDoes(
            String query, int jdbcType, ColumnReader reader, Matcher<Object> value) throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery(query);
        assertThat(resultSet.next(), is(true));

        assertThat(resultSet.getMetaData().getColumnType(1), is(jdbcType));
        assertThat(reader.read(resultSet, 1), value);
    }

    static Stream<Arguments> thePublishedTypeTable() {
        Stream<Arguments> everyServer = Stream.of(
            // A CrateDB boolean travels as the PostgreSQL bool, which pgJDBC
            // describes as BIT rather than as BOOLEAN.
            row("select boolean_field from test", Types.BIT, ResultSet::getBoolean, is(true)),
            // A byte is the PostgreSQL "char" up to CrateDB 6.4 and a small
            // integer from 6.5. getByte reads it at either width, which is what
            // makes the wire type moving underneath it a difference and not a
            // break.
            row("select byte_field from test", serverAtLeast(6, 5) ? Types.SMALLINT : Types.CHAR,
                ResultSet::getByte, is((byte) 120)),
            row("select short_field from test", Types.SMALLINT, ResultSet::getShort, is((short) 1000)),
            row("select integer_field from test", Types.INTEGER, ResultSet::getInt, is(1200000)),
            row("select long_field from test", Types.BIGINT, ResultSet::getLong, is(120000000000L)),
            row("select float_field from test", Types.REAL, ResultSet::getFloat, is(1.4f)),
            row("select double_field from test", Types.DOUBLE, ResultSet::getDouble, is(3.456789d)),
            row("select 1.25::numeric(10,2)", Types.NUMERIC, ResultSet::getBigDecimal,
                is(new BigDecimal("1.25"))),
            row("select string_field from test", Types.VARCHAR, ResultSet::getString, is("Youri")),
            row("select ip_field from test", Types.VARCHAR, ResultSet::getString, is("127.0.0.1")),
            row("select timestamp_field from test", Types.TIMESTAMP, ResultSet::getTimestamp,
                is(new Timestamp(1000L))),
            row("select '2026-08-05T10:00:00Z'::timestamptz", Types.TIMESTAMP,
                ResultSet::getTimestamp, is(Timestamp.from(INSTANT))),
            row("select '2024-01-02'::date", Types.DATE, ResultSet::getDate,
                is(Date.valueOf("2024-01-02"))),
            row("select '12:34:56+01:00'::time with time zone", Types.TIME, ResultSet::getTime,
                instanceOf(Time.class)),
            row("select '1 day 2 hours'::interval", Types.OTHER, ResultSet::getObject,
                instanceOf(PGInterval.class)),
            row("select B'1010'", Types.BIT, ResultSet::getObject, instanceOf(PGobject.class)),
            row("select object_field from test", Types.OTHER, ResultSet::getObject,
                is(Collections.singletonMap("inner", "Zoon"))),
            row("select geo_point_field from test", Types.OTHER, ResultSet::getObject,
                instanceOf(PGpoint.class)),
            row("select geo_shape_field from test", Types.OTHER, ResultSet::getObject,
                instanceOf(Map.class)),
            row("select str_array from arrayTest", Types.ARRAY, ResultSet::getArray,
                instanceOf(Array.class)),
            row("select [[1, 2], [3]]", Types.OTHER, ResultSet::getArray, instanceOf(Array.class)));
        // CrateDB gained the uuid type in 6.2; before it there is none to read.
        return serverAtLeast(6, 2)
            ? Stream.concat(everyServer, Stream.of(
                row("select '55d07626-4927-47c5-ba43-a015c23632ef'::uuid", Types.OTHER,
                    ResultSet::getObject,
                    is(UUID.fromString("55d07626-4927-47c5-ba43-a015c23632ef")))))
            : everyServer;
    }

    @SuppressWarnings("unchecked")
    private static Arguments row(String query, int jdbcType, ColumnReader reader, Matcher<?> value) {
        return Arguments.of(query, jdbcType, reader, (Matcher<Object>) value);
    }

    @FunctionalInterface
    interface ColumnReader {
        Object read(ResultSet resultSet, int column) throws SQLException;
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
     * The conversion a value goes through on the way in does not depend on
     * which {@code setObject} a caller reached for. JDBC defines five, and a
     * framework binding through the ones that name a target type, as
     * most of them — would otherwise be handing the server a value this driver
     * never converted.
     *
     * <p>The target type named is deliberately the one for a value pgJDBC has
     * no mapping of its own for, since that is what a caller with a {@code Map}
     * in hand would name.</p>
     *
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("setObjectForms")
    public void everySetObjectFormConvertsTheValueAlike(String form, ParameterBinder binder)
            throws Exception {
        Map<String, Long> object = Collections.singletonMap("n", 1L);
        List<List<Long>> nested = List.of(List.of(1L, 2L), List.of(3L));

        conn.createStatement().executeUpdate(
            "create table test_set_object (id integer, obj object as (n integer), nested array(array(bigint)))");
        try (PreparedStatement statement = conn.prepareStatement(
                "insert into test_set_object (id, obj, nested) values (?, ?, ?)")) {
            statement.setInt(1, 1);
            binder.bind(statement, 2, object);
            binder.bind(statement, 3, nested);
            statement.execute();
            conn.createStatement().execute("refresh table test_set_object");

            ResultSet resultSet = conn.createStatement()
                .executeQuery("select obj, nested from test_set_object");
            assertThat(resultSet.next(), is(true));
            assertThat(form, resultSet.getObject(1), is(object));
            assertThat(form, resultSet.getObject(2), is(nested));
        } finally {
            conn.createStatement().execute("drop table test_set_object");
        }
    }

    /**
     * Each of those forms is two paths, this driver either converting the
     * value or handing it to pgJDBC, and only the first is the driver's own.
     * Handed a value it does not convert, the two forms that name the target
     * type as a {@link java.sql.SQLType} raise
     * {@link SQLFeatureNotSupportedException}: pgJDBC has not implemented
     * them, and reaches them only for a value this driver passes on.
     *
     * <p>So the {@code SQLType} forms serve a caller binding an OBJECT or a
     * nested array alone, the reverse of what a caller would expect from the
     * form that names the plainest types.</p>
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("setObjectFormsAndWhatTheyForwardTo")
    public void aValueTheDriverPassesOnNeedsAFormPgJdbcImplements(
            String form, ParameterBinder binder, boolean implemented) throws Exception {
        String text = "bound by pgJDBC";
        conn.createStatement().executeUpdate("create table test_forwarded (txt text)");
        try (PreparedStatement statement = conn.prepareStatement(
                "insert into test_forwarded (txt) values (?)")) {
            if (!implemented) {
                assertThrows(SQLFeatureNotSupportedException.class,
                    () -> binder.bind(statement, 1, text));
                return;
            }
            binder.bind(statement, 1, text);
            statement.execute();
            conn.createStatement().execute("refresh table test_forwarded");

            ResultSet resultSet = conn.createStatement().executeQuery("select txt from test_forwarded");
            assertThat(resultSet.next(), is(true));
            assertThat(form, resultSet.getString(1), is(text));
        } finally {
            conn.createStatement().execute("drop table test_forwarded");
        }
    }

    /**
     * The same forms, each with whether pgJDBC implements the call this driver
     * forwards a value to. Derived rather than listed again, so that a form
     * added below is asked about here too.
     */
    static Stream<Arguments> setObjectFormsAndWhatTheyForwardTo() {
        Set<String> notImplemented = Set.of(
            "setObject(index, value, SQLType)", "setObject(index, value, SQLType, scale)");
        return setObjectForms().map(form -> Arguments.of(
            form.get()[0], form.get()[1], !notImplemented.contains(form.get()[0])));
    }

    static Stream<Arguments> setObjectForms() {
        return Stream.of(
            Arguments.of("setObject(index, value)",
                binder(PreparedStatement::setObject)),
            Arguments.of("setObject(index, value, int)",
                binder((s, i, v) -> s.setObject(i, v, Types.OTHER))),
            Arguments.of("setObject(index, value, int, scale)",
                binder((s, i, v) -> s.setObject(i, v, Types.OTHER, 0))),
            Arguments.of("setObject(index, value, SQLType)",
                binder((s, i, v) -> s.setObject(i, v, JDBCType.OTHER))),
            Arguments.of("setObject(index, value, SQLType, scale)",
                binder((s, i, v) -> s.setObject(i, v, JDBCType.OTHER, 0)))
        );
    }

    @FunctionalInterface
    interface ParameterBinder {
        void bind(PreparedStatement statement, int index, Object value) throws SQLException;
    }

    private static ParameterBinder binder(ParameterBinder binder) {
        return binder;
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

    /**
     * A json column read into the type the caller asked for, where the driver
     * has a conversion to it. Where it has none the column is pgJDBC's to
     * answer for, and what arrives is the value as it came off the wire.
     */
    @Test
    public void objectColumnsReadIntoTheRequestedMapType() throws Exception {
        ResultSet resultSet = conn.createStatement().executeQuery(
            "select object_field, geo_shape_field from test");
        assertThat(resultSet.next(), is(true));

        assertThat(resultSet.getObject(1, Map.class).get("inner"), is("Zoon"));
        assertThat(resultSet.getObject(1, HashMap.class).get("inner"), is("Zoon"));
        assertThat(resultSet.getObject(1, Object.class), is(instanceOf(Map.class)));

        assertThat(resultSet.getObject("geo_shape_field", PGobject.class).getValue(),
            containsString("\"type\":\"Polygon\""));
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

        // The statement describes the rows it would produce in the same terms
        // the rows do, which is what a mapper reads before running anything.
        try (PreparedStatement prepared = conn.prepareStatement(query)) {
            assertThat(prepared.getMetaData().getColumnClassName(1), is(describedClass));
        }
    }

    static Stream<Arguments> jsonColumnQueries() {
        return Stream.of(
            Arguments.of("object", "select object_field from test"),
            Arguments.of("array(array(integer))", "select [[1, 2], [3]]")
        );
    }

    /**
     * The PostgreSQL array format cannot hold sub-arrays of differing length,
     * so a column of {@code array(array(...))} travels as json. It reads back
     * through every way of asking for an array or for the value itself.
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
     * A nested column declared as a timestamp takes one, and that is where the
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
     * written from, so it reads back as a {@code Long}. One too large for a
     * {@code bigint} is a {@code numeric}, which CrateDB stores to 38 digits
     * and which reads back as the {@code BigInteger} holding it.
     */
    @Test
    public void wholeNumbersInAnObjectReadBackAsLong() throws Exception {
        BigInteger past = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        Map<String, Object> value = new HashMap<>();
        value.put("small", 5);
        value.put("large", 5_000_000_000L);
        value.put("past", past);

        try (PreparedStatement select = conn.prepareStatement("select ?::object")) {
            select.setObject(1, value);
            ResultSet resultSet = select.executeQuery();
            assertThat(resultSet.next(), is(true));

            Map<?, ?> back = (Map<?, ?>) resultSet.getObject(1);
            assertThat(back.get("small"), is(5L));
            assertThat(back.get("large"), is(5_000_000_000L));
            assertThat(back.get("past"), is(past));
        }
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
     * that names no offset as UTC, not as the JVM's zone, the
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
     * conversions this driver adds read the value before answering, and that is
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
     * the widest of them, the CrateDB array type they describe
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

    /**
     * A series that describes no CrateDB type is refused here, by name. A
     * series of OBJECT values is its own case: one element being a
     * {@code Map} settles what the whole series is, so a plain value beside
     * it is refused rather than half-converted.
     */
    @Test
    public void aSeriesMixingUnrelatedTypesIsRefused() throws Exception {
        try (PreparedStatement select = conn.prepareStatement("select ?")) {
            SQLException raised = assertThrows(SQLException.class,
                () -> select.setObject(1, Arrays.asList(1, "a")));
            assertThat(raised.getMessage(), containsString("Integer"));
            assertThat(raised.getMessage(), containsString("String"));

            assertThrows(SQLException.class,
                () -> select.setObject(1, List.of(Map.of("a", 1), "not an object")));
        }
    }

}
