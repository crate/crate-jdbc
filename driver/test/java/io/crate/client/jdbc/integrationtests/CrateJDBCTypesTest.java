package io.crate.client.jdbc.integrationtests;

import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.jdbc.PgResultSet;
import org.postgresql.util.PGobject;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class CrateJDBCTypesTest extends CrateJDBCIntegrationTest {

    private static Connection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException, InterruptedException {
        connection = DriverManager.getConnection(getConnectionString());
        setUpTestTable();
        insertIntoTestTable();

        // set up the table with array datatypes only
        setUpArrayTable();
        insertIntoArrayTable();
    }

    private static void setUpArrayTable() throws SQLException, InterruptedException {
        connection.createStatement().execute(
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
            " obj_array array(object)" +
            ") clustered by (id) into 1 shards with (number_of_replicas=0)");
        ensureYellow();
    }

    private static void insertIntoArrayTable() throws SQLException {
        PreparedStatement preparedStatement =
            connection.prepareStatement("insert into arrayTest (id, str_array, bool_array, byte_array, " +
                                        "short_array, integer_array, long_array, float_array, double_array, timestamp_array, " +
                                        "ip_array, obj_array) values " +
                                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        preparedStatement.setInt(1, 1);
        preparedStatement.setArray(2, connection.createArrayOf("string", new String[]{"a", "b", "c", "d"}));
        preparedStatement.setArray(2, connection.createArrayOf("string", new String[]{"a", "b", "c", "d"}));
        preparedStatement.setArray(3, connection.createArrayOf("boolean", new Boolean[]{true, false}));
        preparedStatement.setArray(4, connection.createArrayOf("byte", new Byte[]{new Byte("120"), new Byte("100")}));
        preparedStatement.setArray(5, connection.createArrayOf("short", new Short[]{1300, 1200}));
        preparedStatement.setArray(6, connection.createArrayOf("integer", new Integer[]{2147483647, 234583}));
        preparedStatement.setArray(7, connection.createArrayOf("long", new Long[]{9223372036854775807L, 4L}));
        preparedStatement.setArray(8, connection.createArrayOf("float", new Float[]{3.402f, 3.403f, 1.4f}));
        preparedStatement.setArray(9, connection.createArrayOf("double", new Double[]{1.79769313486231570e+308, 1.69769313486231570e+308}));
        preparedStatement.setArray(10, connection.createArrayOf("timestamp", new Timestamp[]{new Timestamp(1000L), new Timestamp(2000L)}));
        preparedStatement.setArray(11, connection.createArrayOf("ip", new String[]{"127.142.132.9", "127.0.0.1"}));
        preparedStatement.setArray(12, connection.createArrayOf("object", new Object[]{
            new HashMap<String, Object>() {{
                put("element1", "testing");
            }}, new HashMap<String, Object>() {{
            put("element2", "testing2");
        }}
        }));
        preparedStatement.execute();
        connection.createStatement().execute("refresh table arrayTest");
    }

    @Test
    public void testSelectStringType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select string_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getString("string_field"), is("Youri"));
    }

    @Test
    public void testSelectBooleanType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select boolean_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getBoolean("boolean_field"), is(true));
    }

    @Test
    public void testSelectByteType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select byte_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getByte("byte_field"), is(new Byte("120")));
    }

    @Test
    public void testSelectShortType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select short_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getShort("short_field"), is(new Short("1000")));
    }

    @Test
    public void testSelectIntegerType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select integer_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getInt("integer_field"), is(1200000));
    }

    @Test
    public void testSelectLongType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select long_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getLong("long_field"), is(120000000000L));
    }

    @Test
    public void testSelectFloatType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select float_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getFloat("float_field"), is(1.4f));
    }

    @Test
    public void testSelectDoubleType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select double_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getDouble("double_field"), is(3.456789d));
    }

    @Test
    public void testSelectTimestampType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select timestamp_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getTimestamp("timestamp_field"), is(new Timestamp(1000L)));
    }

    @Test
    public void testSelectIPType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select ip_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getString("ip_field"), is("127.0.0.1"));
    }

    @Test
    public void testSelectGeoPoint() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select geo_point_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat((Double[]) resultSet.getArray("geo_point_field").getArray(), Matchers.arrayContaining(9.7419021d, 47.4048045d));
    }

    @Test
    public void testSelectGeoShape() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select geo_shape_field from test");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));
        assertThat((Map) resultSet.getObject("geo_shape_field"), Is.<Map>is(new HashMap<String, Object>(){{
            put("coordinates", Collections.singletonList(
                    Arrays.asList(
                            Arrays.asList(30.0, 10.0),
                            Arrays.asList(40.0, 40.0),
                            Arrays.asList(20.0, 40.0),
                            Arrays.asList(10.0, 20.0),
                            Arrays.asList(30.0, 10.0)
                    )
            ));
            put("type", "Polygon");
        }
        }));
        assertThat(resultSet.getObject("geo_shape_field", PGobject.class).getValue(),
                Matchers.allOf(
                        Matchers.containsString("\"type\":\"Polygon\""),
                        Matchers.containsString("\"coordinates\":[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]")
                )
        );
    }

    @Test
    public void testSelectStringArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select str_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array strArray = resultSet.getArray("str_array");
        assertThat(strArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(strArray.getBaseType(), is(Types.VARCHAR));
        assertThat((Object[]) strArray.getArray(), Matchers.<Object>arrayContaining("a", "b", "c", "d"));
    }

    @Test
    public void testSelectBooleanArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select bool_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array boolArray = resultSet.getArray("bool_array");
        assertThat(boolArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(boolArray.getBaseType(), is(Types.BOOLEAN));
        assertThat((Object[]) boolArray.getArray(), Matchers.<Object>arrayContaining(true, false));
    }

    @Test
    public void testSelectByteArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select byte_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array byteArray = resultSet.getArray("byte_array");
        assertThat(byteArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(byteArray.getBaseType(), is(Types.TINYINT));
        assertThat((Object[]) byteArray.getArray(), Matchers.<Object>arrayContaining(new Byte("120"), new Byte("100")));
    }

    @Test
    public void testSelectShortArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select short_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array shortArray = resultSet.getArray("short_array");
        assertThat(shortArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(shortArray.getBaseType(), is(Types.SMALLINT));
        assertThat((Object[]) shortArray.getArray(), Matchers.<Object>arrayContaining(1300, 1200));
    }

    @Test
    public void testSelectIntegerArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select integer_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array integerArray = resultSet.getArray("integer_array");
        assertThat(integerArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(integerArray.getBaseType(), is(Types.INTEGER));
        assertThat((Object[]) integerArray.getArray(), Matchers.<Object>arrayContaining(2147483647, 234583));
    }

    @Test
    public void testSelectLongArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select long_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array longArray = resultSet.getArray("long_array");
        assertThat(longArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(longArray.getBaseType(), is(Types.BIGINT));
        assertThat((Object[]) longArray.getArray(), Matchers.<Object>arrayContaining(9223372036854775807L, 4L));
    }

    @Test
    public void testSelectFloatArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select float_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array floatArray = resultSet.getArray("float_array");
        assertThat(floatArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(floatArray.getBaseType(), is(Types.REAL));
        assertThat((Object[]) floatArray.getArray(), Matchers.<Object>arrayContaining(3.402f, 3.403f, 1.4f));
    }

    @Test
    public void testSelectDoubleArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select double_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array doubleArray = resultSet.getArray("double_array");
        assertThat(doubleArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(doubleArray.getBaseType(), is(Types.DOUBLE));
        assertThat((Object[]) doubleArray.getArray(), Matchers.<Object>arrayContaining(1.79769313486231570e+308, 1.69769313486231570e+308));
    }

    @Test
    public void testSelectTimestampArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select timestamp_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array timestampArray = resultSet.getArray("timestamp_array");
        assertThat(timestampArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(timestampArray.getBaseType(), is(Types.TIMESTAMP));
        assertThat((Object[]) timestampArray.getArray(), Matchers.<Object>arrayContaining(new Timestamp(1000L), new Timestamp(2000L)));
    }

    @Test
    public void testSelectIPArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select ip_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array ipArray = resultSet.getArray("ip_array");
        assertThat(ipArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(ipArray.getBaseType(), is(Types.VARCHAR));
        assertThat((Object[]) ipArray.getArray(), Matchers.<Object>arrayContaining("127.142.132.9", "127.0.0.1"));
    }

    @Test
    public void testSelectObjectArrayType() throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("select obj_array from arrayTest");
        assertThat(resultSet, instanceOf(PgResultSet.class));
        assertThat(resultSet.next(), is(true));

        Array objArray = resultSet.getArray("obj_array");
        assertThat(objArray.getArray().getClass().isArray(), is(true));
        Assert.assertThat(objArray.getBaseType(), is(Types.JAVA_OBJECT));
        Object firstObject = ((Object[]) objArray.getArray())[0];
        Assert.assertThat(firstObject, instanceOf(Map.class));

        Object[] mapArray = new Object[]{
            new HashMap<String, Object>() {{
                put("element1", "testing");
            }}, new HashMap<String, Object>() {{
            put("element2", "testing2");
        }}
        };

        assertThat((Object[]) objArray.getArray(), Matchers.arrayContaining(mapArray));
    }

    @Test
    public void testSetGetObject() throws SQLException {
        Map<String, Integer> expected = new HashMap<>();
        expected.put("n", 1);

        connection.createStatement().executeUpdate("create table test_obj (obj object as (n int))");
        PreparedStatement statement = connection.prepareStatement("insert into test_obj (obj) values (?)");
        statement.setObject(1, expected);
        statement.execute();

        connection.createStatement().execute("refresh table test_obj");
        ResultSet resultSet = connection.createStatement().executeQuery("select obj from test_obj");
        assertThat(resultSet.next(), is(true));
        connection.createStatement().execute("drop table test_obj");

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) resultSet.getObject(1);
        assertEquals(expected, map);
    }
}
