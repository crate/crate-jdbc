package io.crate.client.jdbc.integrationtests;

import io.crate.testing.CrateTestServer;
import org.hamcrest.Matchers;
import org.junit.*;
import org.postgresql.jdbc.PgResultSet;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class CrateJDBCTypesTest extends CrateJDBCIntegrationTest {

    private static Connection connection;
    private static String connectionStr;

    @BeforeClass
    public static void beforeClass() throws Exception {
        CrateTestServer server = testCluster.randomServer();
        connectionStr = String.format("crate://%s:%s/", server.crateHost(), server.psqlPort());
        connection = DriverManager.getConnection(connectionStr);
        setUpTables();
        insertIntoTable();
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
                " ip_field ip" +
                ") clustered by (id) into 1 shards with (number_of_replicas=0)");
        waitForShards();

        connection.createStatement().execute("create table if not exists arrayTest (" +
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

    private static void insertIntoTable() throws SQLException {
        Map<String, Object> objectField = new HashMap<String, Object>() {{
            put("inner", "Zoon");
        }};
        PreparedStatement preparedStatement =
                connection.prepareStatement("insert into test (id, string_field, boolean_field, byte_field, " +
                        "short_field, integer_field, long_field, float_field, double_field, object_field, " +
                        "timestamp_field, ip_field) values " +
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
        preparedStatement.setTimestamp(11, new Timestamp(1000L));
        preparedStatement.setString(12, "127.0.0.1");
        preparedStatement.execute();

        preparedStatement =
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

        connection.createStatement().execute("refresh table test");
        connection.createStatement().execute("refresh table arrayTest");
    }

    @Test
    public void testSelectStringType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select string_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString("string_field"), is("Youri"));
        }
    }

    @Test
    public void testSelectBooleanType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select boolean_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getBoolean("boolean_field"), is(true));
        }
    }

    @Test
    public void testSelectByteType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select byte_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getByte("byte_field"), is(new Byte("120")));
        }
    }

    @Test
    public void testSelectShortType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select short_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getShort("short_field"), is(new Short("1000")));
        }
    }

    @Test
    public void testSelectIntegerType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select integer_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt("integer_field"), is(1200000));
        }
    }

    @Test
    public void testSelectLongType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select long_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong("long_field"), is(120000000000L));
        }
    }

    @Test
    public void testSelectFloatType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select float_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getFloat("float_field"), is(1.4f));
        }
    }

    @Test
    public void testSelectDoubleType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select double_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getDouble("double_field"), is(3.456789d));
        }
    }

    @Test
    public void testSelectTimestampType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select timestamp_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getTimestamp("timestamp_field"), is(new Timestamp(1000L)));
        }
    }

    @Test
    public void testSelectIPType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select ip_field from test");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString("ip_field"), is("127.0.0.1"));
        }
    }

    @Test
    public void testSelectStringArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select str_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array strArray = resultSet.getArray("str_array");
            assertThat(strArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(strArray.getBaseType(), is(Types.VARCHAR));
            assertThat((Object[]) strArray.getArray(), Matchers.<Object>arrayContaining("a", "b", "c", "d"));
        }
    }

    @Test
    public void testSelectBooleanArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select bool_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array boolArray = resultSet.getArray("bool_array");
            assertThat(boolArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(boolArray.getBaseType(), is(Types.BOOLEAN));
            assertThat((Object[]) boolArray.getArray(), Matchers.<Object>arrayContaining(true, false));
        }
    }

    @Test
    public void testSelectByteArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select byte_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array byteArray = resultSet.getArray("byte_array");
            assertThat(byteArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(byteArray.getBaseType(), is(Types.TINYINT));
            assertThat((Object[]) byteArray.getArray(), Matchers.<Object>arrayContaining(new Byte("120"), new Byte("100")));
        }
    }

    @Test
    public void testSelectShortArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select short_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array shortArray = resultSet.getArray("short_array");
            assertThat(shortArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(shortArray.getBaseType(), is(Types.SMALLINT));
            assertThat((Object[]) shortArray.getArray(), Matchers.<Object>arrayContaining(1300, 1200));
        }
    }

    @Test
    public void testSelectIntegerArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select integer_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array integerArray = resultSet.getArray("integer_array");
            assertThat(integerArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(integerArray.getBaseType(), is(Types.INTEGER));
            assertThat((Object[]) integerArray.getArray(), Matchers.<Object>arrayContaining(2147483647, 234583));;
        }
    }

    @Test
    public void testSelectLongArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select long_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array longArray = resultSet.getArray("long_array");
            assertThat(longArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(longArray.getBaseType(), is(Types.BIGINT));
            assertThat((Object[]) longArray.getArray(), Matchers.<Object>arrayContaining(9223372036854775807L, 4L));
        }
    }

    @Test
    public void testSelectFloatArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select float_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array floatArray = resultSet.getArray("float_array");
            assertThat(floatArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(floatArray.getBaseType(), is(Types.REAL));
            assertThat((Object[]) floatArray.getArray(), Matchers.<Object>arrayContaining(3.402f, 3.403f, 1.4f));
        }
    }

    @Test
    public void testSelectDoubleArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select double_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array doubleArray = resultSet.getArray("double_array");
            assertThat(doubleArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(doubleArray.getBaseType(), is(Types.DOUBLE));
            assertThat((Object[]) doubleArray.getArray(), Matchers.<Object>arrayContaining(1.79769313486231570e+308, 1.69769313486231570e+308));
        }
    }

    @Test
    public void testSelectTimestampArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select timestamp_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array timestampArray = resultSet.getArray("timestamp_array");
            assertThat(timestampArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(timestampArray.getBaseType(), is(Types.TIMESTAMP));
            assertThat((Object[]) timestampArray.getArray(), Matchers.<Object>arrayContaining(new Timestamp(1000L), new Timestamp(2000L)));
        }
    }

    @Test
    public void testSelectIPArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select ip_array from arrayTest");
            assertThat(resultSet, instanceOf(PgResultSet.class));
            assertThat(resultSet.next(), is(true));

            Array ipArray = resultSet.getArray("ip_array");
            assertThat(ipArray.getArray().getClass().isArray(), is(true));
            Assert.assertThat(ipArray.getBaseType(), is(Types.VARCHAR));
            assertThat((Object[]) ipArray.getArray(), Matchers.<Object>arrayContaining("127.142.132.9", "127.0.0.1"));
        }
    }

    @Test
    public void testSelectObjectArrayType() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStr)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select obj_array from arrayTest");
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

            assertThat((Object[]) objArray.getArray(), Matchers.<Object>arrayContaining(mapArray));
        }
    }

}
