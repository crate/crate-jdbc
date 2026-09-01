package io.crate.client.jdbc.integrationtests;

import org.postgresql.PGConnection;
import org.postgresql.PGResultSetMetaData;
import org.postgresql.PGStatement;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Executor;

/**
 * The JDBC surface the driver has to answer: every method of every interface
 * an application reaches, paired with arguments to call it with.
 *
 * <p>{@link io.crate.client.jdbc.WrapperCompletenessTest} walks the same
 * interfaces asking whether a method is overridden. This walk asks what it
 * <em>does</em>, so that an oracle can be computed over the answer instead of
 * being written out by hand for each of several hundred methods.</p>
 *
 * <p>Arguments come from the parameter types, with a table of exceptions for
 * the methods where a type says too little — an index means a column, a string
 * means a SQL statement or a catalog pattern. A merely plausible argument is
 * enough: an oracle that compares two drivers, or a driver against itself, is
 * answered as well by a rejection as by a value, as long as both sides are
 * asked the same question.</p>
 */
final class JdbcSurface {

    /**
     * The table the fixtures query. Its columns cover the shapes the driver
     * converts, and its rows cover the cases a single value cannot: nothing
     * stored at all, and enough rows that a fetch size smaller than the result
     * makes a cursor rather than a formality.
     *
     * <p>Columns are appended, never reordered or removed, and row 1 keeps the
     * values it has. Every entry in the delta was written against an answer
     * this table produced, so widening it can only add differences — reordering
     * or rewriting would quietly change what a listed call is being held to.</p>
     */
    static final String PROBE_TABLE = "differential_probe";

    static final String CREATE_PROBE_TABLE =
        "create table if not exists " + PROBE_TABLE + " (" +
        " id integer primary key," +
        " name string," +
        " amount double precision," +
        " tags array(string)," +
        " details object as (note string, count integer)," +
        " flag boolean," +
        " count_ bigint," +
        " ratio real," +
        " stamp timestamp with time zone," +
        " address ip," +
        " exact_ numeric(10, 2)," +
        " numbers array(integer)," +
        " matrix array(array(integer))," +
        " nothing text" +
        ") clustered into 1 shards with (number_of_replicas = 0)";

    private static final String PROBE_COLUMNS =
        "id, name, amount, tags, details, flag, count_, ratio, stamp, address, exact_, "
        + "numbers, matrix, nothing";

    /**
     * The columns of {@link #PROBE_SQL}, in the order the query returns them,
     * so that a method taking a column index can be asked about each of them.
     */
    static final List<String> PROBE_COLUMN_NAMES = List.of(PROBE_COLUMNS.split(",\\s*"));

    /**
     * The rows: the one every listed answer was written against, one holding
     * nothing but its key, and three at the edges of what the columns take.
     */
    static final List<String> INSERT_PROBE_ROWS = List.of(
        "insert into " + PROBE_TABLE + " (id, name, amount, tags, details) values "
        + "(1, 'first', 2.5, ['a', 'b'], {note = 'note', count = 3})",
        "insert into " + PROBE_TABLE + " (id) values (2)",
        "insert into " + PROBE_TABLE + " (" + PROBE_COLUMNS + ") values "
        + "(3, '', 0.0, [], {note = '', count = 0}, false, 0, 0.0, 0, '0.0.0.0', 0.00, [], [], null)",
        "insert into " + PROBE_TABLE + " (" + PROBE_COLUMNS + ") values "
        + "(4, 'héllo·日本', 1.7976931348623157e308, ['', null], {note = null, count = null}, "
        + "true, 9007199254740993, 3.4028235e38, 253402300799000, '255.255.255.255', 99999999.99, "
        + "[null, 2147483647], [[1], []], null)",
        "insert into " + PROBE_TABLE + " (" + PROBE_COLUMNS + ") values "
        + "(5, 'last', -1.0, ['z'], {note = 'z', count = -1}, false, -9007199254740993, -1.5, "
        + "-2208988800000, '::1', -99999999.99, [-2147483648], [[]], null)");

    static final String PROBE_SQL =
        "select " + PROBE_COLUMNS + " from " + PROBE_TABLE + " order by id";

    /** A column of {@link #PROBE_SQL}, for the methods that take a column label. */
    static final String PROBE_COLUMN = "name";

    /** The array-valued column, which the {@link Array} fixture is read from. */
    static final String PROBE_ARRAY_COLUMN = "tags";

    /**
     * The interfaces an application holds. The eight from {@code java.sql} are
     * the driver's own contract; the three from pgJDBC come with building on
     * it, and an application written against pgJDBC will reach for them.
     */
    static final List<Class<?>> INTERFACES = List.of(
        Connection.class, Statement.class, PreparedStatement.class, CallableStatement.class,
        ResultSet.class, ResultSetMetaData.class, DatabaseMetaData.class, Array.class,
        PGConnection.class, PGStatement.class, PGResultSetMetaData.class);

    /**
     * Parameter types with no value this harness can produce: a driver hands
     * them out rather than taking them from a caller, and CrateDB supports
     * none of the features that would produce one. A method taking one is
     * counted as unreached rather than called with a null that would measure
     * only null handling.
     */
    private static final Set<Class<?>> UNPRODUCIBLE = Set.of(
        Blob.class, Clob.class, NClob.class, SQLXML.class,
        Ref.class, RowId.class, Savepoint.class, Struct.class);

    private JdbcSurface() {
    }

    /**
     * Every method of {@link #INTERFACES} that can be called, in a stable
     * order, each paired with the arguments to call it with and the state to
     * call it in.
     */
    static List<Invocation> invocations() {
        List<Invocation> invocations = new ArrayList<>();
        for (Posture posture : Posture.values()) {
            for (Class<?> iface : INTERFACES) {
                if (!posture.appliesTo(iface)) {
                    continue;
                }
                for (Method method : callableMethods(iface)) {
                    Object[] arguments = arguments(iface, method);
                    if (arguments == null) {
                        continue;
                    }
                    invocations.add(new Invocation(iface, method, arguments, posture));
                    if (posture == Posture.FRESH) {
                        acrossTheColumns(invocations, iface, method, arguments);
                    }
                }
            }
        }
        invocations.sort(Comparator.comparing(Invocation::id));
        return invocations;
    }

    /**
     * The same reading call, pointed at each of the other probe columns.
     *
     * <p>A method taking a column index is otherwise only ever asked about the
     * first, which holds an integer — so the sweep asks thirty-odd getters what
     * they make of an integer, thirty-odd times, and never asks any of them
     * what they make of a timestamp, a json object or an array. Reading one
     * type through the getter for another is where a driver layered over
     * another driver's conversions has somewhere to go wrong, and it is the
     * whole of what {@code CrateResultSet} and the json gate in
     * {@code CrateResultSetMetaData} do.</p>
     *
     * <p>Only on an object that is open: a closed result set refuses whichever
     * column it was asked about, and one walked past its last row has no row to
     * read from either, so pointing those at fourteen columns would say one
     * thing fourteen times.</p>
     */
    private static void acrossTheColumns(List<Invocation> invocations, Class<?> iface,
                                         Method method, Object[] arguments) {
        if (iface != ResultSet.class && iface != ResultSetMetaData.class) {
            return;
        }
        String called = method.getName();
        if (!called.startsWith("get") && !called.startsWith("is")) {
            return;
        }
        int index = firstColumnIndex(method, arguments);
        if (index < 0) {
            return;
        }
        for (int column = 2; column <= PROBE_COLUMN_NAMES.size(); column++) {
            Object[] pointed = arguments.clone();
            pointed[index] = column;
            invocations.add(new Invocation(iface, method, pointed, Posture.FRESH,
                PROBE_COLUMN_NAMES.get(column - 1)));
        }
    }

    /**
     * Where the column index sits in a call's arguments, or -1 where it takes
     * none. The first {@code int} is the column; a second — the scale
     * {@code getBigDecimal} once took — is something else.
     */
    private static int firstColumnIndex(Method method, Object[] arguments) {
        Class<?>[] parameters = method.getParameterTypes();
        for (int at = 0; at < parameters.length; at++) {
            if (parameters[at] == int.class) {
                return arguments[at] instanceof Integer && (Integer) arguments[at] == 1 ? at : -1;
            }
        }
        return -1;
    }

    /** The methods left out of the sweep for want of an argument to pass. */
    static List<String> unreachable() {
        List<String> unreachable = new ArrayList<>();
        for (Class<?> iface : INTERFACES) {
            for (Method method : callableMethods(iface)) {
                if (arguments(iface, method) == null) {
                    unreachable.add(Invocation.id(iface, method, Posture.FRESH));
                }
            }
        }
        unreachable.sort(Comparator.naturalOrder());
        return unreachable;
    }

    private static List<Method> callableMethods(Class<?> iface) {
        List<Method> methods = new ArrayList<>();
        for (Method method : iface.getMethods()) {
            if (method.isSynthetic() || method.isBridge() || Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            methods.add(method);
        }
        return methods;
    }

    /**
     * Arguments for one method, or null when one of its parameters has no
     * value the harness can produce.
     */
    private static Object[] arguments(Class<?> iface, Method method) {
        Class<?>[] parameters = method.getParameterTypes();
        for (Class<?> parameter : parameters) {
            if (UNPRODUCIBLE.contains(parameter)) {
                return null;
            }
        }
        Object[] listed = OVERRIDES.get(key(method));
        if (listed != null && fits(listed, parameters)) {
            return listed;
        }
        Object[] arguments = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            arguments[i] = byType(iface, method, i, parameters[i]);
        }
        return arguments;
    }

    /**
     * Whether a listed argument vector can be passed to a method. Overloads
     * share a name and an arity — {@code getArray(int)} and
     * {@code getArray(String)} — so a listing written for one of them falls
     * through to the type defaults for the others rather than being rejected
     * by the call.
     */
    private static boolean fits(Object[] arguments, Class<?>[] parameters) {
        for (int i = 0; i < parameters.length; i++) {
            Object argument = arguments[i];
            Class<?> parameter = parameters[i];
            if (argument == null) {
                if (parameter.isPrimitive()) {
                    return false;
                }
            } else if (!boxed(parameter).isInstance(argument)) {
                return false;
            }
        }
        return true;
    }

    private static Class<?> boxed(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        if (type == int.class) {
            return Integer.class;
        }
        if (type == long.class) {
            return Long.class;
        }
        if (type == short.class) {
            return Short.class;
        }
        if (type == byte.class) {
            return Byte.class;
        }
        if (type == float.class) {
            return Float.class;
        }
        if (type == double.class) {
            return Double.class;
        }
        if (type == boolean.class) {
            return Boolean.class;
        }
        return Character.class;
    }

    /**
     * A value of the given parameter type. A string is the one type whose
     * meaning depends on where it sits: leading a statement method it is SQL,
     * leading a row method it is a column label, and everywhere else a schema
     * name is the least surprising thing to hand a driver.
     */
    private static Object byType(Class<?> iface, Method method, int position, Class<?> type) {
        if (type == String.class) {
            if (position == 0 && TAKES_SQL_FIRST.contains(method.getName())) {
                return PROBE_SQL;
            }
            if (position == 0 && (iface == ResultSet.class || iface == CallableStatement.class)) {
                return PROBE_COLUMN;
            }
            return "doc";
        }
        if (type == int.class) {
            return 1;
        }
        if (type == long.class) {
            return 1L;
        }
        if (type == short.class) {
            return (short) 1;
        }
        if (type == byte.class) {
            return (byte) 1;
        }
        if (type == float.class) {
            return 1.0f;
        }
        if (type == double.class) {
            return 1.0d;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == Object.class) {
            return "probe";
        }
        if (type == Object[].class) {
            return new Object[]{"probe"};
        }
        if (type == String[].class) {
            return new String[]{"TABLE"};
        }
        if (type == int[].class) {
            return new int[]{1};
        }
        if (type == byte[].class) {
            return new byte[]{1, 2, 3};
        }
        if (type == char[].class) {
            return new char[]{'x'};
        }
        if (type == BigDecimal.class) {
            return BigDecimal.ONE;
        }
        if (type == Date.class) {
            return new Date(0L);
        }
        if (type == Time.class) {
            return new Time(0L);
        }
        if (type == Timestamp.class) {
            return new Timestamp(0L);
        }
        if (type == Calendar.class) {
            return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        }
        if (type == Class.class) {
            return String.class;
        }
        if (type == SQLType.class) {
            return JDBCType.VARCHAR;
        }
        if (type == Map.class) {
            return new HashMap<String, Class<?>>();
        }
        if (type == Properties.class) {
            return new Properties();
        }
        if (type == Executor.class) {
            return (Executor) Runnable::run;
        }
        if (type == Reader.class) {
            return Placeholder.READER;
        }
        if (type == InputStream.class) {
            return Placeholder.INPUT_STREAM;
        }
        if (type == Array.class) {
            return Placeholder.ARRAY;
        }
        if (type == URL.class) {
            return url();
        }
        if (type.isEnum()) {
            return type.getEnumConstants()[0];
        }
        // An interface the harness holds no instance of. What a driver makes
        // of a null is its own answer, and both sides are asked the same.
        return null;
    }

    private static URL url() {
        try {
            return new URL("http://localhost/probe");
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A value the fixture builds, because it belongs to the connection it is
     * used on: each driver binds its own {@link Array}, and a stream is spent
     * by the call that reads it.
     */
    enum Placeholder {
        READER,
        INPUT_STREAM,
        ARRAY;

        Object resolve(Connection connection) throws SQLException {
            switch (this) {
                case READER:
                    return new StringReader("probe");
                case INPUT_STREAM:
                    return new ByteArrayInputStream("probe".getBytes(StandardCharsets.UTF_8));
                case ARRAY:
                    return connection.createArrayOf("text", new String[]{"a", "b"});
                default:
                    throw new IllegalStateException(name());
            }
        }
    }

    /** Methods whose first parameter is a SQL statement rather than a name. */
    private static final Set<String> TAKES_SQL_FIRST = new HashSet<>(List.of(
        "execute", "executeQuery", "executeUpdate", "executeLargeUpdate", "addBatch",
        "prepareStatement", "prepareCall", "nativeSQL"));

    /**
     * The methods whose parameter types say too little. Catalog queries want a
     * table that exists, the result-set methods want the constants that name a
     * kind of cursor, and {@code supportsConvert} wants two type codes.
     *
     * <p>Keyed by the interface that declares the method, so that a listing
     * for {@code Statement} also answers when the sweep reaches the same
     * method through {@code CallableStatement}.</p>
     */
    private static final Map<String, Object[]> OVERRIDES = new HashMap<>();

    private static String key(Method method) {
        return method.getDeclaringClass().getSimpleName()
            + "." + method.getName() + "/" + method.getParameterCount();
    }

    private static void listed(Class<?> declaringInterface, String method, Object... arguments) {
        OVERRIDES.put(declaringInterface.getSimpleName() + "." + method + "/" + arguments.length, arguments);
    }

    static {
        String schema = "doc";
        String table = PROBE_TABLE;

        listed(DatabaseMetaData.class, "getTables", null, schema, table, null);
        listed(DatabaseMetaData.class, "getColumns", null, schema, table, "%");
        listed(DatabaseMetaData.class, "getColumnPrivileges", null, schema, table, "%");
        listed(DatabaseMetaData.class, "getTablePrivileges", null, schema, table);
        listed(DatabaseMetaData.class, "getPrimaryKeys", null, schema, table);
        listed(DatabaseMetaData.class, "getImportedKeys", null, schema, table);
        listed(DatabaseMetaData.class, "getExportedKeys", null, schema, table);
        listed(DatabaseMetaData.class, "getVersionColumns", null, schema, table);
        listed(DatabaseMetaData.class, "getPseudoColumns", null, schema, table, "%");
        listed(DatabaseMetaData.class, "getSuperTables", null, schema, table);
        listed(DatabaseMetaData.class, "getSuperTypes", null, schema, "%");
        listed(DatabaseMetaData.class, "getAttributes", null, schema, "%", "%");
        listed(DatabaseMetaData.class, "getUDTs", null, schema, "%", null);
        listed(DatabaseMetaData.class, "getSchemas", null, schema);
        listed(DatabaseMetaData.class, "getProcedures", null, schema, "%");
        listed(DatabaseMetaData.class, "getProcedureColumns", null, schema, "%", "%");
        listed(DatabaseMetaData.class, "getFunctions", null, "pg_catalog", "abs");
        listed(DatabaseMetaData.class, "getFunctionColumns", null, "pg_catalog", "abs", "%");
        listed(DatabaseMetaData.class, "getCrossReference", null, schema, table, null, schema, table);
        listed(DatabaseMetaData.class, "getIndexInfo", null, schema, table, false, false);
        listed(DatabaseMetaData.class, "getBestRowIdentifier",
            null, schema, table, DatabaseMetaData.bestRowSession, true);
        listed(DatabaseMetaData.class, "supportsConvert", Types.INTEGER, Types.VARCHAR);
        listed(DatabaseMetaData.class, "supportsResultSetType", ResultSet.TYPE_FORWARD_ONLY);
        listed(DatabaseMetaData.class, "supportsResultSetConcurrency",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        listed(DatabaseMetaData.class, "supportsResultSetHoldability", ResultSet.HOLD_CURSORS_OVER_COMMIT);
        listed(DatabaseMetaData.class, "supportsTransactionIsolationLevel", Connection.TRANSACTION_READ_COMMITTED);
        for (String detection : List.of(
            "ownUpdatesAreVisible", "ownDeletesAreVisible", "ownInsertsAreVisible",
            "othersUpdatesAreVisible", "othersDeletesAreVisible", "othersInsertsAreVisible",
            "updatesAreDetected", "deletesAreDetected", "insertsAreDetected")) {
            listed(DatabaseMetaData.class, detection, ResultSet.TYPE_FORWARD_ONLY);
        }

        listed(Connection.class, "createStatement", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        listed(Connection.class, "createStatement",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        listed(Connection.class, "prepareStatement", PROBE_SQL, Statement.RETURN_GENERATED_KEYS);
        listed(Connection.class, "prepareStatement",
            PROBE_SQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        listed(Connection.class, "prepareStatement",
            PROBE_SQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        listed(Connection.class, "prepareCall", PROBE_SQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        listed(Connection.class, "prepareCall",
            PROBE_SQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        listed(Connection.class, "setTransactionIsolation", Connection.TRANSACTION_READ_COMMITTED);
        listed(Connection.class, "setHoldability", ResultSet.HOLD_CURSORS_OVER_COMMIT);
        listed(Connection.class, "setSavepoint", "probe");
        listed(Connection.class, "createArrayOf", "text", new Object[]{"a", "b"});
        listed(Connection.class, "createStruct", "text", new Object[]{"a"});
        listed(Connection.class, "setClientInfo", "ApplicationName", "probe");

        listed(Statement.class, "execute", PROBE_SQL, Statement.RETURN_GENERATED_KEYS);
        listed(Statement.class, "executeUpdate", PROBE_SQL, Statement.RETURN_GENERATED_KEYS);
        listed(Statement.class, "executeLargeUpdate", PROBE_SQL, Statement.RETURN_GENERATED_KEYS);
        listed(Statement.class, "setFetchDirection", ResultSet.FETCH_FORWARD);
        listed(Statement.class, "getMoreResults", Statement.CLOSE_CURRENT_RESULT);

        listed(ResultSet.class, "setFetchDirection", ResultSet.FETCH_FORWARD);
        // The array column, so that getArray answers with a value rather than
        // with the type error every other column would raise.
        listed(ResultSet.class, "getArray", PROBE_ARRAY_COLUMN);

        listed(Array.class, "getArray", 1L, 1);
        listed(Array.class, "getResultSet", 1L, 1);
    }
}
