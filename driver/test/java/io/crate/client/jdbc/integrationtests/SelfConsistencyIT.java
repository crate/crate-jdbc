package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * The driver held against itself.
 *
 * <p>Every rule here compares two of the driver's own answers, so none of them
 * needs a reference implementation or a value written down in advance. A
 * driver that says a column holds one class and then hands out another, or
 * that denies a feature it goes on to perform, has contradicted itself — and
 * that is decidable without anyone deciding first which of the two answers was
 * meant.</p>
 *
 * <p>{@link DifferentialIT} covers what pgJDBC would have answered. These are
 * the faults it cannot see, because a driver can be wrong in exactly the way
 * the one it is compared against is wrong.</p>
 */
public class SelfConsistencyIT extends BaseIntegrationTest {

    private static final String TABLE = "self_consistency";

    /** The row every rule reads. Its columns cover each shape the driver converts. */
    private static final int POPULATED_ROW = 1;

    /** A row whose every column but the key is null, for the rules about nulls. */
    private static final int EMPTY_ROW = 2;

    private static Connection conn;
    private static PgJdbcDelta delta;

    @BeforeAll
    static void setUpTable() throws Exception {
        delta = PgJdbcDelta.load();
        dropAllUserTables();
        conn = connect();
        try (Statement statement = conn.createStatement()) {
            statement.execute(
                "create table " + TABLE + " (" +
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
                " ip_field ip," +
                " object_field object as (note string, count integer)," +
                " string_array array(string)," +
                " integer_array array(integer)," +
                " nested_array array(array(integer))," +
                " object_array array(object as (note string))," +
                " geo_point_field geo_point," +
                " geo_shape_field geo_shape" +
                ") clustered into 1 shards with (number_of_replicas = 0)");
            statement.execute(
                "insert into " + TABLE + " values (" + POPULATED_ROW + ", 'text', true, 1, 2, 3, 4, 5.5, 6.5," +
                " 1000, '127.0.0.1', {note = 'note', count = 7}, ['a', 'b'], [1, 2], [[1, 2], [3]]," +
                " [{note = 'first'}], [9.7419021, 47.4048045]," +
                " 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')");
            statement.execute("insert into " + TABLE + " (id) values (" + EMPTY_ROW + ")");
            statement.execute("refresh table " + TABLE);
        }
        ensureYellow();
    }

    @AfterAll
    static void dropTable() throws Exception {
        if (conn != null) {
            conn.close();
        }
        dropAllUserTables();
    }

    private static ResultSet rowsOf(String sql) throws SQLException {
        return conn.createStatement().executeQuery(sql);
    }

    private static ResultSet populatedRow() throws SQLException {
        ResultSet row = rowsOf("select * from " + TABLE + " where id = " + POPULATED_ROW);
        if (!row.next()) {
            throw new SQLException("The populated row is missing");
        }
        return row;
    }

    /**
     * What {@code getColumnClassName} promises has to be what {@code getObject}
     * delivers. The two are read from the same driver about the same column, so
     * a caller that follows the metadata to decide how to hold a value and then
     * finds something else in its hands has been told two different things.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("readableResultSets")
    public void everyColumnIsReadAsTheClassItsMetadataNames(String description, ResultSetSource source)
            throws Exception {
        List<String> contradictions = new ArrayList<>();
        try (ResultSet rows = source.open()) {
            ResultSetMetaData metaData = rows.getMetaData();
            int rowsRead = 0;
            while (rows.next() && rowsRead++ < 20) {
                for (int column = 1; column <= metaData.getColumnCount(); column++) {
                    Object value = rows.getObject(column);
                    if (value == null) {
                        continue;
                    }
                    String promised = metaData.getColumnClassName(column);
                    if (!Class.forName(promised).isInstance(value)) {
                        contradictions.add(metaData.getColumnLabel(column)
                            + ": metadata names " + promised
                            + ", getObject produced " + value.getClass().getName());
                    }
                }
            }
        }
        assertThat(description + " reads columns as classes its metadata does not name:\n  "
            + String.join("\n  ", contradictions), contradictions, is(empty()));
    }

    /**
     * A column index outside the row is refused, and refused as a
     * {@link SQLException}. The metadata says how many columns there are, so
     * asking about one it does not describe is a caller's mistake — and JDBC
     * has a driver report a mistake as something the caller can catch. A
     * reading call that decides what a column holds before reading it has two
     * chances to reach past the end instead: what it kept about the columns
     * that exist, and what it asks the wrapped driver.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("readableResultSets")
    public void everyReadingCallRefusesAColumnOutsideTheRow(String description, ResultSetSource source)
            throws Exception {
        List<String> escaped = new ArrayList<>();
        try (ResultSet rows = source.open()) {
            ResultSetMetaData metaData = rows.getMetaData();
            boolean positioned = rows.next();
            for (int column : new int[]{0, -1, metaData.getColumnCount() + 1}) {
                for (Map.Entry<String, Reading> reading : readingsByColumn().entrySet()) {
                    if (reading.getValue().needsARow() && !positioned) {
                        continue;
                    }
                    escaped.addAll(refusal(reading.getKey(), column,
                        () -> reading.getValue().from(rows, metaData, column)));
                }
            }
        }
        assertThat(description + " does not refuse a column outside the row:\n  "
            + String.join("\n  ", escaped), escaped, is(empty()));
    }

    /**
     * What a call did about a column that is not there, in the terms the rule
     * cares about: nothing where it should have refused, or a failure a caller
     * cannot catch.
     */
    private static List<String> refusal(String called, int column, Reading.Call call) {
        try {
            call.run();
            return List.of(called + "(" + column + ") answered instead of refusing");
        } catch (SQLException refused) {
            return List.of();
        } catch (RuntimeException | Error escaped) {
            return List.of(called + "(" + column + ") raised " + escaped.getClass().getName()
                + ", which a caller catching SQLException does not catch");
        }
    }

    /**
     * The calls that take a column index, each named as an application would
     * write it. The ones this driver answers for itself come first — they
     * decide what a column holds before reading it, which is the step that can
     * reach past the columns there are.
     */
    private static Map<String, Reading> readingsByColumn() {
        Map<String, Reading> readings = new LinkedHashMap<>();
        readings.put("ResultSetMetaData.getColumnClassName",
            Reading.ofMetaData(ResultSetMetaData::getColumnClassName));
        readings.put("ResultSetMetaData.getColumnTypeName",
            Reading.ofMetaData(ResultSetMetaData::getColumnTypeName));
        readings.put("ResultSet.getObject", Reading.ofRow(ResultSet::getObject));
        readings.put("ResultSet.getObject(Class)",
            Reading.ofRow((rows, column) -> rows.getObject(column, String.class)));
        readings.put("ResultSet.getArray", Reading.ofRow(ResultSet::getArray));
        readings.put("ResultSet.getString", Reading.ofRow(ResultSet::getString));
        return readings;
    }

    /**
     * One call that takes a column index, and whether it has to be made while
     * the result set is on a row. A call about the columns themselves is
     * answerable before the first row and after the last.
     */
    interface Reading {

        @FunctionalInterface
        interface Call {
            void run() throws SQLException;
        }

        @FunctionalInterface
        interface OfMetaData {
            Object from(ResultSetMetaData metaData, int column) throws SQLException;
        }

        @FunctionalInterface
        interface OfRow {
            Object from(ResultSet rows, int column) throws SQLException;
        }

        void from(ResultSet rows, ResultSetMetaData metaData, int column) throws SQLException;

        boolean needsARow();

        static Reading ofMetaData(OfMetaData call) {
            return new Reading() {
                @Override
                public void from(ResultSet rows, ResultSetMetaData metaData, int column) throws SQLException {
                    call.from(metaData, column);
                }

                @Override
                public boolean needsARow() {
                    return false;
                }
            };
        }

        static Reading ofRow(OfRow call) {
            return new Reading() {
                @Override
                public void from(ResultSet rows, ResultSetMetaData metaData, int column) throws SQLException {
                    call.from(rows, column);
                }

                @Override
                public boolean needsARow() {
                    return true;
                }
            };
        }
    }

    /**
     * {@code wasNull} answers about the value just read, so it has to agree
     * with it — for a column that holds nothing, and for one that holds
     * something the read converts.
     */
    @ParameterizedTest(name = "row {0}")
    @MethodSource("rowIds")
    public void wasNullAgreesWithTheValueRead(int id) throws Exception {
        List<String> contradictions = new ArrayList<>();
        try (ResultSet rows = rowsOf("select * from " + TABLE + " where id = " + id)) {
            rows.next();
            ResultSetMetaData metaData = rows.getMetaData();
            for (int column = 1; column <= metaData.getColumnCount(); column++) {
                Object value = rows.getObject(column);
                if ((value == null) != rows.wasNull()) {
                    contradictions.add(metaData.getColumnLabel(column)
                        + ": getObject gave " + value + ", wasNull said " + rows.wasNull());
                }
                String text = rows.getString(column);
                if ((text == null) != rows.wasNull()) {
                    contradictions.add(metaData.getColumnLabel(column)
                        + ": getString gave " + text + ", wasNull said " + rows.wasNull());
                }
            }
        }
        assertThat(String.join("\n  ", contradictions), contradictions, is(empty()));
    }

    /**
     * An array read whole and the same array read by range describe one value.
     * The driver has two {@link Array} implementations — one wrapping pgJDBC's
     * for arrays of scalars, one reading json for arrays of arrays — and a
     * caller reaches them through the same interface without being told which
     * it holds.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("arrayColumns")
    public void anArrayReadWholeAndReadByRangeDescribeOneValue(String column) throws Exception {
        try (ResultSet rows = populatedRow()) {
            Array array = rows.getArray(column);
            Object whole = array.getArray();
            int length = java.lang.reflect.Array.getLength(whole);
            assertThat(column + ": a range starting at the first element and counting zero is "
                    + "the rest of the array",
                Arrays.deepToString((Object[]) array.getArray(1, 0)),
                is(Arrays.deepToString((Object[]) whole)));
            assertThat(column + ": a range covering every element is the whole array",
                Arrays.deepToString((Object[]) array.getArray(1, length)),
                is(Arrays.deepToString((Object[]) whole)));
            assertThat(column + ": a range of one element is that element",
                Arrays.deepToString((Object[]) array.getArray(1, 1)),
                is(Arrays.deepToString(new Object[]{java.lang.reflect.Array.get(whole, 0)})));
        }
    }

    /**
     * A feature the metadata denies has to be refused, and one it claims has to
     * work. An application asks the metadata precisely so it can avoid the
     * call, and a driver that answers one way and behaves the other sends it
     * down a path that fails later and elsewhere.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("featuresThatAreRefusedWhenUnsupported")
    public void everyFeatureTheMetadataDeniesIsRefused(
            String description, Claim claim, Operation operation) throws Exception {
        boolean claimed;
        try (Connection connection = connect()) {
            claimed = claim.of(connection.getMetaData());
        }
        try (Connection connection = connect()) {
            operation.on(connection);
            if (!claimed) {
                fail(description + " is denied by the metadata and performed anyway");
            }
        } catch (SQLFeatureNotSupportedException refused) {
            if (claimed) {
                fail(description + " is claimed by the metadata and refused as unsupported");
            }
        }
    }

    /**
     * A setting the metadata claims has to be the one a connection ends up in,
     * and one it denies has to leave the connection somewhere else. JDBC lets a
     * driver grant the nearest thing it has instead of refusing — a cursor
     * type, an isolation level — but not report back a setting it did not
     * apply, which would leave a caller believing it has a guarantee it does
     * not have.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("settingsAndTheClaimsAboutThem")
    public void everySettingTheMetadataClaimsIsTheOneInEffect(
            String description, Claim claim, Setting setting) throws Exception {
        boolean claimed;
        try (Connection connection = connect()) {
            claimed = claim.of(connection.getMetaData());
        }
        String inEffect;
        try (Connection connection = connect()) {
            inEffect = String.valueOf(setting.applyTo(connection));
        } catch (SQLFeatureNotSupportedException refused) {
            // Refusing outright is the other way a driver may answer for a
            // setting it does not have.
            inEffect = "refused";
        }
        boolean granted = inEffect.equals(setting.requested());
        if (granted == claimed) {
            return;
        }
        String complaint = claimed
            ? description + " is claimed by the metadata, and what took effect was " + inEffect
            : description + " is denied by the metadata, and took effect anyway";
        String inherited = delta.reason(PgJdbcDelta.Kind.INCONSISTENT, description);
        if (inherited == null) {
            fail(complaint + "\n  to accept it: inconsistent " + description + " :: <why>");
        }
    }

    @org.junit.jupiter.api.Test
    public void everyInheritedContradictionInTheDeltaIsStillThere() throws Exception {
        List<String> stale = new ArrayList<>(delta.listed(PgJdbcDelta.Kind.INCONSISTENT));
        stale.removeAll(contradictions());
        assertThat(
            "The delta claims these settings are reported without being applied, and they no "
            + "longer are. A driver that stopped contradicting itself needs no entry:\n  "
            + String.join("\n  ", stale),
            stale, is(empty()));
    }

    /** The settings a connection reports without having applied them. */
    private static List<String> contradictions() throws Exception {
        List<String> contradictions = new ArrayList<>();
        for (Arguments arguments : settingsAndTheClaimsAboutThem().toArray(Arguments[]::new)) {
            Object[] parts = arguments.get();
            String description = (String) parts[0];
            Setting setting = (Setting) parts[2];
            boolean claimed;
            try (Connection connection = connect()) {
                claimed = ((Claim) parts[1]).of(connection.getMetaData());
            }
            String inEffect;
            try (Connection connection = connect()) {
                inEffect = String.valueOf(setting.applyTo(connection));
            } catch (SQLFeatureNotSupportedException refused) {
                inEffect = "refused";
            }
            if (inEffect.equals(setting.requested()) != claimed) {
                contradictions.add(description);
            }
        }
        return contradictions;
    }

    /**
     * A metadata result set is read by column name, and JDBC fixes both the
     * names and the order they come in. A driver that wraps these result sets
     * has to hand them on carrying what the spec says they carry.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("metadataResultSets")
    public void everyMetadataResultSetCarriesTheColumnsJdbcSpecifies(
            String description, ResultSetSource source, String[] specified) throws Exception {
        List<String> actual = new ArrayList<>();
        try (ResultSet rows = source.open()) {
            ResultSetMetaData metaData = rows.getMetaData();
            for (int column = 1; column <= metaData.getColumnCount(); column++) {
                actual.add(metaData.getColumnLabel(column).toUpperCase(java.util.Locale.ENGLISH));
            }
        } catch (SQLFeatureNotSupportedException unanswered) {
            // JDBC lets a driver decline these outright, and then there is no
            // result set whose columns could be wrong.
            abort(description + " is not answered at all: " + unanswered.getMessage());
        }
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < specified.length; i++) {
            // A name of "?" marks a column JDBC reserves without naming.
            if (specified[i].equals("?")) {
                continue;
            }
            if (i >= actual.size()) {
                missing.add(specified[i] + " (no column " + (i + 1) + " at all)");
            } else if (!actual.get(i).equals(specified[i])) {
                missing.add(specified[i] + " (column " + (i + 1) + " is " + actual.get(i) + ")");
            }
        }
        assertThat(description + " does not carry the columns JDBC specifies:\n  "
            + String.join("\n  ", missing) + "\n  it carries " + actual, missing, is(empty()));
    }

    @FunctionalInterface
    interface ResultSetSource {
        ResultSet open() throws SQLException;
    }

    @FunctionalInterface
    interface Claim {
        boolean of(DatabaseMetaData metaData) throws SQLException;
    }

    @FunctionalInterface
    interface Operation {
        void on(Connection connection) throws SQLException;
    }

    /**
     * A request for a connection to be configured some way, and what the
     * connection says it is configured as afterwards.
     */
    abstract static class Setting {

        private final String requested;

        Setting(int requested) {
            this.requested = String.valueOf(requested);
        }

        /** The value asked for, as the connection would report it. */
        String requested() {
            return requested;
        }

        /** Applies the setting and gives back what is in effect. */
        abstract Object applyTo(Connection connection) throws SQLException;
    }

    static Stream<Integer> rowIds() {
        return Stream.of(POPULATED_ROW, EMPTY_ROW);
    }

    static Stream<String> arrayColumns() {
        return Stream.of("string_array", "integer_array", "nested_array", "object_array");
    }

    /**
     * Result sets with rows in them to read: the table covering every shape the
     * driver converts, and the metadata queries that answer with rows on a
     * CrateDB.
     */
    static Stream<Arguments> readableResultSets() {
        return Stream.of(
            Arguments.of("the probe table", (ResultSetSource) () -> rowsOf("select * from " + TABLE)),
            Arguments.of("getTables", (ResultSetSource) () ->
                conn.getMetaData().getTables(null, "doc", TABLE, null)),
            Arguments.of("getColumns", (ResultSetSource) () ->
                conn.getMetaData().getColumns(null, "doc", TABLE, "%")),
            Arguments.of("getPrimaryKeys", (ResultSetSource) () ->
                conn.getMetaData().getPrimaryKeys(null, "doc", TABLE)),
            Arguments.of("getTypeInfo", (ResultSetSource) () -> conn.getMetaData().getTypeInfo()),
            Arguments.of("getSchemas", (ResultSetSource) () -> conn.getMetaData().getSchemas()),
            Arguments.of("getCatalogs", (ResultSetSource) () -> conn.getMetaData().getCatalogs()),
            Arguments.of("getTableTypes", (ResultSetSource) () -> conn.getMetaData().getTableTypes()),
            Arguments.of("getFunctions", (ResultSetSource) () ->
                conn.getMetaData().getFunctions(null, "pg_catalog", "abs")),
            Arguments.of("getClientInfoProperties", (ResultSetSource) () ->
                conn.getMetaData().getClientInfoProperties()));
    }

    /**
     * The features JDBC has a driver refuse outright when it does not have
     * them, rather than substitute something for.
     */
    static Stream<Arguments> featuresThatAreRefusedWhenUnsupported() {
        return Stream.of(
            Arguments.of("setSavepoint()",
                (Claim) DatabaseMetaData::supportsSavepoints,
                (Operation) Connection::setSavepoint),
            Arguments.of("setSavepoint(name)",
                (Claim) DatabaseMetaData::supportsSavepoints,
                (Operation) connection -> connection.setSavepoint("probe")),
            Arguments.of("a parameter set by name",
                (Claim) DatabaseMetaData::supportsNamedParameters,
                (Operation) connection -> connection.prepareCall("select 1").setInt("probe", 1)),
            Arguments.of("getGeneratedKeys()",
                (Claim) DatabaseMetaData::supportsGetGeneratedKeys,
                (Operation) connection -> connection.createStatement().getGeneratedKeys().close()));
    }

    /**
     * The settings a connection carries, each with the metadata call that says
     * whether it can be in it.
     */
    static Stream<Arguments> settingsAndTheClaimsAboutThem() {
        List<Arguments> settings = new ArrayList<>();
        for (int level : new int[]{
            Connection.TRANSACTION_NONE, Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE}) {
            settings.add(Arguments.of("setTransactionIsolation(" + isolationName(level) + ")",
                (Claim) metaData -> metaData.supportsTransactionIsolationLevel(level),
                new Setting(level) {
                    @Override
                    Object applyTo(Connection connection) throws SQLException {
                        connection.setTransactionIsolation(level);
                        return connection.getTransactionIsolation();
                    }
                }));
        }
        for (int type : new int[]{
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.TYPE_SCROLL_SENSITIVE}) {
            settings.add(Arguments.of("createStatement(" + cursorName(type) + ", CONCUR_READ_ONLY)",
                (Claim) metaData -> metaData.supportsResultSetType(type),
                new Setting(type) {
                    @Override
                    Object applyTo(Connection connection) throws SQLException {
                        try (Statement statement =
                                 connection.createStatement(type, ResultSet.CONCUR_READ_ONLY)) {
                            return statement.getResultSetType();
                        }
                    }
                }));
        }
        for (int concurrency : new int[]{ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE}) {
            settings.add(Arguments.of(
                "createStatement(TYPE_FORWARD_ONLY, " + cursorName(concurrency) + ")",
                (Claim) metaData ->
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, concurrency),
                new Setting(concurrency) {
                    @Override
                    Object applyTo(Connection connection) throws SQLException {
                        try (Statement statement =
                                 connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, concurrency)) {
                            return statement.getResultSetConcurrency();
                        }
                    }
                }));
        }
        for (int holdability : new int[]{
            ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT}) {
            settings.add(Arguments.of("setHoldability(" + cursorName(holdability) + ")",
                (Claim) metaData -> metaData.supportsResultSetHoldability(holdability),
                new Setting(holdability) {
                    @Override
                    Object applyTo(Connection connection) throws SQLException {
                        connection.setHoldability(holdability);
                        return connection.getHoldability();
                    }
                }));
        }
        return settings.stream();
    }

    private static String isolationName(int level) {
        switch (level) {
            case Connection.TRANSACTION_NONE:
                return "TRANSACTION_NONE";
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                return "TRANSACTION_READ_UNCOMMITTED";
            case Connection.TRANSACTION_READ_COMMITTED:
                return "TRANSACTION_READ_COMMITTED";
            case Connection.TRANSACTION_REPEATABLE_READ:
                return "TRANSACTION_REPEATABLE_READ";
            default:
                return "TRANSACTION_SERIALIZABLE";
        }
    }

    private static String cursorName(int constant) {
        switch (constant) {
            case ResultSet.TYPE_FORWARD_ONLY:
                return "TYPE_FORWARD_ONLY";
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
                return "TYPE_SCROLL_INSENSITIVE";
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return "TYPE_SCROLL_SENSITIVE";
            case ResultSet.CONCUR_READ_ONLY:
                return "CONCUR_READ_ONLY";
            case ResultSet.CONCUR_UPDATABLE:
                return "CONCUR_UPDATABLE";
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                return "HOLD_CURSORS_OVER_COMMIT";
            default:
                return "CLOSE_CURSORS_AT_COMMIT";
        }
    }

    /**
     * The columns JDBC specifies for each metadata result set, in the order it
     * specifies them. A {@code ?} stands for a column the spec reserves without
     * giving it a name.
     */
    static Stream<Arguments> metadataResultSets() {
        return Stream.of(
            metadata("getTables", () -> conn.getMetaData().getTables(null, "doc", TABLE, null),
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
            metadata("getSchemas", () -> conn.getMetaData().getSchemas(),
                "TABLE_SCHEM", "TABLE_CATALOG"),
            metadata("getCatalogs", () -> conn.getMetaData().getCatalogs(), "TABLE_CAT"),
            metadata("getTableTypes", () -> conn.getMetaData().getTableTypes(), "TABLE_TYPE"),
            metadata("getColumns", () -> conn.getMetaData().getColumns(null, "doc", TABLE, "%"),
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"),
            metadata("getColumnPrivileges",
                () -> conn.getMetaData().getColumnPrivileges(null, "doc", TABLE, "%"),
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE",
                "PRIVILEGE", "IS_GRANTABLE"),
            metadata("getTablePrivileges",
                () -> conn.getMetaData().getTablePrivileges(null, "doc", TABLE),
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
                "IS_GRANTABLE"),
            metadata("getBestRowIdentifier",
                () -> conn.getMetaData().getBestRowIdentifier(
                    null, "doc", TABLE, DatabaseMetaData.bestRowSession, true),
                "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
            metadata("getVersionColumns",
                () -> conn.getMetaData().getVersionColumns(null, "doc", TABLE),
                "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
            metadata("getPrimaryKeys", () -> conn.getMetaData().getPrimaryKeys(null, "doc", TABLE),
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
            metadata("getImportedKeys", () -> conn.getMetaData().getImportedKeys(null, "doc", TABLE),
                FOREIGN_KEY_COLUMNS),
            metadata("getExportedKeys", () -> conn.getMetaData().getExportedKeys(null, "doc", TABLE),
                FOREIGN_KEY_COLUMNS),
            metadata("getCrossReference",
                () -> conn.getMetaData().getCrossReference(null, "doc", TABLE, null, "doc", TABLE),
                FOREIGN_KEY_COLUMNS),
            metadata("getTypeInfo", () -> conn.getMetaData().getTypeInfo(),
                "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
                "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE",
                "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"),
            metadata("getIndexInfo",
                () -> conn.getMetaData().getIndexInfo(null, "doc", TABLE, false, false),
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
                "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"),
            metadata("getUDTs", () -> conn.getMetaData().getUDTs(null, "doc", "%", null),
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS",
                "BASE_TYPE"),
            metadata("getSuperTypes", () -> conn.getMetaData().getSuperTypes(null, "doc", "%"),
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM",
                "SUPERTYPE_NAME"),
            metadata("getSuperTables", () -> conn.getMetaData().getSuperTables(null, "doc", TABLE),
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"),
            metadata("getAttributes", () -> conn.getMetaData().getAttributes(null, "doc", "%", "%"),
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME",
                "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"),
            metadata("getClientInfoProperties", () -> conn.getMetaData().getClientInfoProperties(),
                "NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"),
            metadata("getFunctions", () -> conn.getMetaData().getFunctions(null, "pg_catalog", "abs"),
                "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE",
                "SPECIFIC_NAME"),
            metadata("getFunctionColumns",
                () -> conn.getMetaData().getFunctionColumns(null, "pg_catalog", "abs", "%"),
                "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE",
                "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE",
                "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
            metadata("getProcedures", () -> conn.getMetaData().getProcedures(null, "doc", "%"),
                "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "?", "?", "?", "REMARKS",
                "PROCEDURE_TYPE", "SPECIFIC_NAME"),
            metadata("getProcedureColumns",
                () -> conn.getMetaData().getProcedureColumns(null, "doc", "%", "%"),
                "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE",
                "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE",
                "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
            metadata("getPseudoColumns",
                () -> conn.getMetaData().getPseudoColumns(null, "doc", TABLE, "%"),
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE",
                "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH",
                "IS_NULLABLE"));
    }

    /** The columns every foreign-key metadata call answers with. */
    private static final String[] FOREIGN_KEY_COLUMNS = {
        "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
        "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
        "FK_NAME", "PK_NAME", "DEFERRABILITY"};

    private static Arguments metadata(String description, ResultSetSource source, String... specified) {
        return Arguments.of(description, source, specified);
    }
}
