package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Reading a {@link Array} by anything other than the whole of it.
 *
 * <p>Two implementations answer these calls. An array of ordinary elements is
 * pgJDBC's, wrapped only so that OBJECT elements arrive as {@code Map}. An
 * array whose elements are themselves arrays is the driver's own, because the
 * PostgreSQL array format cannot hold sub-arrays of differing length and such a
 * column travels as json instead. They agree on most of this API and part
 * company in two places, and most of the pinning here covers those.</p>
 */
public class ArrayIT extends BaseIntegrationTest {

    private static Connection conn;

    /** {@code [{n:1}, {n:2}, {n:3}]}, read through pgJDBC's array. */
    private static Array objects;

    /** {@code [[1,2], [3], [4,5,6]]}, read through the driver's own. */
    private static Array arrays;

    @BeforeAll
    static void readTheArrays() throws Exception {
        dropAllUserTables();
        conn = connect();
        try (Statement statement = conn.createStatement()) {
            statement.execute(
                "create table arrays (objs array(object as (n integer)), nested array(array(integer)))"
                + " clustered into 1 shards with (number_of_replicas=0)");
            statement.execute(
                "insert into arrays (objs, nested) values"
                + " ([{n=1}, {n=2}, {n=3}], [[1,2],[3],[4,5,6]])");
            statement.execute("refresh table arrays");
        }
        try (Statement statement = conn.createStatement()) {
            ResultSet row = statement.executeQuery("select objs, nested from arrays");
            row.next();
            objects = row.getArray(1);
            arrays = row.getArray(2);
        }
    }

    @AfterAll
    static void dropTheTable() throws Exception {
        if (conn != null) {
            conn.close();
        }
        dropAllUserTables();
    }

    /** The {@code n} of each OBJECT element, in order. */
    private static List<Object> valuesOf(Object slice) {
        List<Object> values = new ArrayList<>();
        for (Object element : (Object[]) slice) {
            values.add(((Map<?, ?>) element).get("n"));
        }
        return values;
    }

    private static List<Object> rowValues(ResultSet rows, int column) throws SQLException {
        List<Object> values = new ArrayList<>();
        while (rows.next()) {
            values.add(rows.getObject(column));
        }
        return values;
    }

    /** JDBC counts array elements from one, and both implementations do. */
    @Test
    public void aSliceIsCountedFromTheFirstElement() throws Exception {
        assertThat(valuesOf(objects.getArray(1, 2)), is(List.of(1L, 2L)));
        assertThat(valuesOf(objects.getArray(3, 1)), is(List.of(3L)));

        assertThat((Object[]) arrays.getArray(2, 2),
            is(new Object[]{new Object[]{3L}, new Object[]{4L, 5L, 6L}}));
    }

    /**
     * A count of zero reads the rest of the array rather than none of it — but
     * only from the first element. Past it the two part company: the driver's
     * own array reads the remainder, and pgJDBC's refuses, having added the
     * count to the index before noticing it was zero.
     */
    @Test
    public void aCountOfZeroReadsTheRestOnlyFromTheFirstElement() throws Exception {
        assertThat(valuesOf(objects.getArray(1, 0)), is(List.of(1L, 2L, 3L)));
        assertThat((Object[]) arrays.getArray(1, 0), is(arrayWithSize(3)));

        assertThrows(SQLException.class, () -> objects.getArray(2, 0));
        assertThat((Object[]) arrays.getArray(2, 0), is(arrayWithSize(2)));

        // The rest of an array starting just past its last element is none of
        // it, a slice the array has rather than one it does not.
        assertThat((Object[]) arrays.getArray(4, 0), is(arrayWithSize(0)));
    }

    /**
     * A slice naming elements the array does not have is refused rather than
     * shortened. The driver's own array says how many it holds, and a
     * caller needs to correct the call.
     */
    @Test
    public void aSliceOutsideTheArrayIsRefused() throws Exception {
        assertThrows(SQLException.class, () -> objects.getArray(0, 1));
        assertThrows(SQLException.class, () -> objects.getArray(1, 99));

        assertThrows(SQLDataException.class, () -> arrays.getArray(0, 1));
        assertThrows(SQLDataException.class, () -> arrays.getArray(1, -1));
        assertThrows(SQLDataException.class, () -> arrays.getArray(4, 1));
        SQLException refused = assertThrows(SQLDataException.class, () -> arrays.getArray(1, 99));
        assertThat(refused.getMessage(), containsString("holds 3"));
    }

    /**
     * The type map names the Java classes to read SQL user types into, and
     * neither implementation has one to name: json carries no such type. Both
     * read the same elements with a map as without.
     */
    @Test
    public void aTypeMapChangesNothing() throws Exception {
        assertThat(valuesOf(objects.getArray(Map.of())), is(List.of(1L, 2L, 3L)));
        assertThat(valuesOf(objects.getArray(1, 2, Map.of())), is(List.of(1L, 2L)));
        assertThat((Object[]) arrays.getArray(Map.of()), is(arrayWithSize(3)));
        assertThat((Object[]) arrays.getArray(2, 2, Map.of()), is(arrayWithSize(2)));
    }

    /**
     * Read as rows, an array is index and value pairs, and an OBJECT value is
     * a {@code Map} there too. The rows belong to the array rather than to a
     * statement, so they report none.
     */
    @Test
    public void anArrayReadsAsRowsOfIndexAndValue() throws Exception {
        ResultSet rows = objects.getResultSet();
        assertThat(rows.getStatement(), is(nullValue()));

        assertThat(rows.next(), is(true));
        assertThat(rows.getInt(1), is(1));
        assertThat(rows.getObject(2), is(instanceOf(Map.class)));
        assertThat(((Map<?, ?>) rows.getObject(2)).get("n"), is(1L));
        assertThat(rowValues(rows, 1), is(List.of(2, 3)));
    }

    /** The rows can be sliced the same way the values can, type map or not. */
    @Test
    public void theRowsOfAnArrayCanBeSliced() throws Exception {
        assertThat(rowValues(objects.getResultSet(2, 2), 1), is(List.of(2, 3)));
        assertThat(rowValues(objects.getResultSet(1, 1, Map.of()), 1), is(List.of(1)));
        assertThat(rowValues(objects.getResultSet(Map.of()), 1), is(List.of(1, 2, 3)));
    }

    /**
     * An array of arrays has no row form: a row would have to describe a column
     * whose type is "array", which the PostgreSQL protocol has no descriptor
     * for. The refusal names the call that does work.
     */
    @Test
    public void anArrayOfArraysHasNoRowForm() {
        for (ThrowingCall call : List.<ThrowingCall>of(
                arrays::getResultSet,
                () -> arrays.getResultSet(Map.of()),
                () -> arrays.getResultSet(1, 2),
                () -> arrays.getResultSet(1, 2, Map.of()))) {
            SQLException refused = assertThrows(SQLFeatureNotSupportedException.class, call::run);
            assertThat(refused.getMessage(), containsString("getArray()"));
        }
    }

    @FunctionalInterface
    interface ThrowingCall {
        Object run() throws SQLException;
    }

    /**
     * Both report json as the element type: one because its elements are
     * OBJECT values, the other because there is no PostgreSQL element type
     * behind an array of arrays to name. json is the type JDBC has no code of
     * its own for, so both describe their elements as {@code OTHER}.
     */
    @Test
    public void bothReportJsonAsTheElementType() throws Exception {
        assertThat(objects.getBaseTypeName(), is("json"));
        assertThat(objects.getBaseType(), is(Types.OTHER));

        assertThat(arrays.getBaseTypeName(), is("json"));
        assertThat(arrays.getBaseType(), is(Types.OTHER));
    }

    /** The text of an array is the literal it arrived as. */
    @Test
    public void theTextOfAnArrayIsTheFormItArrivedIn() {
        assertThat(objects.toString(), containsString("\\\"n\\\":1"));
        assertThat(arrays.toString(), is("[[1,2],[3],[4,5,6]]"));
    }

    /**
     * Giving up an array ends it, but only where there was something to give
     * up. pgJDBC's array drops what it read the elements from, so every read
     * raises afterwards. The driver's own holds nothing open, the value
     * having come in with the row, so freeing it changes nothing.
     *
     * <p>Read last, because it leaves the arrays the other tests share
     * unusable.</p>
     */
    @Test
    public void freeingEndsADelegatedArrayAndNotAJsonOne() throws Exception {
        try (Statement statement = conn.createStatement()) {
            ResultSet row = statement.executeQuery("select objs, nested from arrays");
            row.next();
            Array ownObjects = row.getArray(1);
            Array ownArrays = row.getArray(2);

            ownArrays.free();
            assertThat((Object[]) ownArrays.getArray(), is(arrayWithSize(3)));

            ownObjects.free();
            assertThrows(SQLException.class, ownObjects::getArray);
            assertThrows(SQLException.class, ownObjects::getResultSet);
            assertThrows(SQLException.class, ownObjects::getBaseTypeName);
            assertThrows(SQLException.class, ownObjects::getBaseType);
            assertThrows(SQLException.class, () -> ownObjects.getArray(1, 1));
            // Freeing is the one call an ended array still answers.
            ownObjects.free();
        }
    }
}
