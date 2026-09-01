package io.crate.client.jdbc.integrationtests;

import java.lang.reflect.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * What one call answered, as a string two drivers can be compared on.
 *
 * <p>What the rendering leaves out is the design. Exception messages carry the
 * URL a connection was opened with and the wording each layer chose, so an
 * error is its class and its SQLState — the two things a caller can branch on.
 * A returned JDBC object is named by its interface, because which wrapper
 * class it is has its own check and every value reachable through it has its
 * own entry in the sweep. What is left is what an application actually reads:
 * values, rows, and column metadata, rendered in full.</p>
 */
final class Outcome {

    /** Rows read from a returned result set, beyond which the rendering says so and stops. */
    private static final int ROW_LIMIT = 500;

    private final String rendering;
    private final Object answer;
    private final List<RuntimeException> raisedWhileReading;
    private final Throwable thrown;
    private final boolean reached;
    private final boolean timedOut;

    private Outcome(String rendering, Object answer, List<RuntimeException> raisedWhileReading,
                    Throwable thrown, boolean reached, boolean timedOut) {
        this.rendering = rendering;
        this.answer = answer;
        this.raisedWhileReading = List.copyOf(raisedWhileReading);
        this.thrown = thrown;
        this.reached = reached;
        this.timedOut = timedOut;
    }

    /** An outcome with no answer of its own, reached without reading anything. */
    private Outcome(String rendering, Throwable thrown, boolean reached, boolean timedOut) {
        this(rendering, null, List.of(), thrown, reached, timedOut);
    }

    static Outcome of(Object value) {
        List<RuntimeException> raisedWhileReading = new ArrayList<>();
        String rendering = render(value, raisedWhileReading);
        return new Outcome(rendering, kept(value), raisedWhileReading, null, true, false);
    }

    static Outcome raised(Throwable thrown) {
        return new Outcome("threw " + describe(thrown), thrown, true, false);
    }

    /**
     * An object a caller goes on to use, named by the interface it was asked
     * for. {@link #of} would read a result set to the end and close it, which
     * is the right rendering for an answer and the wrong one for something the
     * next call is made on.
     */
    static Outcome bound(Class<?> jdbcInterface, Object value) {
        return new Outcome(value == null ? "null" : "<" + jdbcInterface.getSimpleName() + ">",
            null, true, false);
    }

    /**
     * The object to call on could not be built, or an argument bound to the
     * connection could not be made. That is an answer about the driver too, so
     * it is recorded rather than skipped.
     */
    static Outcome unavailable(Throwable cause) {
        return new Outcome("unavailable " + describe(cause), cause, false, false);
    }

    /**
     * The call did not come back inside the time the sweep waits.
     *
     * <p>What it eventually answers is unknown, so this is not an answer and
     * not a failure either: holding it to a rule about what a method raises
     * would report the wait as a JDBC fault, and the wait is the machine the
     * suite is running on more often than it is the driver. It carries the
     * exception the wait produced, so a run that keeps timing out is still
     * visible, and reads as not reached so no contract is applied to it.</p>
     */
    static Outcome timedOut(Throwable cause, long seconds) {
        return new Outcome("answered nothing within " + seconds + "s", cause, false, true);
    }

    String rendering() {
        return rendering;
    }

    /**
     * Whether the call was made at all. A rule about what a method answers has
     * nothing to say where the object to call it on was never built.
     */
    boolean reached() {
        return reached;
    }

    /**
     * Whether the call ran out of time rather than answering. Two sweeps that
     * disagree because one of them waited too long disagree about the machine
     * they ran on, so this is told apart from an answer that wandered.
     */
    boolean timedOut() {
        return timedOut;
    }

    /**
     * What the call threw, or null. A JDBC method may only raise
     * {@link SQLException}; anything else escaping is the driver failing to
     * hold its end of the contract.
     */
    Throwable thrown() {
        return thrown;
    }

    /**
     * What escaped unchecked while the answer was being read, which is the
     * same fault under a different name.
     *
     * <p>Reading rows and column metadata is driver behavior too, and a call
     * that hands back a result set has already returned by the time a cell
     * blows up — so {@link #thrown} is null and the rule about what a JDBC
     * method may raise would have nothing to look at. The rendering says an
     * error was met there; this is what it was, for a rule to hold the driver
     * to.</p>
     */
    List<RuntimeException> raisedWhileReading() {
        return raisedWhileReading;
    }

    /**
     * The answer itself, where it was of the class asked for, and null
     * otherwise. A rule about one particular answer asks here rather than
     * reading the rendering, which is written to be compared between drivers
     * and not to be parsed. A JDBC object is never one of these — see
     * {@link #kept}.
     */
    <T> T answered(Class<T> type) {
        return type.isInstance(answer) ? type.cast(answer) : null;
    }

    /** The SQLState of what the call threw, or null if it threw nothing. */
    String sqlState() {
        return thrown instanceof SQLException ? ((SQLException) thrown).getSQLState() : null;
    }

    private static String describe(Throwable thrown) {
        if (thrown instanceof SQLException) {
            return thrown.getClass().getSimpleName() + "/" + ((SQLException) thrown).getSQLState();
        }
        return thrown.getClass().getSimpleName();
    }

    /**
     * The answer, held on to where holding it costs nothing.
     *
     * <p>An outcome lives as long as the sweep it belongs to, so a JDBC object
     * — everything an application can unwrap — is let go of instead: it keeps
     * a connection or a cursor from being collected for the rest of the run,
     * and what there is to ask about one of those is the interface it was
     * asked for, which the rendering already names.</p>
     */
    private static Object kept(Object value) {
        return value instanceof java.sql.Wrapper ? null : value;
    }

    private static String render(Object value, List<RuntimeException> raisedWhileReading) {
        if (value == null) {
            return "null";
        }
        if (value instanceof ResultSet) {
            return renderRows((ResultSet) value, raisedWhileReading);
        }
        if (value instanceof ResultSetMetaData) {
            return renderColumns((ResultSetMetaData) value, raisedWhileReading);
        }
        if (value instanceof java.sql.Array) {
            return renderArray((java.sql.Array) value, raisedWhileReading);
        }
        // Named rather than unfolded: an object handed out is reached by its
        // own sweep entries, and which class implements it is not this check's
        // question.
        if (value instanceof CallableStatement) {
            return "<CallableStatement>";
        }
        if (value instanceof PreparedStatement) {
            return "<PreparedStatement>";
        }
        if (value instanceof Statement) {
            return "<Statement>";
        }
        if (value instanceof Connection) {
            return "<Connection>";
        }
        if (value instanceof DatabaseMetaData) {
            return "<DatabaseMetaData>";
        }
        if (value instanceof ParameterMetaData) {
            return "<ParameterMetaData>";
        }
        if (value instanceof byte[]) {
            return renderBytes((byte[]) value);
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<String> elements = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                elements.add(render(Array.get(value, i), raisedWhileReading));
            }
            return elements.toString();
        }
        if (value instanceof Map) {
            Map<String, String> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                sorted.put(String.valueOf(entry.getKey()), render(entry.getValue(), raisedWhileReading));
            }
            return sorted.toString();
        }
        if (value instanceof Collection) {
            List<String> elements = new ArrayList<>();
            for (Object element : (Collection<?>) value) {
                elements.add(render(element, raisedWhileReading));
            }
            return elements.toString();
        }
        if (value instanceof Class) {
            return ((Class<?>) value).getName();
        }
        String text = String.valueOf(value);
        // A value with no toString of its own would otherwise be rendered as
        // its identity hash, which differs on every run.
        if (text.equals(defaultToString(value))) {
            return "<" + value.getClass().getSimpleName() + ">";
        }
        // The class a value came back as is half the answer, and the half this
        // driver decides: a whole number read back one class narrower, or a
        // moment read back as text, is a different answer that the text alone
        // renders identically.
        return value.getClass().getSimpleName() + "(" + text + ")";
    }

    private static String defaultToString(Object value) {
        return value.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(value));
    }

    private static String renderBytes(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return "0x" + hex;
    }

    private static String renderArray(java.sql.Array array, List<RuntimeException> raisedWhileReading) {
        try {
            return "array<" + array.getBaseTypeName() + ">"
                + render(array.getArray(), raisedWhileReading);
        } catch (SQLException e) {
            return "array!" + describe(e);
        } finally {
            try {
                array.free();
            } catch (SQLException | RuntimeException ignored) {
                // Nothing left to say about an array already given up on.
            }
        }
    }

    /**
     * Every row of a result set, each cell read as a string. Reading is itself
     * driver behavior, so a cell that cannot be read is rendered as the error
     * it raised rather than failing the rendering.
     */
    private static String renderRows(ResultSet resultSet, List<RuntimeException> raisedWhileReading) {
        StringBuilder rendering = new StringBuilder("rows");
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columns = metaData.getColumnCount();
            rendering.append('[');
            for (int i = 1; i <= columns; i++) {
                rendering.append(i > 1 ? "," : "").append(metaData.getColumnLabel(i));
            }
            rendering.append(']');
            int row = 0;
            while (resultSet.next()) {
                if (++row > ROW_LIMIT) {
                    rendering.append("\n  … more than ").append(ROW_LIMIT).append(" rows");
                    break;
                }
                rendering.append("\n  ");
                for (int i = 1; i <= columns; i++) {
                    rendering.append(i > 1 ? "|" : "")
                        .append(cell(resultSet, i, raisedWhileReading));
                }
            }
            if (row == 0) {
                rendering.append("\n  (no rows)");
            }
        } catch (SQLException e) {
            rendering.append("\n  !").append(describe(e));
        } finally {
            try {
                resultSet.close();
            } catch (SQLException | RuntimeException ignored) {
                // Nothing left to say about a result set already given up on.
            }
        }
        return rendering.toString();
    }

    private static String cell(ResultSet resultSet, int column,
                               List<RuntimeException> raisedWhileReading) {
        try {
            String value = resultSet.getString(column);
            return value == null ? "null" : value;
        } catch (SQLException e) {
            return "!" + describe(e);
        } catch (RuntimeException e) {
            raisedWhileReading.add(e);
            return "!" + describe(e);
        }
    }

    /**
     * The full metadata tuple of every column: the answers a caller uses to
     * decide how to read a value, which is where a driver contradicting itself
     * shows up.
     *
     * <p>Asking for it is driver behavior in the same way reading a cell is, so
     * a metadata answer that cannot be given is rendered as the error it raised
     * rather than escaping the rendering — where it would be laid at the door of
     * whichever method handed the result set back.</p>
     */
    private static String renderColumns(ResultSetMetaData metaData,
                                        List<RuntimeException> raisedWhileReading) {
        StringBuilder rendering = new StringBuilder("columns");
        try {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                rendering.append("\n  ")
                    .append(metaData.getColumnLabel(i))
                    .append(" name=").append(metaData.getColumnName(i))
                    .append(" type=").append(metaData.getColumnType(i))
                    .append(" typeName=").append(metaData.getColumnTypeName(i))
                    .append(" className=").append(metaData.getColumnClassName(i))
                    .append(" display=").append(metaData.getColumnDisplaySize(i))
                    .append(" precision=").append(metaData.getPrecision(i))
                    .append(" scale=").append(metaData.getScale(i))
                    .append(" nullable=").append(metaData.isNullable(i))
                    .append(" signed=").append(metaData.isSigned(i))
                    .append(" table=").append(metaData.getTableName(i))
                    .append(" schema=").append(metaData.getSchemaName(i))
                    .append(" catalog=").append(metaData.getCatalogName(i));
            }
        } catch (SQLException e) {
            rendering.append("\n  !").append(describe(e));
        } catch (RuntimeException e) {
            raisedWhileReading.add(e);
            rendering.append("\n  !").append(describe(e));
        }
        return rendering.toString();
    }
}
