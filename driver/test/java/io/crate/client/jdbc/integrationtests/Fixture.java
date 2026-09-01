package io.crate.client.jdbc.integrationtests;

import org.postgresql.PGConnection;
import org.postgresql.PGResultSetMetaData;
import org.postgresql.PGStatement;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The objects one call is made on, built fresh for that call and put into the
 * state the call is to be made in.
 *
 * <p>Freshness is the whole point. JDBC objects carry state — a cursor
 * position, an auto-commit flag, a batch — so a sweep sharing them would
 * measure the order it happened to run in. A connection to a local CrateDB
 * costs a couple of milliseconds, which buys an answer that depends on nothing
 * but the call and the {@link Posture} it was asked for.</p>
 *
 * <p>Each object is built only when a call asks for it, so a
 * {@code DatabaseMetaData} question never pays for a query.</p>
 */
final class Fixture implements AutoCloseable {

    private final String url;
    private final Posture posture;

    private Connection connection;
    private Statement statement;
    private PreparedStatement preparedStatement;
    private CallableStatement callableStatement;
    private ResultSet resultSet;
    private ResultSetMetaData resultSetMetaData;
    private Array array;

    private Fixture(String url, Posture posture) {
        this.url = url;
        this.posture = posture;
    }

    static Fixture on(String url, Posture posture) {
        return new Fixture(url, posture);
    }

    /** The object a call on the given interface is made on. */
    Object target(Class<?> jdbcInterface) throws SQLException {
        if (jdbcInterface == Connection.class || jdbcInterface == PGConnection.class) {
            return connection();
        }
        if (jdbcInterface == Statement.class || jdbcInterface == PGStatement.class) {
            return statement();
        }
        if (jdbcInterface == PreparedStatement.class) {
            return preparedStatement();
        }
        if (jdbcInterface == CallableStatement.class) {
            return callableStatement();
        }
        if (jdbcInterface == ResultSet.class) {
            return resultSet();
        }
        if (jdbcInterface == ResultSetMetaData.class || jdbcInterface == PGResultSetMetaData.class) {
            return resultSetMetaData();
        }
        if (jdbcInterface == DatabaseMetaData.class) {
            return connection().getMetaData();
        }
        if (jdbcInterface == Array.class) {
            return array();
        }
        throw new IllegalArgumentException("No fixture for " + jdbcInterface);
    }

    /**
     * Puts what has been built into the posture's state, after the arguments
     * are resolved and before the call is made. The order matters: an argument
     * the connection has to make — a stream, an array — cannot be made once
     * the connection is closed, and building it afterwards would measure this
     * harness rather than the driver.
     */
    void settle(Class<?> jdbcInterface) throws SQLException {
        if (posture == Posture.CLOSED) {
            closeWhatIsCalledOn(jdbcInterface);
        } else if (posture == Posture.EXECUTED) {
            execute(jdbcInterface);
        } else if (posture == Posture.EXHAUSTED) {
            while (resultSet().next()) {
                // Walked to the end, which is where the cursor is asked from.
            }
        } else if (posture == Posture.BATCHED) {
            fillBatch(jdbcInterface);
        }
    }

    /**
     * Closes the object the call is to be made on. A
     * {@code ResultSetMetaData} and a {@code DatabaseMetaData} are handed out
     * rather than opened and have nothing of their own to close, so what they
     * describe is closed instead — which is the state an application reaches
     * by holding one past the result set or the connection it came from.
     */
    private void closeWhatIsCalledOn(Class<?> jdbcInterface) throws SQLException {
        if (jdbcInterface == Connection.class || jdbcInterface == PGConnection.class
                || jdbcInterface == DatabaseMetaData.class) {
            connection().close();
        } else if (jdbcInterface == Statement.class || jdbcInterface == PGStatement.class) {
            statement().close();
        } else if (jdbcInterface == PreparedStatement.class) {
            preparedStatement().close();
        } else if (jdbcInterface == CallableStatement.class) {
            callableStatement().close();
        } else if (jdbcInterface == Array.class) {
            array().free();
        } else {
            resultSet().close();
        }
    }

    /**
     * Runs the statement the call is to be made on. The probe query is a
     * select, here as everywhere in the sweep: a statement that changed the
     * probe table would make every later call depend on how many times the
     * sweep had run.
     */
    private void execute(Class<?> jdbcInterface) throws SQLException {
        if (jdbcInterface == PreparedStatement.class) {
            preparedStatement().execute();
        } else if (jdbcInterface == CallableStatement.class) {
            callableStatement().execute();
        } else {
            statement().execute(JdbcSurface.PROBE_SQL);
        }
    }

    /** Two entries, so that a batch has a length to answer with. */
    private void fillBatch(Class<?> jdbcInterface) throws SQLException {
        for (int entry = 0; entry < 2; entry++) {
            if (jdbcInterface == PreparedStatement.class) {
                preparedStatement().addBatch();
            } else if (jdbcInterface == CallableStatement.class) {
                callableStatement().addBatch();
            } else {
                statement().addBatch(JdbcSurface.PROBE_SQL);
            }
        }
    }

    /**
     * The argument vector with its connection-bound placeholders made real.
     * An {@link Array} argument has to come from the driver under test, and a
     * stream is spent by the call that reads it.
     */
    Object[] resolve(Object[] arguments) throws SQLException {
        Object[] resolved = new Object[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            resolved[i] = arguments[i] instanceof JdbcSurface.Placeholder
                ? ((JdbcSurface.Placeholder) arguments[i]).resolve(connection())
                : arguments[i];
        }
        return resolved;
    }

    private Connection connection() throws SQLException {
        if (connection == null) {
            connection = DriverManager.getConnection(url);
        }
        return connection;
    }

    private Statement statement() throws SQLException {
        if (statement == null) {
            statement = connection().createStatement();
        }
        return statement;
    }

    private PreparedStatement preparedStatement() throws SQLException {
        if (preparedStatement == null) {
            preparedStatement = connection().prepareStatement(JdbcSurface.PROBE_SQL);
        }
        return preparedStatement;
    }

    private CallableStatement callableStatement() throws SQLException {
        if (callableStatement == null) {
            callableStatement = connection().prepareCall(JdbcSurface.PROBE_SQL);
        }
        return callableStatement;
    }

    /** A result set over the probe row, positioned on it so getters answer. */
    private ResultSet resultSet() throws SQLException {
        if (resultSet == null) {
            resultSet = statement().executeQuery(JdbcSurface.PROBE_SQL);
            if (!resultSet.next()) {
                throw new SQLException("The probe table holds no row to read");
            }
        }
        return resultSet;
    }

    private ResultSetMetaData resultSetMetaData() throws SQLException {
        if (resultSetMetaData == null) {
            resultSetMetaData = resultSet().getMetaData();
        }
        return resultSetMetaData;
    }

    private Array array() throws SQLException {
        if (array == null) {
            array = resultSet().getArray(JdbcSurface.PROBE_ARRAY_COLUMN);
        }
        return array;
    }

    /**
     * Gives back what the call left. A call may have closed or aborted any of
     * it on purpose, so failures here say nothing and are dropped.
     */
    @Override
    public void close() {
        if (array != null) {
            try {
                array.free();
            } catch (SQLException | RuntimeException ignored) {
                // The call under test is allowed to have left this unusable.
            }
        }
        closeQuietly(resultSet);
        closeQuietly(callableStatement);
        closeQuietly(preparedStatement);
        closeQuietly(statement);
        closeQuietly(connection);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // The call under test is allowed to have left this unusable.
        }
    }
}
