package io.crate.client.jdbc.integrationtests;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

/**
 * What a generated program is allowed to do, one operation at a time.
 *
 * <p>Not the JDBC surface: the sweep already asks every method of every
 * interface, each on an object built for it. What no sweep can ask is what a
 * method answers after another one has run — a cursor read past its close, a
 * batch executed twice, a connection committed with a statement still open.
 * So the vocabulary here is the operations that move an object from one state
 * to another, and the point of a program is the order it puts them in.</p>
 *
 * <p>Every verb declares what it needs to be called on and what it hands back,
 * which is what lets a program be generated without running it: a name enters
 * scope when the step that binds it is written, whether or not that step will
 * succeed.</p>
 */
enum Verb {

    OPEN(null, Connection.class, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return DriverManager.getConnection(url);
        }
    },
    CREATE_STATEMENT(Connection.class, Statement.class, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Connection) receiver).createStatement();
        }
    },
    PREPARE_STATEMENT(Connection.class, PreparedStatement.class, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Connection) receiver).prepareStatement((String) arguments.get(0));
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(parameterised(random, table));
        }
    },
    CONNECTION_METADATA(Connection.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Connection) receiver).getMetaData();
        }
    },
    SET_AUTO_COMMIT(Connection.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Connection) receiver).setAutoCommit((Boolean) arguments.get(0));
            return null;
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(random.nextBoolean());
        }
    },
    COMMIT(Connection.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Connection) receiver).commit();
            return null;
        }
    },
    ROLLBACK(Connection.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Connection) receiver).rollback();
            return null;
        }
    },
    SET_READ_ONLY(Connection.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Connection) receiver).setReadOnly((Boolean) arguments.get(0));
            return null;
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(random.nextBoolean());
        }
    },
    IS_VALID(Connection.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Connection) receiver).isValid(1);
        }
    },
    CREATE_ARRAY(Connection.class, Array.class, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Connection) receiver).createArrayOf("integer", new Object[]{1, 2});
        }
    },
    CLOSE_CONNECTION(Connection.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Connection) receiver).close();
            return null;
        }
    },

    EXECUTE_QUERY(Statement.class, ResultSet.class, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).executeQuery((String) arguments.get(0));
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(query(random, table));
        }
    },
    EXECUTE(Statement.class, null, true) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).execute((String) arguments.get(0));
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(anyStatement(random, table));
        }
    },
    EXECUTE_UPDATE(Statement.class, null, true) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).executeUpdate((String) arguments.get(0));
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(change(random, table));
        }
    },
    ADD_BATCH(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Statement) receiver).addBatch((String) arguments.get(0));
            return null;
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(change(random, table));
        }
    },
    EXECUTE_BATCH(Statement.class, null, true) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).executeBatch();
        }
    },
    CLEAR_BATCH(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Statement) receiver).clearBatch();
            return null;
        }
    },
    STATEMENT_RESULT(Statement.class, ResultSet.class, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).getResultSet();
        }
    },
    UPDATE_COUNT(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).getUpdateCount();
        }
    },
    MORE_RESULTS(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).getMoreResults();
        }
    },
    GENERATED_KEYS(Statement.class, ResultSet.class, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).getGeneratedKeys();
        }
    },
    SET_FETCH_SIZE(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Statement) receiver).setFetchSize((Integer) arguments.get(0));
            return null;
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(random.nextInt(3));
        }
    },
    SET_MAX_ROWS(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Statement) receiver).setMaxRows((Integer) arguments.get(0));
            return null;
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(random.nextInt(3));
        }
    },
    CLOSE_ON_COMPLETION(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Statement) receiver).closeOnCompletion();
            return null;
        }
    },
    STATEMENT_WARNINGS(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Statement) receiver).getWarnings();
        }
    },
    CLOSE_STATEMENT(Statement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Statement) receiver).close();
            return null;
        }
    },

    BIND(PreparedStatement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((PreparedStatement) receiver).setObject(1, arguments.get(0));
            return null;
        }

        @Override
        List<Object> draw(Random random, String table) {
            return List.of(random.nextInt(8));
        }
    },
    BIND_NULL(PreparedStatement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((PreparedStatement) receiver).setNull(1, java.sql.Types.INTEGER);
            return null;
        }
    },
    CLEAR_PARAMETERS(PreparedStatement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((PreparedStatement) receiver).clearParameters();
            return null;
        }
    },
    EXECUTE_PREPARED_QUERY(PreparedStatement.class, ResultSet.class, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((PreparedStatement) receiver).executeQuery();
        }
    },
    ADD_PREPARED_BATCH(PreparedStatement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((PreparedStatement) receiver).addBatch();
            return null;
        }
    },
    PARAMETER_METADATA(PreparedStatement.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((PreparedStatement) receiver).getParameterMetaData();
        }
    },

    NEXT(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((ResultSet) receiver).next();
        }
    },
    READ_STRING(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((ResultSet) receiver).getString(1);
        }
    },
    READ_OBJECT(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((ResultSet) receiver).getObject(1);
        }
    },
    WAS_NULL(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((ResultSet) receiver).wasNull();
        }
    },
    ROW_NUMBER(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((ResultSet) receiver).getRow();
        }
    },
    IS_AFTER_LAST(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((ResultSet) receiver).isAfterLast();
        }
    },
    RESULT_METADATA(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((ResultSet) receiver).getMetaData();
        }
    },
    RESULT_STATEMENT(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((ResultSet) receiver).getStatement();
        }
    },
    CLOSE_RESULT(ResultSet.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((ResultSet) receiver).close();
            return null;
        }
    },

    ARRAY_ELEMENTS(Array.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Array) receiver).getArray();
        }
    },
    ARRAY_TYPE(Array.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            return ((Array) receiver).getBaseTypeName();
        }
    },
    FREE_ARRAY(Array.class, null, false) {
        @Override
        Object call(Object receiver, List<Object> arguments, String url) throws SQLException {
            ((Array) receiver).free();
            return null;
        }
    };

    /** The table every generated statement reads and writes, until it is named. */
    static final String TABLE = "<t>";

    private final Class<?> receiver;
    private final Class<?> binds;
    private final boolean mutates;

    Verb(Class<?> receiver, Class<?> binds, boolean mutates) {
        this.receiver = receiver;
        this.binds = binds;
        this.mutates = mutates;
    }

    /** The interface this has to be called on, or null when it needs nothing. */
    Class<?> receiver() {
        return receiver;
    }

    /** The interface this hands back and a program can go on to use, or null. */
    Class<?> binds() {
        return binds;
    }

    /** Whether the rows may be different afterwards, so a refresh has to follow. */
    boolean mutates() {
        return mutates;
    }

    /** The arguments to call this with, drawn from the program's seed. */
    List<Object> draw(Random random, String table) {
        return List.of();
    }

    abstract Object call(Object receiver, List<Object> arguments, String url) throws SQLException;

    /**
     * A query. Every one of them orders its rows: a program is compared
     * against itself run by another driver, and an order the query does not
     * fix is an order the two need not agree on.
     */
    private static String query(Random random, String table) {
        List<String> queries = List.of(
            "select id, name from " + table + " order by id",
            "select id from " + table + " where id > 1 order by id",
            "select count(*) from " + table,
            "select 1",
            "not a statement at all");
        return queries.get(random.nextInt(queries.size()));
    }

    private static String change(Random random, String table) {
        List<String> changes = List.of(
            "insert into " + table + " (id, name) values (9, 'nine')",
            "update " + table + " set name = 'renamed' where id = 1",
            "delete from " + table + " where id = 3",
            "insert into " + table + " (id, name) values (1, 'a second first')");
        return changes.get(random.nextInt(changes.size()));
    }

    private static String anyStatement(Random random, String table) {
        return random.nextBoolean() ? query(random, table) : change(random, table);
    }

    private static String parameterised(Random random, String table) {
        List<String> statements = List.of(
            "select id, name from " + table + " where id = ? order by id",
            "insert into " + table + " (id, name) values (?, 'bound')",
            "select ?");
        return statements.get(random.nextInt(statements.size()));
    }
}
