package io.crate.client.jdbc.integrationtests;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A program run against one driver, and what each of its steps answered.
 *
 * <p>The table is the program's own, put back to the same three rows before
 * each run. Two drivers running one program therefore share nothing: no rows
 * left by the other, no order to agree on, no timing between them. That is
 * what makes the two traces comparable line by line, which is the whole
 * oracle.</p>
 */
final class ProgramRun {

    private final List<String> trace;

    private ProgramRun(List<String> trace) {
        this.trace = trace;
    }

    /** What each step of the program answered, in order. */
    List<String> trace() {
        return trace;
    }

    static ProgramRun of(Program program, String url, String table) throws SQLException {
        Map<String, Object> bound = new LinkedHashMap<>();
        List<String> trace = new ArrayList<>(program.steps().size());
        try {
            for (Program.Step step : program.steps()) {
                trace.add(step + " -> " + answer(step, bound, url, table).rendering());
            }
        } finally {
            closeQuietly(bound);
        }
        return new ProgramRun(trace);
    }

    private static Outcome answer(Program.Step step, Map<String, Object> bound,
                                  String url, String table) {
        Object receiver = null;
        if (step.receiver() != null) {
            receiver = bound.get(step.receiver());
            if (receiver == null) {
                // The step that would have produced it failed, so there is
                // nothing to call this on. Recorded rather than skipped: the
                // two drivers have to agree about that too.
                return Outcome.unavailable(new SQLException("unbound"));
            }
        }
        List<Object> arguments = new ArrayList<>(step.arguments().size());
        for (Object argument : step.arguments()) {
            arguments.add(argument instanceof String
                ? ((String) argument).replace(Verb.TABLE, table) : argument);
        }
        try {
            Object answered = step.verb().call(receiver, arguments, url);
            if (step.binds() != null) {
                bound.put(step.binds(), answered);
                return Outcome.bound(step.verb().binds(), answered);
            }
            return Outcome.of(answered);
        } catch (Throwable raised) {
            // Including what is not an exception at all: a driver failing an
            // assertion is an answer about the driver, and a program that met
            // one has the rest of its steps still to make.
            return Outcome.raised(raised);
        }
    }

    /**
     * The rows a program starts from, put back as they were. Every run begins
     * from the same three rows, whichever driver ran last and whatever it did
     * to them, which is what lets two traces be compared line for line. The
     * table itself is made once: creating one costs more than every program
     * that runs against it.
     */
    static void createTable(Connection admin, String table) throws SQLException {
        try (Statement statement = admin.createStatement()) {
            statement.execute("drop table if exists " + table);
            statement.execute("create table " + table + " (id integer primary key, name text)"
                + " clustered into 1 shards with (number_of_replicas = 0)");
        }
    }

    static void resetTable(Connection admin, String table) throws SQLException {
        try (Statement statement = admin.createStatement()) {
            statement.execute("delete from " + table);
            statement.execute("insert into " + table + " (id, name) values "
                + "(1, 'one'), (2, 'two'), (3, 'three')");
            statement.execute("refresh table " + table);
        }
    }

    private static void closeQuietly(Map<String, Object> bound) {
        for (Object held : bound.values()) {
            if (held instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) held).close();
                } catch (Exception ignored) {
                    // The program is allowed to have left this unusable.
                }
            }
        }
    }
}
