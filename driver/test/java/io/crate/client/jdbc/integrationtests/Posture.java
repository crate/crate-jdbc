package io.crate.client.jdbc.integrationtests;

import org.postgresql.PGStatement;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

/**
 * The state an object is in when the call under test is made on it.
 *
 * <p>A JDBC object is a state machine, and most of what a driver owes its
 * caller is what a method answers in a state that is not the first one. A
 * sweep of fresh objects reads one row of that table: it never sees what a
 * closed object refuses, what a statement says once it has run, or what a
 * cursor answers past its last row.</p>
 *
 * <p>A posture that has nothing to say about an interface applies to none of
 * its methods rather than falling back to {@link #FRESH}. Falling back would
 * sweep the same calls in the same state twice and compare them with
 * themselves.</p>
 */
enum Posture {

    /** Built and untouched. */
    FRESH(null) {
        @Override
        boolean appliesTo(Class<?> jdbcInterface) {
            return true;
        }
    },

    /**
     * No longer usable: the object is closed, or — for the two an application
     * is handed rather than opens — whatever it was read from is.
     */
    CLOSED("closed") {
        @Override
        boolean appliesTo(Class<?> jdbcInterface) {
            return true;
        }
    },

    /** A statement that has run, so its result and its counts are there to ask about. */
    EXECUTED("executed") {
        @Override
        boolean appliesTo(Class<?> jdbcInterface) {
            return STATEMENTS.contains(jdbcInterface);
        }
    },

    /** A result set walked past its last row. */
    EXHAUSTED("exhausted") {
        @Override
        boolean appliesTo(Class<?> jdbcInterface) {
            return jdbcInterface == ResultSet.class;
        }
    },

    /** A statement carrying entries no one has executed yet. */
    BATCHED("batched") {
        @Override
        boolean appliesTo(Class<?> jdbcInterface) {
            return STATEMENTS.contains(jdbcInterface);
        }
    };

    private static final Set<Class<?>> STATEMENTS = Set.of(
        Statement.class, PreparedStatement.class, CallableStatement.class, PGStatement.class);

    private final String tag;

    Posture(String tag) {
        this.tag = tag;
    }

    /**
     * What this posture is called where a call is named, or null for the one
     * that needs no saying. Leaving {@link #FRESH} unnamed keeps every call
     * the sweep already knew about under the name it already had.
     */
    String tag() {
        return tag;
    }

    abstract boolean appliesTo(Class<?> jdbcInterface);
}
