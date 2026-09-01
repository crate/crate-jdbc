package io.crate.client.jdbc.integrationtests;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * What an application can rely on when it catches a {@link SQLException}.
 *
 * <p>A SQLState is the one part of an error a program is meant to branch on:
 * the message is for a person and the vendor code is the server's own. So it
 * has to be there, has to be five characters, and its first two — the class —
 * have to name a condition the standard defines. Where JDBC also gives the
 * condition a subclass of {@code SQLException}, the two have to agree, or a
 * caller catching {@code SQLTimeoutException} and a caller testing the state
 * are told different things about one error.</p>
 */
final class SqlStateContract {

    /**
     * The condition classes the standard and PostgreSQL's appendix define
     * between them. A state outside these was invented by whoever threw it,
     * and no caller can look it up.
     */
    private static final Set<String> CLASSES = Set.of(
        "00", "01", "02", "03", "08", "09", "0A", "0B", "0F", "0L", "0P", "0Z",
        "20", "21", "22", "23", "24", "25", "26", "27", "28", "2B", "2D", "2F",
        "34", "38", "39", "3B", "3D", "3F", "40", "42", "44",
        "53", "54", "55", "57", "58", "72", "F0", "HV", "P0", "XX");

    /**
     * The condition class each JDBC subclass names, most specific first —
     * several of them extend one another, and the nearest is the one that
     * says the most.
     */
    private static final Map<Class<? extends SQLException>, Set<String>> IMPLIED = implied();

    private static Map<Class<? extends SQLException>, Set<String>> implied() {
        Map<Class<? extends SQLException>, Set<String>> implied = new LinkedHashMap<>();
        implied.put(SQLFeatureNotSupportedException.class, Set.of("0A"));
        implied.put(SQLIntegrityConstraintViolationException.class, Set.of("23"));
        implied.put(SQLInvalidAuthorizationSpecException.class, Set.of("28"));
        implied.put(SQLSyntaxErrorException.class, Set.of("42"));
        implied.put(SQLDataException.class, Set.of("22"));
        implied.put(SQLTransactionRollbackException.class, Set.of("40"));
        // A timeout reaches the caller either as the cancellation the server
        // acted on or as the connection it was cancelled through.
        implied.put(SQLTimeoutException.class, Set.of("57", "08"));
        implied.put(SQLTransientConnectionException.class, Set.of("08"));
        implied.put(SQLNonTransientConnectionException.class, Set.of("08"));
        return implied;
    }

    private SqlStateContract() {
    }

    /**
     * What is wrong with this exception's SQLState, said plainly enough to
     * decide from, or null when nothing is.
     */
    static String faultIn(SQLException thrown) {
        String state = thrown.getSQLState();
        if (state == null || state.isEmpty()) {
            return "carries no SQLState at all";
        }
        if (state.length() != 5) {
            return "carries '" + state + "', which is not five characters";
        }
        for (int i = 0; i < state.length(); i++) {
            char character = state.charAt(i);
            if ((character < '0' || character > '9') && (character < 'A' || character > 'Z')) {
                return "carries '" + state + "', which is not five digits or capitals";
            }
        }
        if (state.equals("00000")) {
            return "reports success as the state of a failure";
        }
        String condition = state.substring(0, 2);
        if (!CLASSES.contains(condition)) {
            return "carries '" + state + "', whose class " + condition + " names no defined condition";
        }
        for (Map.Entry<Class<? extends SQLException>, Set<String>> entry : IMPLIED.entrySet()) {
            if (entry.getKey().isInstance(thrown)) {
                return entry.getValue().contains(condition) ? null
                    : "is a " + entry.getKey().getSimpleName() + " carrying '" + state
                      + "', where its class means " + String.join(" or ", entry.getValue());
            }
        }
        return null;
    }
}
