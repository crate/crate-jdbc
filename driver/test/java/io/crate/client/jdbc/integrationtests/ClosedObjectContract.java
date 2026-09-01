package io.crate.client.jdbc.integrationtests;

import java.lang.reflect.Method;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

/**
 * What JDBC says a closed object owes its caller: every method raises a
 * {@link java.sql.SQLException}, save for the handful whose whole purpose is
 * to be asked afterwards.
 *
 * <p>This is a rule rather than a comparison, which is the point of writing it
 * down. A differential can only find a driver that refuses where the one it is
 * compared against does not; two drivers that both answer a closed object
 * agree, and an application that reads a row from a result set it already
 * closed gets a number back either way.</p>
 *
 * <p>The rule is held over {@code java.sql} alone. The pgJDBC interfaces
 * describe extensions to a connection rather than objects with a life of their
 * own, and nothing specifies what they do once it has ended.</p>
 */
final class ClosedObjectContract {

    /** The interfaces JDBC gives a closed state and a contract about it. */
    private static final Set<Class<?>> GOVERNED = Set.of(
        Connection.class, Statement.class, PreparedStatement.class,
        CallableStatement.class, ResultSet.class, Array.class);

    /**
     * Methods a closed object still answers.
     *
     * <p>{@code close} is specified as a no-op when there is nothing left to
     * close, and {@code free} the same. {@code isClosed} and {@code isValid}
     * exist to be asked exactly then. {@code unwrap} and {@code isWrapperFor}
     * ask what a class is, which closing does not change. {@code abort} is
     * defined to do nothing to a connection already gone. The quoting helpers
     * are pure functions of their argument, carried as default methods that
     * reach no database at all.</p>
     */
    private static final Set<String> ANSWERED = Set.of(
        "close", "isClosed", "isValid", "free", "unwrap", "isWrapperFor", "abort",
        "enquoteLiteral", "enquoteIdentifier", "enquoteNCharLiteral", "isSimpleIdentifier");

    private ClosedObjectContract() {
    }

    /** Whether the rule has anything to say about calls on this interface. */
    static boolean governs(Class<?> jdbcInterface) {
        return GOVERNED.contains(jdbcInterface);
    }

    /** Whether this method is one of the few a closed object still answers. */
    static boolean isAnswered(Method method) {
        return ANSWERED.contains(method.getName());
    }
}
