package io.crate.client.jdbc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.PGConnection;
import org.postgresql.PGResultSetMetaData;
import org.postgresql.PGStatement;

import java.lang.reflect.Method;
import java.lang.reflect.TypeVariable;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * A JDBC object handed to an application has to be one of this driver's
 * wrappers: a raw pgJDBC object reached through a getter would answer without
 * the CrateDB behavior the wrappers add, and so would everything navigated
 * from it: statement, connection, rows.
 *
 * <p>The rule is mechanical, so it is checked mechanically: every method that
 * hands out another JDBC object must carry {@link Adapted}, marking an answer
 * the driver gives itself rather than one that forwards. A JDBC release that
 * adds such a method fails here rather than silently leaking.</p>
 */
public class WrapperCompletenessTest {

    /**
     * JDBC types whose instances carry driver behavior. The two metadata types
     * are among them because this driver reads some columns and binds some
     * parameters itself, so what a value is read as and what it may be given as
     * are its answers to give. The types left out, the large-object ones,
     * hold values without converting any, so pgJDBC's own are what an
     * application should get.
     */
    private static final Set<Class<?>> WRAPPED_TYPES = Set.of(
        Connection.class, Statement.class, PreparedStatement.class,
        CallableStatement.class, ResultSet.class, DatabaseMetaData.class,
        ResultSetMetaData.class, ParameterMetaData.class, Array.class);

    @ParameterizedTest(name = "{0}")
    @MethodSource("wrappers")
    public void everyJdbcObjectHandedOutIsAWrapper(String description, Class<?> jdbcType, Class<?> wrapper) {
        assertThat(description, unwrappedGetters(jdbcType, wrapper), is(empty()));
    }

    static Stream<Arguments> wrappers() {
        return Stream.of(
            Arguments.of("Connection", Connection.class, CrateConnection.class),
            Arguments.of("Statement", Statement.class, CrateStatement.class),
            Arguments.of("PreparedStatement", PreparedStatement.class, CratePreparedStatement.class),
            Arguments.of("CallableStatement", CallableStatement.class, CrateCallableStatement.class),
            Arguments.of("ResultSet", ResultSet.class, CrateResultSet.class),
            Arguments.of("ResultSetMetaData", ResultSetMetaData.class, CrateResultSetMetaData.class),
            Arguments.of("ParameterMetaData", ParameterMetaData.class, CrateParameterMetaData.class),
            Arguments.of("DatabaseMetaData", DatabaseMetaData.class, CrateDatabaseMetaData.class),
            Arguments.of("Array", Array.class, CrateArray.class),
            Arguments.of("Array of arrays", Array.class, CrateJsonArray.class),
            // The wrappers carry pgJDBC's own interfaces too, and a method
            // added to one of those hands out a JDBC object just the same.
            Arguments.of("PGConnection", PGConnection.class, CrateConnection.class),
            Arguments.of("PGStatement", PGStatement.class, CrateStatement.class),
            Arguments.of("PGResultSetMetaData", PGResultSetMetaData.class, CrateResultSetMetaData.class)
        );
    }

    /**
     * The methods of {@code jdbcType} that return another JDBC object and
     * forward, which hands out pgJDBC's own instance.
     */
    private static List<String> unwrappedGetters(Class<?> jdbcType, Class<?> wrapper) {
        List<String> unwrapped = new ArrayList<>();
        for (Method method : jdbcType.getMethods()) {
            if (isUnwrapMethod(method)) {
                continue;
            }
            if (!WRAPPED_TYPES.contains(method.getReturnType()) && !isTypeTokenGetter(method)) {
                continue;
            }
            if (!isOverriddenIn(wrapper, method)) {
                unwrapped.add(jdbcType.getSimpleName() + "." + signature(method));
            }
        }
        return unwrapped;
    }

    /**
     * Whether a method hands back whatever type the caller names, as
     * {@code getObject(int, Class<T>)} does. Its return type erases to
     * {@code Object}, so every type it can produce is
     * invisible to a check that reads the erased signature.
     */
    private static boolean isTypeTokenGetter(Method method) {
        if (!(method.getGenericReturnType() instanceof TypeVariable)) {
            return false;
        }
        for (Class<?> parameter : method.getParameterTypes()) {
            if (parameter == Class.class) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@code unwrap} is how an application asks for the pgJDBC object on
     * purpose, so it is the one getter that may hand one out.
     */
    private static boolean isUnwrapMethod(Method method) {
        return method.getName().equals("unwrap");
    }

    /**
     * Whether the nearest declaration of the method is one the driver answers
     * itself. Wrappers build on one another, so the search follows the whole
     * chain, and stops at the first class declaring the method: a wrapper
     * further up may forward what this one adapts, or the reverse.
     */
    private static boolean isOverriddenIn(Class<?> wrapper, Method method) {
        for (Class<?> type = wrapper; type != null && type != Object.class; type = type.getSuperclass()) {
            try {
                return type.getDeclaredMethod(method.getName(), method.getParameterTypes())
                    .isAnnotationPresent(Adapted.class);
            } catch (NoSuchMethodException expected) {
                // keep walking up
            }
        }
        return false;
    }

    private static String signature(Method method) {
        StringBuilder signature = new StringBuilder(method.getName()).append('(');
        Class<?>[] parameters = method.getParameterTypes();
        for (int i = 0; i < parameters.length; i++) {
            signature.append(i > 0 ? ", " : "").append(parameters[i].getSimpleName());
        }
        return signature.append(')').toString();
    }
}
