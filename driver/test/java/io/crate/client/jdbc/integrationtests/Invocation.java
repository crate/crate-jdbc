package io.crate.client.jdbc.integrationtests;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

/**
 * One call the sweep makes: a method of a JDBC interface, the arguments to
 * pass it, and the state the object is in when it is called. The interface is
 * part of the identity, not only the method — the same method reached through
 * {@code Statement} and through {@code CallableStatement} runs on different
 * objects and can answer differently — and so is the state, for the same
 * reason.
 */
final class Invocation {

    private final Class<?> jdbcInterface;
    private final Method method;
    private final Object[] arguments;
    private final Posture posture;
    private final String id;

    Invocation(Class<?> jdbcInterface, Method method, Object[] arguments, Posture posture) {
        this(jdbcInterface, method, arguments, posture, null);
    }

    Invocation(Class<?> jdbcInterface, Method method, Object[] arguments, Posture posture,
               String column) {
        this.jdbcInterface = jdbcInterface;
        this.method = method;
        this.arguments = arguments;
        this.posture = posture;
        this.id = id(jdbcInterface, method, posture, column);
    }

    static String id(Class<?> jdbcInterface, Method method, Posture posture) {
        return id(jdbcInterface, method, posture, null);
    }

    /**
     * How an invocation is named in the delta and in a failure report:
     * {@code DatabaseMetaData.getTables(String,String,String,String[])}, with
     * the state appended where there is one to name —
     * {@code ResultSet.getString(int)@closed} — and the column where the call
     * was pointed at one other than the first — {@code
     * ResultSet.getString(int)#details}.
     *
     * <p>The first column is left unnamed, so that a call reading it is named
     * as it always was. Every entry in the delta was written against that
     * column, and asking the same method about the others can only add names
     * rather than change the ones already spoken for.</p>
     */
    static String id(Class<?> jdbcInterface, Method method, Posture posture, String column) {
        StringBuilder id = new StringBuilder(jdbcInterface.getSimpleName())
            .append('.').append(method.getName()).append('(');
        Class<?>[] parameters = method.getParameterTypes();
        for (int i = 0; i < parameters.length; i++) {
            id.append(i > 0 ? "," : "").append(parameters[i].getSimpleName());
        }
        id.append(')');
        if (posture.tag() != null) {
            id.append('@').append(posture.tag());
        }
        return column == null ? id.toString() : id.append('#').append(column).toString();
    }

    String id() {
        return id;
    }

    Class<?> jdbcInterface() {
        return jdbcInterface;
    }

    Method method() {
        return method;
    }

    Posture posture() {
        return posture;
    }

    /**
     * Calls the method on the object the fixture holds for this interface and
     * says what came back. A fixture that cannot produce the object is an
     * answer of its own: whether a connection yields a callable statement is
     * as much a driver behavior as what that statement then does.
     */
    Outcome runOn(Fixture fixture) {
        Object target;
        Object[] resolved;
        try {
            target = fixture.target(jdbcInterface);
            resolved = fixture.resolve(arguments);
            fixture.settle(jdbcInterface);
        } catch (SQLException | RuntimeException e) {
            return Outcome.unavailable(e);
        }
        try {
            return Outcome.of(method.invoke(target, resolved));
        } catch (InvocationTargetException e) {
            return Outcome.raised(e.getCause());
        } catch (IllegalAccessException | IllegalArgumentException e) {
            throw new IllegalStateException("Cannot call " + id, e);
        }
    }

    @Override
    public String toString() {
        return id;
    }
}
