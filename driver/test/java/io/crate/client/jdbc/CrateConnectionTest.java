package io.crate.client.jdbc;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.PGConnection;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;

public class CrateConnectionTest {

    /**
     * The two answers a connection reads from the server once and keeps. Each
     * is a check of the kept field followed by an assignment to it, so a caller
     * arriving between the two would send a second query and be handed a second
     * object where the connection promises one.
     */
    private enum Cache {

        METADATA {
            @Override
            Object readFrom(CrateConnection connection) throws SQLException {
                return connection.getMetaData();
            }
        },

        SERVER_VERSION {
            @Override
            Object readFrom(CrateConnection connection) throws SQLException {
                return connection.getCrateVersion();
            }
        };

        abstract Object readFrom(CrateConnection connection) throws SQLException;
    }

    /**
     * Two callers arriving at once are one query and one object. The second is
     * let go the instant the first is inside the server call, which is the
     * window the whole promise turns on: it either waits there for the answer
     * being read or reads its own.
     */
    @ParameterizedTest
    @EnumSource(Cache.class)
    @Timeout(30)
    public void anAnswerKeptAcrossCallsIsReadOnceHoweverManyCallersAskForIt(Cache cache)
            throws Exception {
        Server server = new Server();
        CrateConnection connection = new CrateConnection(server.connection());

        Caller first = new Caller(cache, connection);
        first.start();
        server.awaitACallerInside();
        Caller second = new Caller(cache, connection);
        second.start();
        server.awaitTheSecondCaller(second);
        server.releaseTheCallersInside();
        first.join();
        second.join();

        assertThat(first.failure, is(nullValue()));
        assertThat(second.failure, is(nullValue()));
        assertThat("the connection queried the server " + server.queries()
                + " times for an answer it reads once",
            server.queries(), is(1));
        assertThat(second.answer, is(sameInstance(first.answer)));
    }

    /** One thread asking the connection for a cached answer. */
    private static final class Caller extends Thread {

        private final Cache cache;
        private final CrateConnection connection;

        private Object answer;
        private Throwable failure;

        private Caller(Cache cache, CrateConnection connection) {
            this.cache = cache;
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                answer = cache.readFrom(connection);
            } catch (Throwable thrown) {
                failure = thrown;
            }
        }
    }

    /**
     * A stand-in for the server, holding whoever reaches it until it is told to
     * let them go, so that a second caller arrives while the first is still
     * waiting for its answer. It is reached through a {@link Proxy} because the
     * wrapper resolves pgJDBC's own interface from its delegate, so the double
     * has to carry both.
     */
    private static final class Server {

        /** What {@code select version()} answers with. */
        private static final String VERSION = "CrateDB 6.4.1 (built 0a1b2c3/2026-01-01T00:00:00Z, "
                                              + "OpenJDK 64-Bit Server VM 24.0.2+12)";

        private final CountDownLatch aCallerIsInside = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final AtomicInteger queries = new AtomicInteger();

        private int queries() {
            return queries.get();
        }

        private void awaitACallerInside() throws InterruptedException {
            aCallerIsInside.await();
        }

        private void releaseTheCallersInside() {
            released.countDown();
        }

        /**
         * Waits for the second caller to have settled: either blocked out of
         * the connection while the first holds it, or through the check and at
         * the server on its own. Reading its state rather than sleeping is what
         * keeps a missing lock a failure in milliseconds instead of a wait long
         * enough to be sure.
         */
        private void awaitTheSecondCaller(Thread caller) {
            while (caller.getState() == Thread.State.NEW
                   || (caller.getState() == Thread.State.RUNNABLE && queries() < 2)) {
                Thread.onSpinWait();
            }
        }

        /** Every call the connection makes of the server costs a turn here. */
        private Object query(Object answer) throws InterruptedException {
            queries.incrementAndGet();
            aCallerIsInside.countDown();
            released.await();
            return answer;
        }

        private Connection connection() {
            return proxy(
                (proxy, called) -> {
                    switch (called) {
                        case "unwrap":
                            return proxy;
                        case "isClosed":
                            return false;
                        case "getMetaData":
                            return query(proxy((metaData, method) -> {
                                throw new UnsupportedOperationException(method);
                            }, DatabaseMetaData.class));
                        case "createStatement":
                            return statement();
                        default:
                            throw new UnsupportedOperationException(called);
                    }
                }, Connection.class, PGConnection.class);
        }

        private Statement statement() {
            return proxy((proxy, called) -> {
                switch (called) {
                    case "executeQuery":
                        return query(rows());
                    case "close":
                        return null;
                    default:
                        throw new UnsupportedOperationException(called);
                }
            }, Statement.class);
        }

        private ResultSet rows() {
            return proxy((proxy, called) -> {
                switch (called) {
                    case "next":
                        return true;
                    case "getString":
                        return VERSION;
                    case "close":
                        return null;
                    default:
                        throw new UnsupportedOperationException(called);
                }
            }, ResultSet.class);
        }

        @FunctionalInterface
        private interface Answers {
            Object to(Object proxy, String called) throws Exception;
        }

        @SuppressWarnings("unchecked")
        private static <T> T proxy(Answers answers, Class<?>... faces) {
            return (T) Proxy.newProxyInstance(CrateConnectionTest.class.getClassLoader(), faces,
                (proxy, method, arguments) -> answers.to(proxy, method.getName()));
        }
    }
}
