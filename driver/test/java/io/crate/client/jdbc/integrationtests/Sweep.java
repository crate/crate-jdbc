package io.crate.client.jdbc.integrationtests;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One pass of the whole JDBC surface against one driver, and what each call
 * answered.
 *
 * <p>Calls run on a thread the sweep can walk away from. A driver is being
 * asked questions nobody wrote it to expect, and one of them blocking would
 * otherwise take the suite with it; a call that outstays its welcome is
 * recorded as such and the sweep moves on.</p>
 */
final class Sweep {

    /**
     * How long one call may take. Generous next to the milliseconds a local
     * server answers in, because a metadata query on a cold cluster is slow
     * without being stuck. Not more generous than that: a call that outstays
     * it is abandoned rather than interrupted, so its thread and its
     * connection are held for the rest of the run.
     */
    private static final long CALL_TIMEOUT_SECONDS = 20;

    private final String url;
    private final Map<String, Outcome> outcomes;

    private Sweep(String url, Map<String, Outcome> outcomes) {
        this.url = url;
        this.outcomes = outcomes;
    }

    static Sweep of(String url, List<Invocation> invocations) {
        Map<String, Outcome> outcomes = new LinkedHashMap<>();
        ExecutorService calls = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "jdbc-sweep");
            thread.setDaemon(true);
            return thread;
        });
        try {
            for (Invocation invocation : invocations) {
                Outcome outcome = run(calls, url, invocation);
                if (refusedTheConnection(outcome)) {
                    outcome = run(calls, url, invocation);
                }
                outcomes.put(invocation.id(), outcome);
            }
        } finally {
            calls.shutdownNow();
        }
        return new Sweep(url, outcomes);
    }

    /**
     * Whether the server would not take the connection this call needed.
     *
     * <p>Every call opens one of its own, and a server that gives each
     * connection a process of its own turns a sweep of several thousand into a
     * rate it briefly cannot meet. Nothing here is measuring what happens when
     * a connection cannot be opened — {@code FaultIT} is where that question
     * belongs — so the call is simply made again, and a server that is really
     * gone refuses the second one too.</p>
     */
    private static boolean refusedTheConnection(Outcome outcome) {
        String state = outcome.sqlState();
        return !outcome.reached() && state != null && state.startsWith("08");
    }

    private static Outcome run(ExecutorService calls, String url, Invocation invocation) {
        Future<Outcome> answer = calls.submit(() -> {
            try (Fixture fixture = Fixture.on(url, invocation.posture())) {
                return invocation.runOn(fixture);
            }
        });
        try {
            return answer.get(CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            answer.cancel(true);
            return Outcome.timedOut(e, CALL_TIMEOUT_SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted sweeping " + invocation.id(), e);
        } catch (java.util.concurrent.ExecutionException e) {
            return Outcome.raised(e.getCause());
        }
    }

    String url() {
        return url;
    }

    Map<String, Outcome> outcomes() {
        return outcomes;
    }

    Outcome get(String id) {
        return outcomes.get(id);
    }
}
