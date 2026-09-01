package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Holds the driver to its own claim: pgJDBC, plus a small set of deliberate
 * differences. Where JDBC itself says what an answer must be, holds it to that
 * as well.
 *
 * <p>The claim is an oracle. Both drivers are pointed at the same CrateDB and
 * asked the same several thousand questions — every method of every JDBC
 * interface an application reaches, in every state {@link Posture} names — and
 * each answer is compared. What comes out is the delta between them, which
 * {@value PgJdbcDelta#RESOURCE} spells out call by call. Nothing in that
 * comparison says what any single method ought to return; the expected answer
 * is whatever pgJDBC gave, and the file records where CrateDB makes that
 * answer wrong.</p>
 *
 * <p>The rules that do not compare are here for what a comparison cannot
 * reach: two drivers can agree and both be wrong. A closed object that answers,
 * a SQLState no caller can look up and an exception chain that loops are all
 * things JDBC settles on its own, so they are checked against the
 * specification rather than against pgJDBC.</p>
 *
 * <p>The two URLs are deliberately left unequal. {@code PGDBNAME},
 * {@code loadBalanceHosts} and {@code assumeMinServerVersion} are defaults
 * this driver fills in, so they are part of what is being compared — passing
 * them to pgJDBC as well would configure the difference away.</p>
 */
@Tag("pgjdbc-types")
public class DifferentialIT extends BaseIntegrationTest {

    private static List<Invocation> surface;
    private static Sweep crate;
    private static Sweep crateAgain;
    private static Sweep pgjdbc;
    private static Sweep pgjdbcAgain;
    private static PgJdbcDelta delta;

    @BeforeAll
    static void sweepBothDrivers() throws Exception {
        dropAllUserTables();
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute(JdbcSurface.CREATE_PROBE_TABLE);
            for (String row : JdbcSurface.INSERT_PROBE_ROWS) {
                statement.execute(row);
            }
            statement.execute("refresh table " + JdbcSurface.PROBE_TABLE);
        }
        ensureYellow();

        delta = PgJdbcDelta.load();
        surface = JdbcSurface.invocations();
        // Each driver is swept twice, so that an answer which is merely
        // different from one moment to the next is told apart from one that is
        // different between drivers.
        crate = Sweep.of(connectionUrl(), surface);
        crateAgain = Sweep.of(connectionUrl(), surface);
        pgjdbc = Sweep.of(pgJdbcUrl(), surface);
        pgjdbcAgain = Sweep.of(pgJdbcUrl(), surface);

        answered(crate, "crate://");
        answered(crateAgain, "crate:// again");
        answered(pgjdbc, "jdbc:postgresql://");
        answered(pgjdbcAgain, "jdbc:postgresql:// again");

        List<String> unreachable = JdbcSurface.unreachable();
        System.out.println("Differential sweep: " + surface.size() + " calls reached, "
            + unreachable.size() + " left out for want of an argument to pass "
            + "(" + String.join(", ", unreachable) + ")");
    }

    /**
     * A sweep that mostly could not open a connection is not a delta.
     *
     * <p>Every call opens one of its own, so a server that goes away partway
     * leaves hundreds of calls holding a connection failure — which compares
     * unequal to whatever the other sweep got and reads as the drivers
     * disagreeing about hundreds of methods. Saying so here costs one pass over
     * the outcomes and turns a wall of findings back into the one fact behind
     * them.</p>
     */
    private static void answered(Sweep sweep, String named) {
        int refused = 0;
        for (Invocation invocation : surface) {
            String state = sweep.get(invocation.id()).sqlState();
            if (state != null && state.startsWith("08")) {
                refused++;
            }
        }
        if (refused > surface.size() / 20) {
            throw new IllegalStateException("The sweep of " + named + " could not reach the server: "
                + refused + " of " + surface.size() + " calls failed to connect. What that sweep "
                + "holds is the server going away, not a difference between drivers.");
        }
    }

    @AfterAll
    static void giveBackTheControlServer() {
        ControlCell.stop();
    }

    /**
     * What the ledger holds, said out loud on a run that passes.
     *
     * <p>A file of decisions that is only ever printed when one of them breaks
     * teaches nothing the rest of the time. This is where the shape of it is
     * visible: how much of the delta is this driver's own doing, how much is
     * inherited, and how much is owed to somebody upstream who has not been
     * told.</p>
     */
    @AfterAll
    static void census() {
        Map<PgJdbcDelta.Kind, Integer> byKind = new LinkedHashMap<>();
        Map<String, Integer> byDisposition = new LinkedHashMap<>();
        Map<Disposition.Upstream, Integer> owed = new LinkedHashMap<>();
        int entries = 0;
        for (PgJdbcDelta.Kind kind : PgJdbcDelta.Kind.values()) {
            Map<String, PgJdbcDelta.Entry> listed = delta.all(kind);
            byKind.put(kind, listed.size());
            entries += listed.size();
            for (PgJdbcDelta.Entry entry : listed.values()) {
                Disposition disposition = entry.disposition();
                String token = disposition == null ? "undeclared" : disposition.token();
                byDisposition.merge(token, 1, Integer::sum);
                if (disposition != null && disposition.owed()) {
                    owed.merge(disposition.upstream(), 1, Integer::sum);
                }
            }
        }
        StringBuilder census = new StringBuilder("Differential ledger against pgJDBC ")
            .append(pgJdbcVersion()).append(", ").append(entries)
            .append(" entries in effect for this run:")
            .append("\n  by kind         ").append(counted(byKind, k -> k.name().toLowerCase(java.util.Locale.ENGLISH)))
            .append("\n  by disposition  ").append(counted(byDisposition, token -> token));
        if (owed.isEmpty()) {
            census.append("\n  nothing is owed upstream");
        } else {
            census.append("\n  owed upstream   ").append(counted(owed, Disposition.Upstream::named))
                .append(" — `grep -- '-todo'` in ").append(PgJdbcDelta.RESOURCE).append(" lists them");
        }
        List<String> unwarranted = chosenWithoutOverriding();
        if (!unwarranted.isEmpty()) {
            census.append("\n  ").append(unwarranted.size())
                .append(" marked by-design name a method this driver does not override, so the")
                .append("\n  difference comes from somewhere else and the disposition may be wrong:");
            for (String id : unwarranted) {
                census.append("\n    ").append(id);
            }
        }
        System.out.println(census);
    }

    private static <K> String counted(Map<K, Integer> counts, java.util.function.Function<K, String> named) {
        List<Map.Entry<K, Integer>> ordered = new ArrayList<>(counts.entrySet());
        ordered.sort(Comparator.comparing(Map.Entry<K, Integer>::getValue).reversed());
        List<String> said = new ArrayList<>();
        for (Map.Entry<K, Integer> count : ordered) {
            if (count.getValue() > 0) {
                said.add(named.apply(count.getKey()) + " " + count.getValue());
            }
        }
        return String.join(", ", said);
    }

    private static String pgJdbcVersion() {
        Package pgjdbc = org.postgresql.Driver.class.getPackage();
        String version = pgjdbc == null ? null : pgjdbc.getImplementationVersion();
        return version == null ? "an unstated version" : version;
    }

    /**
     * Entries claiming the difference is this driver's choice, on a method this
     * driver never overrides.
     *
     * <p>A call pgJDBC reaches through untouched cannot be answering
     * differently because of a decision made here, so the disposition is
     * describing something else — the server, or a wrapper further down.
     * Reported rather than failed: the mapping from an interface to the class
     * that implements it is a convention, and a convention is too thin a thing
     * to break a build on.</p>
     */
    private static List<String> chosenWithoutOverriding() {
        List<String> unwarranted = new ArrayList<>();
        for (PgJdbcDelta.Kind kind : PgJdbcDelta.Kind.values()) {
            delta.all(kind).forEach((id, entry) -> {
                if (entry.disposition() == null || !"by-design".equals(entry.disposition().token())) {
                    return;
                }
                Boolean overridden = overrides(id);
                if (overridden != null && !overridden) {
                    unwarranted.add(id);
                }
            });
        }
        return unwarranted;
    }

    /** Whether this driver declares the named method, or null where that cannot be told. */
    private static Boolean overrides(String id) {
        int dot = id.indexOf('.');
        int open = id.indexOf('(');
        if (dot < 0 || open < dot) {
            return null;
        }
        String iface = id.substring(0, dot);
        String called = id.substring(dot + 1, open);
        String wrapper = "io.crate.client.jdbc.Crate"
            + (iface.startsWith("PG") ? iface.substring(2) : iface);
        Class<?> implementation;
        try {
            implementation = Class.forName(wrapper);
        } catch (ClassNotFoundException notThisOne) {
            return null;
        }
        // Up the wrappers as well: a method the driver settles is as often
        // declared on the forwarding class it extends as on the class named
        // after the interface.
        for (Class<?> layer = implementation; layer != null && layer != Object.class;
                layer = layer.getSuperclass()) {
            for (Method method : layer.getDeclaredMethods()) {
                if (method.getName().equals(called)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * A fault the delta hands to pgJDBC still happens where CrateDB is not
     * involved.
     *
     * <p>Saying a behavior is upstream's is a claim about pgJDBC, and pgJDBC is
     * not what the rest of this suite measures — every other rule watches it
     * through a server it was not written for. This puts the same questions to
     * it against the database it was written for, and holds the answer to the
     * two rules a change of substrate cannot move: what a closed object owes,
     * and what a SQLState has to be. A rendering is never compared, because two
     * servers with different catalogs, different column types and a different
     * name for json would differ in ways that say nothing about either
     * driver.</p>
     *
     * <p>Both directions matter. A fault reaching the control and named nowhere
     * is one this driver has not noticed inheriting; an entry claiming a fault
     * the control no longer meets has outlived it, and the prose saying both
     * drivers are affected has quietly become untrue.</p>
     */
    @Test
    public void everyInheritedFaultReproducesAgainstStockPostgres() throws Exception {
        assumeTrue(ControlCell.asked(),
            "the control starts a PostgreSQL of its own; -PtestControl=true asks for it");
        Set<String> namedAnywhere = inheritedUnder(PgJdbcDelta.Kind.values());
        List<Invocation> asked = worthAsking(namedAnywhere);
        Sweep control = ControlCell.sweep(asked);
        Set<String> reproduced = new TreeSet<>();
        Set<String> unanswered = new TreeSet<>();
        for (Invocation invocation : asked) {
            Outcome outcome = control.get(invocation.id());
            if (!outcome.reached()) {
                if (spoke(outcome)) {
                    unanswered.add(invocation.id());
                }
                continue;
            }
            boolean answeredWhenClosed = invocation.posture() == Posture.CLOSED
                && ClosedObjectContract.governs(invocation.jdbcInterface())
                && !ClosedObjectContract.isAnswered(invocation.method())
                && outcome.thrown() == null;
            boolean raisedTheWrongThing = raisedSomethingUnchecked(outcome);
            boolean statedNoCondition = outcome.thrown() instanceof SQLException
                && SqlStateContract.faultIn((SQLException) outcome.thrown()) != null;
            if (answeredWhenClosed || raisedTheWrongThing || statedNoCondition) {
                reproduced.add(invocation.id());
            }
        }

        // The other direction reads only the kinds that assert the fault is
        // there. A difference between two drivers and an answer that wanders
        // are about a comparison the control is not part of, so its silence
        // about one of those says nothing at all.
        Set<String> asserted = inheritedUnder(PgJdbcDelta.Kind.UNCHECKED,
            PgJdbcDelta.Kind.PERMISSIVE, PgJdbcDelta.Kind.MALFORMED);

        List<String> disagreed = new ArrayList<>();
        for (String id : reproduced) {
            if (!named(namedAnywhere, id)) {
                disagreed.add(id + "\n    stock pgJDBC does this against stock PostgreSQL, and the "
                    + "delta does not say the driver inherits it"
                    + "\n    postgres:17        " + indented(control.get(id).rendering()));
            }
        }
        for (String id : asserted) {
            if (unanswered.contains(id) || covers(unanswered, id)) {
                continue;
            }
            if (!named(reproduced, id) && !covers(reproduced, id)) {
                Outcome answered = control.get(id);
                disagreed.add(id + "\n    listed as pgJDBC's, and stock pgJDBC does not do it "
                    + "against stock PostgreSQL — so either the fault is this driver's own, or "
                    + "\n    it is a fault of pgJDBC's that CrateDB is what brings out"
                    + "\n    postgres:17        "
                    + (answered == null ? "not swept" : indented(answered.rendering())));
            }
        }
        // What it could not reach is said out loud. A cell that quietly had no
        // opinion about a third of the surface would pass exactly as loudly as
        // one that confirmed all of it.
        System.out.println("Control cell: " + reproduced.size() + " faults reproduced against "
            + "stock PostgreSQL, " + asserted.size() + " listed as pgJDBC's, "
            + unanswered.size() + " calls it could not reach");
        assertThat("The control could not reach " + unanswered.size() + " of " + asked.size()
            + " calls, which is too much of the surface for what it did reach to mean much. "
            + "PostgreSQL gives every connection a process and the sweep opens one per call.",
            unanswered.size() < asked.size() / 50, is(true));
        assertThat("\n  " + String.join("\n  ", disagreed), disagreed, is(empty()));
    }

    /**
     * The calls worth putting to the control, which is fewer than all of them.
     *
     * <p>Two sorts: every call on a closed object the closed-object rule
     * governs, which is where a fault of pgJDBC's would show up unlisted, and
     * every call the delta already lays at pgJDBC's door, so that each listed
     * entry is confirmed or denied. Nothing else can move the answer — a call
     * on an open object that both drivers answer alike says nothing about
     * whether a fault is upstream's.</p>
     *
     * <p>Asking less is also what makes the cell work at all. PostgreSQL gives
     * every connection a process, the sweep opens one per call, and several
     * thousand in a row is a rate the connection between the two cannot hold —
     * so a sweep of the whole surface comes back with a quarter of it
     * unanswered, which is worth less than a smaller sweep that answers.</p>
     */
    private static List<Invocation> worthAsking(Set<String> inherited) {
        List<Invocation> asked = new ArrayList<>();
        for (Invocation invocation : surface) {
            boolean onAClosedObject = invocation.posture() == Posture.CLOSED
                && ClosedObjectContract.governs(invocation.jdbcInterface());
            if (onAClosedObject || named(inherited, invocation.id())) {
                asked.add(invocation);
            }
        }
        return asked;
    }

    /**
     * Whether the control got far enough to have a view. A call it could not
     * open a connection for, or ran out of time on, tells nothing about pgJDBC
     * — reading that silence as the fault having gone would report the server
     * it was asked of rather than the driver it was asking about.
     */
    private static boolean spoke(Outcome outcome) {
        String state = outcome.sqlState();
        return outcome.timedOut() || (state != null && state.startsWith("08"));
    }

    private static Set<String> inheritedUnder(PgJdbcDelta.Kind... kinds) {
        Set<String> inherited = new TreeSet<>();
        for (PgJdbcDelta.Kind kind : kinds) {
            delta.all(kind).forEach((id, entry) -> {
                if (entry.disposition() != null && entry.disposition().inherited()) {
                    inherited.add(id);
                }
            });
        }
        return inherited;
    }

    /**
     * Whether a set names a call, by its own name or by the plainer one an
     * entry covering every state and every column goes under. The delta says a
     * fact about a method once and lets it stand for the states and columns
     * that method answers alike in, and a rule reading the delta has to read it
     * the same way.
     */
    private static boolean named(Set<String> ids, String id) {
        return ids.contains(id) || ids.contains(plainly(id));
    }

    /** Whether a set holds some state or column of a call named plainly. */
    private static boolean covers(Set<String> ids, String listed) {
        for (String id : ids) {
            if (plainly(id).equals(listed)) {
                return true;
            }
        }
        return false;
    }

    /** A call with the state it was in and the column it read taken off. */
    private static String plainly(String id) {
        int cut = id.length();
        for (char mark : new char[]{'@', '#'}) {
            int at = id.indexOf(mark);
            if (at >= 0 && at < cut) {
                cut = at;
            }
        }
        return id.substring(0, cut);
    }

    /**
     * Every entry says what is to be done about it.
     *
     * <p>Optional in the grammar so that a file can be migrated a section at a
     * time, and required here so that it is. An entry with no disposition is an
     * observation rather than a decision, and a file of those cannot be asked
     * what is outstanding.</p>
     */
    @Test
    public void everyEntryDeclaresADisposition() {
        List<String> undeclared = new ArrayList<>();
        for (String resource : List.of(PgJdbcDelta.RESOURCE, SequenceIT.RESOURCE)) {
            PgJdbcDelta listed = PgJdbcDelta.load(resource);
            for (PgJdbcDelta.Kind kind : PgJdbcDelta.Kind.values()) {
                listed.all(kind).forEach((id, entry) -> {
                    if (entry.disposition() == null) {
                        undeclared.add(resource + " " + kind.name().toLowerCase(java.util.Locale.ENGLISH)
                            + " " + id);
                    }
                });
            }
        }
        assertThat("These entries say what was seen and not what is to be done about it. Each "
            + "needs a disposition — by-design, for-crate, spec, inherent, wont-file, jdk, "
            + "pgjdbc-todo, crate-todo, or a ticket:\n  " + String.join("\n  ", undeclared),
            undeclared, is(empty()));
    }

    /**
     * The same URL as the rest of the suite, with the scheme swapped so stock
     * pgJDBC answers it. Everything else — host, port, path, user — is held
     * equal, because those are the caller's inputs rather than the driver's.
     */
    private static String pgJdbcUrl() {
        String url = connectionUrl();
        String withoutJdbc = url.startsWith("jdbc:") ? url.substring("jdbc:".length()) : url;
        if (!withoutJdbc.startsWith("crate://")) {
            throw new IllegalStateException(
                "The differential needs a crate:// URL to derive a pgJDBC one from, not " + url);
        }
        return "jdbc:postgresql://" + withoutJdbc.substring("crate://".length());
    }

    /**
     * The calls whose answer changed between two sweeps of the same driver.
     *
     * <p>A call that ran out of time in one sweep and answered in the other is
     * left out. What that pair disagrees about is how loaded the machine was,
     * and calling it an unsteady answer would put the load in the delta.</p>
     */
    private static Set<String> unstable() {
        Set<String> unstable = new LinkedHashSet<>();
        for (Invocation invocation : surface) {
            String id = invocation.id();
            if (wandered(crate.get(id), crateAgain.get(id))
                || wandered(pgjdbc.get(id), pgjdbcAgain.get(id))) {
                unstable.add(id);
            }
        }
        return unstable;
    }

    /**
     * Whether one driver gave two different answers to the same question.
     *
     * <p>A pair where one sweep ran out of time and the other answered is not
     * one of those. The same driver was asked the same thing twice, so what
     * that pair disagrees about is the machine underneath it, and reading it
     * as an unsteady answer would put the load of the run into the delta.</p>
     */
    private static boolean wandered(Outcome once, Outcome again) {
        return !once.timedOut() && !again.timedOut()
            && !once.rendering().equals(again.rendering());
    }

    /**
     * Whether the two drivers answered differently, with nothing passed over.
     *
     * <p>Running out of time is compared here rather than excused the way
     * {@link #wandered} excuses it. A call that blocks for the whole budget
     * against one driver while the other answers at once is as large a
     * difference as there is between them — the answer an application waits
     * forever for is the answer it never gets — and two calls that both ran
     * out of time carry the same rendering and so read as agreeing, which they
     * do.</p>
     */
    private static boolean disagreed(Outcome mine, Outcome reference) {
        return !mine.rendering().equals(reference.rendering());
    }

    /**
     * The calls the two drivers answer differently, instability aside.
     *
     * <p>A call the delta names unstable is left out whether or not this run's
     * two sweeps happened to agree. An answer that wanders — a row order the
     * query does not fix, which several nodes answering make visible — can
     * come out the same twice by chance and still differ between the drivers,
     * and calling that a difference would report the wandering rather than
     * anything about the drivers.</p>
     */
    private static Set<String> differences() {
        Set<String> unstable = unstable();
        Set<String> differences = new LinkedHashSet<>();
        for (Invocation invocation : surface) {
            String id = invocation.id();
            if (unstable.contains(id) || delta.reason(PgJdbcDelta.Kind.UNSTABLE, id) != null) {
                continue;
            }
            if (disagreed(crate.get(id), pgjdbc.get(id))) {
                differences.add(id);
            }
        }
        return differences;
    }

    @Test
    public void everyDifferenceFromStockPgJdbcIsInTheDelta() {
        List<String> unlisted = unlisted(PgJdbcDelta.Kind.DIFFERS, differences());
        assertThat(report("answer differently from stock pgJDBC and are not in the delta",
            PgJdbcDelta.Kind.DIFFERS, unlisted), unlisted, is(empty()));
    }

    /**
     * An entry earns its place by being true. One that has stopped being true
     * is worse than no entry: it excuses whatever moved into its place.
     *
     * <p>{@code unstable} is the one kind with no check in this direction.
     * Whether an answer is steady depends on the cluster it is asked of — a
     * query whose ordering is not total settles on one node and wanders on
     * three — so an entry that turns out steady here costs a little comparison
     * rather than hiding anything.</p>
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("kindsThatCanGoStale")
    public void everyEntryInTheDeltaIsStillTrue(PgJdbcDelta.Kind kind) {
        Set<String> stale = new TreeSet<>(delta.listed(kind));
        stale.removeAll(observed(kind));
        assertThat(
            "The delta says these calls are " + kind.name().toLowerCase(java.util.Locale.ENGLISH)
            + ", and they no longer are. Either the behavior was given up on and the entry "
            + "should go, or it moved and the entry should say where:\n  "
            + String.join("\n  ", stale),
            stale, is(empty()));
    }

    static Stream<PgJdbcDelta.Kind> kindsThatCanGoStale() {
        return Stream.of(PgJdbcDelta.Kind.DIFFERS, PgJdbcDelta.Kind.UNCHECKED,
            PgJdbcDelta.Kind.PERMISSIVE, PgJdbcDelta.Kind.MALFORMED);
    }

    /** The calls that are, right now, what a given kind of entry describes. */
    private static Set<String> observed(PgJdbcDelta.Kind kind) {
        if (kind == PgJdbcDelta.Kind.DIFFERS) {
            return differences();
        }
        if (kind == PgJdbcDelta.Kind.UNCHECKED) {
            return uncheckedFailures();
        }
        if (kind == PgJdbcDelta.Kind.PERMISSIVE) {
            return answersOnAClosedObject();
        }
        return malformedStates();
    }

    /**
     * A call that answers differently from one sweep to the next has nothing
     * the comparison can hold on to, and has to be named in the delta before
     * it is passed over. Only this direction is checked: whether an answer is
     * steady depends on the cluster it is asked of — a query whose ordering is
     * not total settles on one node and wanders on three — and an entry that
     * turns out steady costs a little comparison rather than hiding anything.
     */
    @Test
    public void everyCallAnswersTheSameTwice() {
        List<String> unlisted = unlisted(PgJdbcDelta.Kind.UNSTABLE, unstable());
        assertThat(report("answer differently from one sweep to the next and are not in the delta",
            PgJdbcDelta.Kind.UNSTABLE, unlisted), unlisted, is(empty()));
    }

    /**
     * A JDBC method reports failure as a {@link SQLException}. Anything else
     * escaping one reaches an application as a crash rather than as something
     * it can catch, whichever driver it came from.
     */
    @Test
    public void noCallRaisesSomethingOtherThanASqlException() {
        List<String> unlisted = unlisted(PgJdbcDelta.Kind.UNCHECKED, uncheckedFailures());
        assertThat(report("raise something other than a SQLException",
            PgJdbcDelta.Kind.UNCHECKED, unlisted), unlisted, is(empty()));
    }

    /**
     * The rule is about what a call raises, so it is asked only of calls that
     * were made. An object that could not be built and a call that never came
     * back both carry what went wrong, and neither is the probed method failing
     * to hold its end of anything.
     */
    private static Set<String> uncheckedFailures() {
        Set<String> unchecked = new LinkedHashSet<>();
        for (Invocation invocation : surface) {
            Outcome outcome = crate.get(invocation.id());
            if (outcome.reached() && raisedSomethingUnchecked(outcome)) {
                unchecked.add(invocation.id());
            }
        }
        return unchecked;
    }

    /**
     * Whether a call failed in a way its caller cannot catch: by raising
     * something that is not a {@link SQLException}, or by handing back a
     * result whose rows or column metadata raise one as they are read. The
     * second is the same fault one step further on — the method has returned
     * by then, so what escapes reaches the application from inside its own
     * loop over the result, where a {@code catch (SQLException)} around the
     * query does not stand.
     */
    private static boolean raisedSomethingUnchecked(Outcome outcome) {
        return (outcome.thrown() != null && !(outcome.thrown() instanceof SQLException))
            || !outcome.raisedWhileReading().isEmpty();
    }

    /**
     * JDBC has a closed object refuse every call but the few whose purpose is
     * to be asked afterwards. A comparison cannot reach this: both drivers
     * answering a result set that was closed agree, and an application reading
     * from it gets a value either way.
     *
     * <p>What is checked is that the call did not go through. A call that
     * refused with the wrong kind of failure is a fault of its own, and
     * {@link #noCallRaisesSomethingOtherThanASqlException} is where it is
     * named — listing it twice would have one entry excuse the other.</p>
     */
    @Test
    public void everyMethodOnAClosedObjectIsRefused() {
        List<String> unlisted = unlisted(PgJdbcDelta.Kind.PERMISSIVE, answersOnAClosedObject());
        assertThat(report("answer on a closed object instead of refusing",
            PgJdbcDelta.Kind.PERMISSIVE, unlisted), unlisted, is(empty()));
    }

    private static Set<String> answersOnAClosedObject() {
        Set<String> answered = new LinkedHashSet<>();
        for (Invocation invocation : surface) {
            if (invocation.posture() != Posture.CLOSED
                || !ClosedObjectContract.governs(invocation.jdbcInterface())
                || ClosedObjectContract.isAnswered(invocation.method())) {
                continue;
            }
            Outcome outcome = crate.get(invocation.id());
            if (outcome.reached() && outcome.thrown() == null) {
                answered.add(invocation.id());
            }
        }
        return answered;
    }

    /**
     * The one question a closed object exists to answer. Refusing it, or
     * saying no, leaves a caller no way to tell an object it may still use
     * from one it may not.
     */
    @Test
    public void aClosedObjectSaysItIsClosed() {
        List<String> wrong = new ArrayList<>();
        for (Invocation invocation : surface) {
            if (invocation.posture() != Posture.CLOSED
                || !ClosedObjectContract.governs(invocation.jdbcInterface())
                || !invocation.method().getName().equals("isClosed")
                || invocation.method().getParameterCount() != 0) {
                continue;
            }
            Outcome outcome = crate.get(invocation.id());
            if (outcome.reached() && !Boolean.TRUE.equals(outcome.answered(Boolean.class))) {
                wrong.add(invocation.id() + " answered " + outcome.rendering());
            }
        }
        assertThat("A closed object has to say so:\n  " + String.join("\n  ", wrong),
            wrong, is(empty()));
    }

    /**
     * The SQLState is the part of an error a program branches on, so it has to
     * be one — five characters naming a condition the standard defines — and
     * has to agree with the {@link SQLException} subclass carrying it.
     */
    @Test
    public void everySqlExceptionCarriesAConformingSqlState() {
        List<String> unlisted = unlisted(PgJdbcDelta.Kind.MALFORMED, malformedStates());
        assertThat(report("report a SQLState no caller can act on",
            PgJdbcDelta.Kind.MALFORMED, unlisted), unlisted, is(empty()));
    }

    private static Set<String> malformedStates() {
        Set<String> malformed = new LinkedHashSet<>();
        for (Invocation invocation : surface) {
            Throwable thrown = crate.get(invocation.id()).thrown();
            if (thrown instanceof SQLException
                && SqlStateContract.faultIn((SQLException) thrown) != null) {
                malformed.add(invocation.id());
            }
        }
        return malformed;
    }

    /**
     * A failure is reported as a chain — the next exception, and the cause of
     * each. Anything that walks one, a logger above all, walks it to the end,
     * so a link that leads back to a link already passed hangs whatever caught
     * it.
     */
    @Test
    public void everySqlExceptionChainTerminates() {
        List<String> looping = new ArrayList<>();
        for (Invocation invocation : surface) {
            Throwable thrown = crate.get(invocation.id()).thrown();
            if (thrown != null && !terminates(thrown)) {
                looping.add(invocation.id());
            }
        }
        assertThat("These calls raise an exception chain that leads back into itself:\n  "
            + String.join("\n  ", looping), looping, is(empty()));
    }

    private static boolean terminates(Throwable thrown) {
        Set<Throwable> seen = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable link = thrown; link != null; link = link.getCause()) {
            if (!seen.add(link)) {
                return false;
            }
            if (link instanceof SQLException) {
                for (SQLException next = ((SQLException) link).getNextException();
                        next != null; next = next.getNextException()) {
                    if (!seen.add(next)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /** The observed calls of a kind that the delta does not account for. */
    private static List<String> unlisted(PgJdbcDelta.Kind kind, Set<String> observed) {
        List<String> unlisted = new ArrayList<>();
        for (String id : observed) {
            if (!accountedFor(kind, id)) {
                unlisted.add(id);
            }
        }
        return unlisted;
    }

    /**
     * Whether the delta accounts for a call.
     *
     * <p>An entry that names no state accounts for that call in every state it
     * answers the same way in. Most of the delta is about what the driver is
     * rather than what it is doing — a product name, a limit, a capability it
     * denies — and those answer alike whether the object was just built or
     * long closed, so naming each state would say one thing five times. A
     * state in which either driver answers differently is a different fact and
     * carries its own entry.</p>
     */
    private static boolean accountedFor(PgJdbcDelta.Kind kind, String id) {
        if (delta.reason(kind, id) != null) {
            return true;
        }
        int state = id.indexOf('@');
        if (state < 0) {
            return false;
        }
        String inAnyState = id.substring(0, state);
        return delta.reason(kind, inAnyState) != null
            && crate.get(id).rendering().equals(crate.get(inAnyState).rendering())
            && pgjdbc.get(id).rendering().equals(pgjdbc.get(inAnyState).rendering());
    }

    /**
     * Which way a difference runs, which is what says how much it matters.
     *
     * <p>An answer where the reference refused is the one direction that can
     * hide a fault of this driver's: it went further than what it wraps, on a
     * call nothing else checks — {@link ClosedObjectContract} governs the JDBC
     * objects an application closes and not {@link java.sql.DatabaseMetaData},
     * so a metadata call answering past the connection is caught by nothing but
     * this. Two answers that merely differ come next, since a caller acts on
     * both. Refusing where the reference answered is a narrowing, which stops
     * an application rather than misleading it, and two refusals that differ
     * only in their wording are the least of it.</p>
     */
    private enum Direction {
        LAXER("answered where stock pgJDBC refused"),
        SILENT("answered differently"),
        STRICTER("refused where stock pgJDBC answered"),
        WORDED("refused differently");

        private final String said;

        Direction(String said) {
            this.said = said;
        }

        static Direction of(Outcome mine, Outcome reference) {
            boolean iRefused = mine.thrown() != null;
            boolean itRefused = reference.thrown() != null;
            if (iRefused == itRefused) {
                return iRefused ? WORDED : SILENT;
            }
            return iRefused ? STRICTER : LAXER;
        }
    }

    /**
     * Worst first. A call that raises what a caller cannot catch outranks one
     * that answers where it should refuse, which outranks a SQLState nobody can
     * branch on; a plain difference is last, and ordered among itself by which
     * way it runs.
     */
    private static List<String> worstFirst(PgJdbcDelta.Kind kind, List<String> ids) {
        List<String> ordered = new ArrayList<>(ids);
        if (kind == PgJdbcDelta.Kind.DIFFERS) {
            ordered.sort(Comparator.comparing(
                (String id) -> Direction.of(crate.get(id), pgjdbc.get(id))).thenComparing(id -> id));
        }
        return ordered;
    }

    /**
     * What a failing run has to say for itself: the calls at fault, which way
     * each runs, what each driver answered, and the line that would list one —
     * so that deciding between "this is deliberate" and "this is a bug" needs
     * nothing but the output.
     */
    private static String report(String complaint, PgJdbcDelta.Kind kind, List<String> ids) {
        if (ids.isEmpty()) {
            return "";
        }
        StringBuilder report = new StringBuilder(ids.size() + " calls " + complaint + ".\n");
        for (String id : worstFirst(kind, ids)) {
            report.append("\n").append(id);
            if (kind == PgJdbcDelta.Kind.DIFFERS) {
                report.append("  — ").append(Direction.of(crate.get(id), pgjdbc.get(id)).said);
            }
            report.append("\n  crate://           ").append(indented(crate.get(id).rendering()))
                .append("\n  jdbc:postgresql:// ").append(indented(pgjdbc.get(id).rendering()));
            if (kind == PgJdbcDelta.Kind.UNSTABLE) {
                report.append("\n  crate:// again     ").append(indented(crateAgain.get(id).rendering()))
                    .append("\n  postgresql:// again ").append(indented(pgjdbcAgain.get(id).rendering()));
            }
            if (kind == PgJdbcDelta.Kind.MALFORMED) {
                report.append("\n  the fault:         ")
                    .append(SqlStateContract.faultIn((SQLException) crate.get(id).thrown()));
            }
            report.append("\n  to accept it:      ")
                .append(kind.name().toLowerCase(java.util.Locale.ENGLISH))
                .append(" {disposition}").append(scopeToSuggest())
                .append(" ").append(id).append(" :: <why>");
        }
        return report.append('\n').toString();
    }

    /**
     * The scope an accepted entry would need, where this run is not the plain
     * one. A difference met under an arrangement of the wire the default run
     * does not use belongs to that arrangement, and an entry left unscoped
     * would go stale everywhere else — which is a failing build in the cell
     * that found nothing rather than in the cell that found this.
     */
    private static String scopeToSuggest() {
        String properties = System.getProperty("test.connection.properties", "").trim();
        if (properties.isEmpty()) {
            return "";
        }
        StringBuilder scope = new StringBuilder();
        for (String property : properties.split("&")) {
            if (!property.trim().isEmpty()) {
                scope.append(" [").append(property.trim()).append(']');
            }
        }
        return scope.toString();
    }

    private static String indented(String rendering) {
        return rendering.replace("\n", "\n     ");
    }
}
