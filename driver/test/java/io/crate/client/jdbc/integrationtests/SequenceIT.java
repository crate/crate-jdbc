package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Sequences of JDBC calls, run against both drivers and compared step by step.
 *
 * <p>The sweep asks every method once, on an object built for that one call.
 * This asks what happens next: a cursor read after its statement closed, a
 * batch executed twice, a connection committed with rows outstanding. Those
 * are states no catalogue of single calls reaches, and there are far more
 * orders than anyone would write down, so the orders are generated.</p>
 *
 * <p>What a divergence is filed under is the chain of calls that produced the
 * object it happened on, with the names and the drawn arguments taken out —
 * {@code executeQuery -> close -> getString}. A program is one of many that
 * reach the same state, and naming the state rather than the program is what
 * lets an entry outlive the seed that found it.</p>
 */
@Tag("pgjdbc-types")
public class SequenceIT extends BaseIntegrationTest {

    static final String RESOURCE = "sequence-delta.txt";

    /**
     * What a run on a pull request draws. Enough that the pinned seed reaches
     * every state the delta names — an entry no program reaches is an entry
     * nothing is checking — and a deeper run asks for more.
     */
    private static final int DEFAULT_PROGRAMS = 150;

    private static int programsAsked() {
        String asked = System.getProperty("test.sequence.programs", "");
        return asked.trim().isEmpty() ? DEFAULT_PROGRAMS : Integer.parseInt(asked.trim());
    }

    /** How many programs are drawn from the seed, and how long each may be. */
    private static final int PROGRAMS = programsAsked();
    private static final int STEPS = 24;

    /**
     * Programs tried before a shrink gives up. Only ever spent on a run that
     * has already found something, so its cost is paid once per finding.
     */
    private static final int SHRINK_BUDGET = 200;

    private static PgJdbcDelta delta;
    private static String pgJdbcUrl;
    private static Connection admin;

    /**
     * The programs, and what each of them diverged on. Generated and run once:
     * every rule below reads the same programs, and running them again per
     * rule would cost as much as the whole suite.
     */
    private static Map<Program, String> divergences;

    @BeforeAll
    static void runThePrograms() throws Exception {
        dropAllUserTables();
        delta = PgJdbcDelta.load(RESOURCE);
        String url = connectionUrl();
        pgJdbcUrl = "jdbc:postgresql://" + url.substring(url.indexOf("://") + "://".length());
        admin = connect();
        Random random = new Random(DeviceSeed.value());
        ProgramRun.createTable(admin, CRATE_TABLE);
        ProgramRun.createTable(admin, PGJDBC_TABLE);
        divergences = new LinkedHashMap<>();
        for (int i = 0; i < PROGRAMS; i++) {
            Program program = Program.of(random, 1 + random.nextInt(STEPS));
            divergences.put(program, keyOf(program));
        }
    }

    @AfterAll
    static void closeAdmin() throws Exception {
        if (admin != null) {
            admin.close();
        }
        dropAllUserTables();
    }

    /**
     * Every generated program answers the same on both drivers, step for step.
     *
     * <p>A program that does not is shrunk to the shortest one that still
     * diverges the same way, and reported with the line that would accept it
     * and the command that runs it again.</p>
     */
    @Test
    public void everyProgramAnswersTheSameOnBothDrivers() throws Exception {
        Map<String, String> unlisted = new LinkedHashMap<>();
        int index = 0;
        for (Map.Entry<Program, String> ran : divergences.entrySet()) {
            String divergence = ran.getValue();
            int at = index++;
            if (divergence == null || delta.reason(PgJdbcDelta.Kind.DIFFERS, divergence) != null) {
                continue;
            }
            unlisted.put(divergence, report(shrink(ran.getKey(), divergence), divergence, at));
        }
        assertThat(String.join("\n", unlisted.values()), unlisted.keySet(), is(empty()));
    }

    /**
     * Every entry says something that is still true.
     *
     * <p>Only under the pinned seed: a run drawing its own programs need never
     * generate the one an entry describes, and a check that fails for not
     * having looked says nothing.</p>
     */
    @Test
    public void everyEntryInTheDeltaIsStillTrue() {
        assumeTrue(System.getProperty("test.seed", "").trim().isEmpty()
                && PROGRAMS >= DEFAULT_PROGRAMS,
            "a run drawing its own programs, or fewer of them, need not reach the ones the "
            + "delta describes");
        TreeSet<String> stale = new TreeSet<>(delta.listed(PgJdbcDelta.Kind.DIFFERS));
        divergences.values().stream().filter(java.util.Objects::nonNull).forEach(stale::remove);
        assertThat("The delta says these sequences answer differently on the two drivers, and the "
            + "programs drawn from the pinned seed no longer reach them:\n  "
            + String.join("\n  ", stale), stale, is(empty()));
    }

    /**
     * What a program diverges on, or null if it does not. A driver that
     * disagrees with itself is reported as diverging from nothing, since there
     * is no answer to hold the other one against.
     */
    private static String keyOf(Program program) throws Exception {
        List<String> crate = run(program, connectionUrl(), CRATE_TABLE);
        List<String> pgjdbc = run(program, pgJdbcUrl, PGJDBC_TABLE);
        int diverged = firstDifference(crate, pgjdbc);
        if (diverged < 0) {
            return null;
        }
        List<String> again = run(program, connectionUrl(), CRATE_TABLE);
        return firstDifference(crate, again) < 0 ? causalSlice(program, diverged) : null;
    }

    private static List<String> run(Program program, String url, String table) throws Exception {
        ProgramRun.resetTable(admin, table);
        return ProgramRun.of(program, url, table).trace();
    }

    private static int firstDifference(List<String> one, List<String> other) {
        for (int i = 0; i < Math.min(one.size(), other.size()); i++) {
            if (!one.get(i).equals(other.get(i))) {
                return i;
            }
        }
        return one.size() == other.size() ? -1 : Math.min(one.size(), other.size());
    }

    /**
     * The chain of calls that produced the object the divergent step was made
     * on, ending in that step. Receiver names and drawn arguments are left
     * out, so two programs reaching one state through the same calls file
     * under the same entry.
     */
    private static String causalSlice(Program program, int diverged) {
        List<String> chain = new ArrayList<>();
        String needed = program.steps().get(diverged).receiver();
        for (int i = diverged - 1; i >= 0 && needed != null; i--) {
            Program.Step step = program.steps().get(i);
            if (needed.equals(step.binds())) {
                chain.add(0, step.verb().name());
                needed = step.receiver();
            }
        }
        chain.add(program.steps().get(diverged).verb().name());
        return String.join(" -> ", chain);
    }

    /** The shortest program still diverging the same way. */
    private static Program shrink(Program program, String divergence) throws Exception {
        Program shortest = program;
        for (int spent = 0; spent < SHRINK_BUDGET; ) {
            boolean shorter = false;
            for (int i = shortest.steps().size() - 1; i >= 0 && spent < SHRINK_BUDGET; i--) {
                Program candidate = shortest.without(i);
                spent++;
                if (candidate.steps().size() < shortest.steps().size()
                    && divergence.equals(keyOf(candidate))) {
                    shortest = candidate;
                    shorter = true;
                    break;
                }
            }
            if (!shorter) {
                return shortest;
            }
        }
        return shortest;
    }

    /**
     * A table per driver, so the two share nothing. One each rather than one
     * per program: making a table costs more than every program that runs
     * against it, and each run puts the rows back before it starts.
     */
    private static final String CRATE_TABLE = "sequence_crate";
    private static final String PGJDBC_TABLE = "sequence_pgjdbc";

    /**
     * What a failing run has to say for itself: the program that failed, what
     * each driver answered at the step they parted, the line that would accept
     * it, and the command that runs it again.
     */
    private static String report(Program program, String divergence, int index) throws Exception {
        List<String> crate = run(program, connectionUrl(), CRATE_TABLE);
        List<String> pgjdbc = run(program, pgJdbcUrl, PGJDBC_TABLE);
        int diverged = firstDifference(crate, pgjdbc);
        StringBuilder report = new StringBuilder("\nProgram " + (index + 1) + " of " + PROGRAMS
            + " parts from stock pgJDBC at step " + (diverged + 1)
            + ", shrunk to " + program.steps().size() + " steps.\n");
        for (int i = 0; i < diverged && i < crate.size(); i++) {
            report.append(String.format("  %2d  %s%n", i + 1, crate.get(i)));
        }
        report.append(String.format("  %2d  %s%n", diverged + 1, program.steps().get(diverged)))
            .append("        crate://           ").append(answerOf(crate, diverged)).append('\n')
            .append("        jdbc:postgresql:// ").append(answerOf(pgjdbc, diverged)).append('\n')
            .append("\n  to accept it:  differs ").append(divergence).append(" :: <why>\n")
            .append("  to run it again: ./gradlew integrationTest --tests '*SequenceIT'")
            .append(" -PtestSeed=").append(DeviceSeed.value()).append('\n');
        return report.toString();
    }

    private static String answerOf(List<String> trace, int step) {
        if (step >= trace.size()) {
            return "nothing — the program ended here";
        }
        String line = trace.get(step);
        int answered = line.lastIndexOf(" -> ");
        return answered < 0 ? line : line.substring(answered + " -> ".length());
    }
}
