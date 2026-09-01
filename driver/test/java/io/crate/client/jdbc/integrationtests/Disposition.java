package io.crate.client.jdbc.integrationtests;

import java.util.Locale;

/**
 * What is to be done about a listed difference.
 *
 * <p>The kind of an entry says what the machine saw. This says what it means:
 * a difference this driver chose is a different thing from a fault it inherited
 * and has not reported, and until the two are told apart the delta is a list of
 * observations rather than a list of decisions. It is what makes the file
 * answerable — {@code grep -- '-todo'} is the queue of what upstream has not
 * been told.</p>
 *
 * <p>Where a fault has been reported, the entry carries the ticket. That number
 * earns its place twice: it says where the conversation is, and when the entry
 * goes stale under the staleness check it says that upstream shipped the fix,
 * which is the one thing a driver bump should announce and otherwise reads as
 * unexplained drift.</p>
 */
final class Disposition {

    /** Reported against this project, or {@code null} where there is nothing to report. */
    enum Upstream {
        PGJDBC("pgjdbc", "pgJDBC"),
        CRATE("crate", "CrateDB");

        private final String prefix;
        private final String named;

        Upstream(String prefix, String named) {
            this.prefix = prefix;
            this.named = named;
        }

        String named() {
            return named;
        }
    }

    /** A fault whose report is still owed. */
    private static final String OWED = "-todo";

    private final String token;
    private final Upstream upstream;
    private final boolean owed;
    private final boolean inherited;

    private Disposition(String token, Upstream upstream, boolean owed, boolean inherited) {
        this.token = token;
        this.upstream = upstream;
        this.owed = owed;
        this.inherited = inherited;
    }

    /**
     * The disposition a delta entry spells.
     *
     * <p>Seven settled forms, and two open ones. {@code by-design} is this
     * driver's own choice; {@code for-crate} is an accommodation of something
     * CrateDB does; {@code spec} is a reading JDBC leaves open, where neither
     * driver is wrong; {@code inherent} is a property of what is being measured
     * rather than a decision by anyone, which is what a value that differs per
     * connection is. {@code wont-file} is a fault inherited from pgJDBC that is
     * recorded rather than reported. {@code jdk} is a default method of
     * {@code java.sql} that neither driver overrides, so the behavior belongs to
     * the platform and there is no one to report it to. The open forms name a
     * project and either a ticket — {@code pgjdbc#3141} — or the fact that there
     * is none yet — {@code crate-todo}.</p>
     */
    static Disposition of(String token) {
        String spelled = token.trim().toLowerCase(Locale.ENGLISH);
        switch (spelled) {
            case "by-design":
            case "for-crate":
            case "spec":
            case "inherent":
                return new Disposition(spelled, null, false, false);
            case "wont-file":
                return new Disposition(spelled, Upstream.PGJDBC, false, true);
            case "jdk":
                // Inherited, because it reaches the caller through this driver without
                // being written here and so reproduces wherever the interface does; owed
                // to nobody, because java.sql defines it.
                return new Disposition(spelled, null, false, true);
            default:
                break;
        }
        for (Upstream upstream : Upstream.values()) {
            if (spelled.equals(upstream.prefix + OWED)) {
                return new Disposition(spelled, upstream, true, upstream == Upstream.PGJDBC);
            }
            if (spelled.startsWith(upstream.prefix + "#") && ticketed(spelled, upstream)) {
                return new Disposition(spelled, upstream, false, upstream == Upstream.PGJDBC);
            }
        }
        throw new IllegalArgumentException("No such disposition as '" + token + "'. It is one of "
            + "by-design, for-crate, spec, inherent, wont-file, jdk, pgjdbc-todo, crate-todo, "
            + "or a ticket like pgjdbc#3141");
    }

    private static boolean ticketed(String spelled, Upstream upstream) {
        String number = spelled.substring(upstream.prefix.length() + 1);
        if (number.isEmpty()) {
            return false;
        }
        for (int at = 0; at < number.length(); at++) {
            if (!Character.isDigit(number.charAt(at))) {
                return false;
            }
        }
        return true;
    }

    String token() {
        return token;
    }

    /**
     * Whether the behavior reaches the caller from below this driver rather than
     * being written here — pgJDBC's, or a default method {@code java.sql} supplies
     * to every driver that does not override it. Either way it is a behavior the
     * control cell meets against a server this driver was never involved with.
     */
    boolean inherited() {
        return inherited;
    }

    /** Whether a report is owed to whoever the behavior belongs to. */
    boolean owed() {
        return owed;
    }

    /** Who the behavior belongs to, or {@code null} where it belongs here. */
    Upstream upstream() {
        return upstream;
    }

    @Override
    public String toString() {
        return token;
    }
}
