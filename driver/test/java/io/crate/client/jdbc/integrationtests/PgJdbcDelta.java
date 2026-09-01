package io.crate.client.jdbc.integrationtests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The differences between this driver and stock pgJDBC that are there on
 * purpose, written down.
 *
 * <p>The driver's claim is "pgJDBC, plus a small set of deliberate
 * differences". Held against a sweep of both drivers on the same server, that
 * claim is an oracle: every call that answers differently must be named here
 * with a reason, and a call named here must still answer differently. Adding
 * a difference, removing one, or upgrading either driver into one all fail
 * until this file says what changed.</p>
 *
 * <p>Each entry also carries a {@link Disposition}, which is what separates a
 * difference this driver chose from a fault it inherited and has not reported.
 * Without it the file is a list of observations that cannot be asked what is
 * outstanding; with it, {@code grep -- '-todo'} is that answer.</p>
 */
final class PgJdbcDelta {

    static final String RESOURCE = "pgjdbc-delta.txt";

    /** What a listed call does that makes it belong in this file. */
    enum Kind {
        /** Answers differently from stock pgJDBC, on purpose. */
        DIFFERS,
        /** Answers differently from one run to the next, so nothing can be compared. */
        UNSTABLE,
        /** Raises something other than a {@link java.sql.SQLException}. */
        UNCHECKED,
        /** Contradicts itself the way pgJDBC does, having inherited the behavior. */
        INCONSISTENT,
        /** Answers on a closed object where JDBC has one refuse. */
        PERMISSIVE,
        /** Reports a SQLState that is not one, or not the one its class implies. */
        MALFORMED
    }

    /** One listed call: why it is listed, and what is to be done about it. */
    static final class Entry {

        private final String reason;
        private final Disposition disposition;

        private Entry(String reason, Disposition disposition) {
            this.reason = reason;
            this.disposition = disposition;
        }

        String reason() {
            return reason;
        }

        /** What is to be done, or {@code null} where the entry has not said. */
        Disposition disposition() {
            return disposition;
        }
    }

    private final Map<Kind, Map<String, Entry>> entries;

    private PgJdbcDelta(Map<Kind, Map<String, Entry>> entries) {
        this.entries = entries;
    }

    static PgJdbcDelta load() {
        return load(RESOURCE);
    }

    /**
     * The entries of one file. The sequence suites keep their own, because
     * what they name is a chain of calls rather than a single one and mixing
     * the two namespaces would leave neither readable.
     */
    static PgJdbcDelta load(String resource) {
        Map<Kind, Map<String, Entry>> entries = new LinkedHashMap<>();
        for (Kind kind : Kind.values()) {
            entries.put(kind, new LinkedHashMap<>());
        }
        try (InputStream source = PgJdbcDelta.class.getResourceAsStream(resource)) {
            if (source == null) {
                throw new IllegalStateException("The delta is missing from the test resources: " + resource);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(source, StandardCharsets.UTF_8));
            String line;
            int number = 0;
            while ((line = reader.readLine()) != null) {
                number++;
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                int separator = trimmed.indexOf("::");
                if (separator < 0) {
                    throw new IllegalStateException(
                        resource + ":" + number + " needs a '::' between the call and the reason: " + trimmed);
                }
                String[] head = trimmed.substring(0, separator).trim().split("\\s+", 2);
                if (head.length != 2) {
                    throw new IllegalStateException(
                        resource + ":" + number + " needs a kind and a call before the '::': " + trimmed);
                }
                Kind kind;
                try {
                    kind = Kind.valueOf(head[0].toUpperCase(java.util.Locale.ENGLISH));
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException(
                        resource + ":" + number + " has no such kind as '" + head[0] + "'", e);
                }
                String reason = trimmed.substring(separator + 2).trim();
                if (reason.isEmpty()) {
                    throw new IllegalStateException(resource + ":" + number + " has no reason");
                }
                String call = head[1];
                Disposition disposition = null;
                boolean applies = true;
                while (call.startsWith("[") || call.startsWith("{")) {
                    boolean braced = call.charAt(0) == '{';
                    char closing = braced ? '}' : ']';
                    int close = call.indexOf(closing);
                    if (close < 0) {
                        throw new IllegalStateException(resource + ":" + number
                            + " opens a " + (braced ? "disposition" : "scope")
                            + " it never closes: " + call);
                    }
                    String token = call.substring(1, close).trim();
                    if (braced) {
                        if (disposition != null) {
                            throw new IllegalStateException(
                                resource + ":" + number + " gives two dispositions");
                        }
                        try {
                            disposition = Disposition.of(token);
                        } catch (IllegalArgumentException e) {
                            throw new IllegalStateException(resource + ":" + number + " " + e.getMessage(), e);
                        }
                    } else {
                        applies &= inEffect(token);
                    }
                    call = call.substring(close + 1).trim();
                }
                if (!applies) {
                    continue;
                }
                if (entries.get(kind).put(call, new Entry(reason, disposition)) != null) {
                    throw new IllegalStateException(resource + ":" + number + " lists " + call + " twice");
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new PgJdbcDelta(entries);
    }

    /**
     * Whether the run this entry is scoped to is the run in hand.
     *
     * <p>An entry may be scoped to an arrangement of the wire it belongs to —
     * {@code differs [preferQueryMode=simple] ...} — because a difference the
     * simple protocol produces is not there under the extended one, and
     * listing it unscoped would leave the entry stale on every other run. The
     * scope is written as the property itself, so there is no second name for
     * an arrangement to be kept in step with.</p>
     *
     * <p>A scope reading {@code >=6.4} is a server instead. The driver serves
     * a range of CrateDB releases and the suites run against several of them,
     * so a difference a release introduced — or one it settled — belongs to
     * the releases that have it rather than to every run.</p>
     */
    static boolean inEffect(String scope) {
        String property = scope.trim();
        if (property.startsWith(">=")) {
            return serverFrom(property.substring(2).trim());
        }
        boolean whenAbsent = property.startsWith("!");
        if (whenAbsent) {
            property = property.substring(1).trim();
        }
        boolean set = false;
        for (String inEffect : System.getProperty("test.connection.properties", "").split("&")) {
            set |= inEffect.trim().equals(property);
        }
        return set != whenAbsent;
    }

    private static boolean serverFrom(String release) {
        int dot = release.indexOf('.');
        if (dot < 0) {
            throw new IllegalStateException(
                "A server scope names a major and a minor, as in >=6.4, not " + release);
        }
        try {
            return BaseIntegrationTest.serverAtLeast(
                Integer.parseInt(release.substring(0, dot).trim()),
                Integer.parseInt(release.substring(dot + 1).trim()));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                "A server scope names a major and a minor, as in >=6.4, not " + release, e);
        }
    }

    Set<String> listed(Kind kind) {
        return entries.get(kind).keySet();
    }

    String reason(Kind kind, String id) {
        Entry entry = entries.get(kind).get(id);
        return entry == null ? null : entry.reason();
    }

    Entry entry(Kind kind, String id) {
        return entries.get(kind).get(id);
    }

    /** Every entry of a kind, in the order the file gives them. */
    Map<String, Entry> all(Kind kind) {
        return entries.get(kind);
    }
}
