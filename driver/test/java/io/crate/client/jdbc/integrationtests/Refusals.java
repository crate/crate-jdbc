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
 * The values CrateDB will not hold, written down.
 *
 * <p>A value the round trip cannot carry by any route is the server's decision
 * rather than a gap in the driver, and the catalogue holds values at that edge
 * on purpose. Passing over them silently is what let CrateDB's narrowing of
 * {@code bigint} — both endpoints spent as null sentinels — sit unremarked in a
 * suite that stores {@code Long.MIN_VALUE} on every run.</p>
 *
 * <p>So they are listed instead, and the list is checked in both directions: a
 * value the server starts refusing fails until it is written down, and one it
 * stops refusing fails until the entry goes. What the server will not hold is
 * then a thing the suite knows rather than a thing it steps around.</p>
 */
final class Refusals {

    static final String RESOURCE = "refusals.txt";

    /** Why one value is refused, and what is to be done about it. */
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

        Disposition disposition() {
            return disposition;
        }
    }

    private final Map<String, Entry> entries;

    private Refusals(Map<String, Entry> entries) {
        this.entries = entries;
    }

    /**
     * The listed refusals, in the grammar the delta uses:
     * {@code {disposition} [scope] <type>, <sample> :: <why>}.
     */
    static Refusals load() {
        Map<String, Entry> entries = new LinkedHashMap<>();
        try (InputStream source = Refusals.class.getResourceAsStream(RESOURCE)) {
            if (source == null) {
                throw new IllegalStateException(
                    "The refusals are missing from the test resources: " + RESOURCE);
            }
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(source, StandardCharsets.UTF_8));
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
                    throw new IllegalStateException(RESOURCE + ":" + number
                        + " needs a '::' between the value and the reason: " + trimmed);
                }
                String reason = trimmed.substring(separator + 2).trim();
                if (reason.isEmpty()) {
                    throw new IllegalStateException(RESOURCE + ":" + number + " has no reason");
                }
                String value = trimmed.substring(0, separator).trim();
                Disposition disposition = null;
                boolean applies = true;
                while (value.startsWith("{") || value.startsWith("[")) {
                    boolean braced = value.charAt(0) == '{';
                    int close = value.indexOf(braced ? '}' : ']');
                    if (close < 0) {
                        throw new IllegalStateException(RESOURCE + ":" + number
                            + " opens a " + (braced ? "disposition" : "scope")
                            + " it never closes: " + value);
                    }
                    String token = value.substring(1, close).trim();
                    if (braced) {
                        try {
                            disposition = Disposition.of(token);
                        } catch (IllegalArgumentException e) {
                            throw new IllegalStateException(
                                RESOURCE + ":" + number + " " + e.getMessage(), e);
                        }
                    } else {
                        applies &= PgJdbcDelta.inEffect(token);
                    }
                    value = value.substring(close + 1).trim();
                }
                if (!applies) {
                    continue;
                }
                if (disposition == null) {
                    throw new IllegalStateException(RESOURCE + ":" + number
                        + " says what is refused and not what is to be done about it: " + value);
                }
                if (entries.put(value, new Entry(reason, disposition)) != null) {
                    throw new IllegalStateException(RESOURCE + ":" + number + " lists " + value + " twice");
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Refusals(entries);
    }

    /** How a value is named here: the type it belongs to, then the value itself. */
    static String name(String type, String sample) {
        return type + ", " + sample;
    }

    Set<String> listed() {
        return entries.keySet();
    }

    Entry entry(String value) {
        return entries.get(value);
    }
}
