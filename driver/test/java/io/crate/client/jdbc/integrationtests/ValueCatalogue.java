package io.crate.client.jdbc.integrationtests;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * The values a round trip is asked to carry: for each CrateDB type, the ones
 * at the edges of what it holds, and a few drawn at random.
 *
 * <p>The edges are where a conversion stops being the identity. A number one
 * past the width it is carried in, a fraction that cannot be halved, a zero
 * with a sign, a string that is empty or that looks like something else —
 * these are the values a driver rounds, truncates or re-reads, and a
 * catalogue of ordinary ones never asks about them. What is written down here
 * is only which values to try; nothing says what any of them reads back as,
 * which is what lets the catalogue grow without anyone deciding an answer.</p>
 *
 * <p>The type list is written out rather than read from the server's catalog.
 * {@code pg_type} carries types no column can be made of, and a list that
 * changed underneath would change what the suite asks without anyone choosing
 * it; {@code everyTypeTheServerOffersIsInTheCatalogue} is what keeps the
 * written list honest.</p>
 */
final class ValueCatalogue {

    /** One value to carry, and how to spell it in a statement if it can be. */
    static final class Sample {

        private final String description;
        private final Object value;
        private final String literal;

        Sample(String description, Object value, String literal) {
            this.description = description;
            this.value = value;
            this.literal = literal;
        }

        String description() {
            return description;
        }

        Object value() {
            return value;
        }

        /** How the value is written into a statement, or null where it has no plain spelling. */
        String literal() {
            return literal;
        }
    }

    /** One CrateDB type, and the values to carry through it. */
    static final class Type {

        private final String description;
        private final String crateType;
        private final int major;
        private final int minor;
        private final List<Sample> samples;

        Type(String description, String crateType, int major, int minor, List<Sample> samples) {
            this.description = description;
            this.crateType = crateType;
            this.major = major;
            this.minor = minor;
            this.samples = samples;
        }

        String description() {
            return description;
        }

        String crateType() {
            return crateType;
        }

        int major() {
            return major;
        }

        int minor() {
            return minor;
        }

        List<Sample> samples() {
            return samples;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private ValueCatalogue() {
    }

    /** Every type the round trip carries, with the values to carry through it. */
    static List<Type> types() {
        Random draws = new Random(DeviceSeed.value());
        List<Type> types = new ArrayList<>();
        types.add(new Type("a boolean", "boolean", 0, 0, List.of(
            sample("true", true, "true"),
            sample("false", false, "false"))));
        types.add(new Type("a byte", "byte", 0, 0, whole(
            Byte.MIN_VALUE, (byte) -1, (byte) 0, (byte) 1, Byte.MAX_VALUE,
            (byte) draws.nextInt())));
        types.add(new Type("a short", "short", 0, 0, whole(
            Short.MIN_VALUE, (short) -1, (short) 0, (short) 1, Short.MAX_VALUE,
            (short) draws.nextInt())));
        types.add(new Type("an integer", "integer", 0, 0, whole(
            Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE, draws.nextInt())));
        // Two either side of where a double stops counting whole numbers one
        // at a time, which is the width json carries a number in.
        types.add(new Type("a long", "bigint", 0, 0, whole(
            Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE,
            1L << 53, (1L << 53) + 1, draws.nextLong())));
        types.add(new Type("a float", "real", 0, 0, List.of(
            sample("zero", 0.0f, "0.0"),
            sample("negative zero", -0.0f, "-0.0"),
            sample("a third", 1.0f / 3, null),
            sample("the smallest", Float.MIN_VALUE, null),
            sample("the largest", Float.MAX_VALUE, null),
            sample("the most negative", -Float.MAX_VALUE, null),
            sample("a draw", draws.nextFloat(), null))));
        types.add(new Type("a double", "double precision", 0, 0, List.of(
            sample("zero", 0.0d, "0.0"),
            sample("negative zero", -0.0d, "-0.0"),
            sample("a third", 1.0d / 3, null),
            sample("the smallest normal", Double.MIN_NORMAL, null),
            sample("the largest", Double.MAX_VALUE, null),
            // Halfway between two floats, so a double narrowed to one and
            // widened back is a different number.
            sample("more precision than a float holds", 0.1d + 0.2d, null),
            sample("a draw", draws.nextDouble(), null))));
        types.add(new Type("a string", "text", 0, 0, text(draws)));
        types.add(new Type("a fixed-width string", "character(8)", 0, 0, List.of(
            sample("shorter than the width", "ab", "'ab'"),
            sample("exactly the width", "abcdefgh", "'abcdefgh'"),
            sample("empty", "", "''"))));
        types.add(new Type("an ip address", "ip", 0, 0, List.of(
            sample("the unspecified address", "0.0.0.0", "'0.0.0.0'"),
            sample("the broadcast address", "255.255.255.255", "'255.255.255.255'"),
            sample("a loopback address", "127.0.0.1", "'127.0.0.1'"),
            sample("a v6 loopback address", "::1", "'::1'"),
            sample("a v6 address", "2001:db8::8a2e:370:7334", "'2001:db8::8a2e:370:7334'"))));
        types.add(new Type("a decimal", "numeric(10, 2)", 0, 0, List.of(
            sample("zero", new BigDecimal("0.00"), "0.00"),
            sample("a negative zero", new BigDecimal("-0.00"), "-0.00"),
            sample("an everyday amount", new BigDecimal("12.34"), "12.34"),
            sample("the widest that fits", new BigDecimal("99999999.99"), "99999999.99"),
            sample("the most negative that fits", new BigDecimal("-99999999.99"), "-99999999.99"))));
        types.add(new Type("a moment", "timestamp with time zone", 0, 0, moments()));
        types.add(new Type("a wall clock", "timestamp without time zone", 6, 4, List.of(
            sample("the epoch", Timestamp.valueOf("1970-01-01 00:00:00"), "'1970-01-01T00:00:00'"),
            sample("a spring-forward hour", Timestamp.valueOf("2026-03-29 02:30:00"),
                "'2026-03-29T02:30:00'"),
            sample("before the epoch", Timestamp.valueOf("1900-01-01 12:00:00"),
                "'1900-01-01T12:00:00'"))));
        types.add(new Type("a unique identifier", "uuid", 6, 2, List.of(
            sample("an everyday one", UUID.fromString("55d07626-4927-47c5-ba43-a015c23632ef"),
                "'55d07626-4927-47c5-ba43-a015c23632ef'::uuid"),
            sample("all zeroes", new UUID(0L, 0L), "'00000000-0000-0000-0000-000000000000'::uuid"),
            sample("all ones", new UUID(-1L, -1L), "'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid"))));
        types.add(new Type("a geographic point", "geo_point", 0, 0, List.of(
            sample("a place", new double[]{9.7419021, 47.4048045}, "[9.7419021, 47.4048045]"),
            sample("the origin", new double[]{0.0, 0.0}, "[0.0, 0.0]"),
            sample("the corner of the world", new double[]{-180.0, -90.0}, "[-180.0, -90.0]"))));
        return types;
    }

    /** Every whole number to carry, each spelled the way SQL spells one. */
    private static List<Sample> whole(Number... values) {
        List<Sample> samples = new ArrayList<>(values.length);
        for (Number value : values) {
            samples.add(sample(String.valueOf(value), value, String.valueOf(value)));
        }
        return samples;
    }

    /**
     * Strings that are something else as well: empty, made of the characters
     * an array literal and a json document are punctuated with, outside the
     * plane a Java char covers, or shaped like the json an OBJECT column
     * carries.
     */
    private static List<Sample> text(Random draws) {
        StringBuilder drawn = new StringBuilder();
        for (int i = 0; i < 16; i++) {
            drawn.append((char) (' ' + draws.nextInt('℀' - ' ')));
        }
        List<Sample> samples = new ArrayList<>();
        samples.add(sample("empty", "", "''"));
        samples.add(sample("a space", " ", "' '"));
        samples.add(sample("an everyday word", "hello", "'hello'"));
        samples.add(sample("an apostrophe", "it's", "'it''s'"));
        samples.add(sample("a backslash", "a\\b", "'a\\b'"));
        samples.add(sample("the punctuation of an array literal", "{a,\"b\"}", "'{a,\"b\"}'"));
        samples.add(sample("something shaped like json", "{\"a\": 1}", "'{\"a\": 1}'"));
        samples.add(sample("accents and other scripts", "héllo·日本", "'héllo·日本'"));
        samples.add(sample("a character outside the basic plane", "𝔘𝔫𝔦", "'𝔘𝔫𝔦'"));
        samples.add(sample("a newline", "a\nb", null));
        samples.add(sample("longer than a wire buffer", repeat('x', 100_000), null));
        samples.add(sample("a draw", drawn.toString(), null));
        return samples;
    }

    /**
     * Moments at the edges of the calendar and either side of a daylight-saving
     * change, where a conversion through the default zone parts company with
     * one that does not.
     */
    private static List<Sample> moments() {
        return List.of(
            moment("the epoch", 0L),
            moment("a millisecond before the epoch", -1L),
            moment("well before the epoch", -2_208_988_800_000L),
            moment("a spring-forward instant", Instant.parse("2026-03-29T01:00:00Z").toEpochMilli()),
            moment("a fall-back instant", Instant.parse("2026-10-25T01:00:00Z").toEpochMilli()),
            moment("far in the future", 253_402_300_799_000L));
    }

    /**
     * One instant, as the {@link Timestamp} a caller binds and as the epoch
     * milliseconds CrateDB reads a bare number as. The spelling is taken from
     * the value rather than written beside it, so the two cannot drift apart
     * and name different moments.
     */
    private static Sample moment(String description, long epochMillis) {
        return sample(description, new Timestamp(epochMillis), String.valueOf(epochMillis));
    }

    private static String repeat(char character, int length) {
        char[] characters = new char[length];
        java.util.Arrays.fill(characters, character);
        return new String(characters);
    }

    private static Sample sample(String description, Object value, String literal) {
        return new Sample(description, value, literal);
    }
}
