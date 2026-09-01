package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A value put into CrateDB and read back out, by every route it can take.
 *
 * <p>No expected value is written down anywhere here. What is asserted is that
 * the routes agree: a number stored in a column of its own and the same number
 * stored as an element of an array is one value, and reading it back both ways
 * has to say so. The same holds for the ways of writing it — as a parameter or
 * as a literal, on its own or in a batch — and for the accessors that read it.
 * That is what lets this catch what nobody thought to assert.</p>
 *
 * <p>The routes fall into two groups, and only within a group do they have to
 * agree. A column and an array element are typed by the server. An OBJECT
 * member and an element of a nested array travel as json, which carries fewer
 * types than Java does, so a whole number comes back a {@code Long} and a
 * fraction a {@code Double} whatever went in — a documented consequence of how
 * CrateDB sends an OBJECT, not a driver deciding differently in two places.
 * Holding the groups apart is what keeps the rule sharp: within a group, any
 * disagreement at all is a fault.</p>
 */
public class ValueRoundTripIT extends BaseIntegrationTest {

    private static final String TABLE = "round_trip";

    /** The instant every moment in the catalogue names. */
    private static final long EPOCH_MILLIS = 1_000_000L;

    private static final String SCALAR = "a column of its own";
    private static final String ARRAY = "an element of an array";
    private static final String MEMBER = "a member of an OBJECT";
    private static final String NESTED = "an element of a nested array";

    /** The routes the server types, which have to agree exactly. */
    private static final List<String> TYPED = List.of(SCALAR, ARRAY);

    /** The routes that travel as json, which have to agree exactly with each other. */
    private static final List<String> AS_JSON = List.of(MEMBER, NESTED);

    /**
     * Values this driver cannot carry by a route it carries them by otherwise,
     * keyed by the case and the route. Each is a gap the round trip found, and
     * each has a way around it; listing one keeps the suite reporting the rest
     * rather than stopping at the first, and a gap that closes fails here until
     * its entry goes.
     */
    private static final Map<String, String> KNOWN_GAPS = new LinkedHashMap<>();

    static {
        for (String place : List.of("a place", "the origin", "the corner of the world")) {
            KNOWN_GAPS.put("a geographic point, " + place + " / " + ARRAY,
                "CrateDB takes no array of numbers for a geo_point array — it refuses the "
                + "conversion from double precision_array, whichever driver sends it. A series of "
                + "WKT strings is the form it does take, and a column of nested arrays takes the "
                + "pairs as json.");
        }
    }

    /**
     * Routes of one kind that answer with different classes, keyed by the case
     * and the kind. Stock pgJDBC answers the same way, so these are inherited
     * rather than decided here; they are listed because an application that
     * casts what {@code getObject} returns meets them as a
     * {@code ClassCastException} and has no warning otherwise.
     */
    private static final Map<String, String> KNOWN_DISAGREEMENTS = new LinkedHashMap<>();

    static {
        for (String place : List.of("a place", "the origin", "the corner of the world")) {
            KNOWN_DISAGREEMENTS.put(
                "a geographic point, " + place + " / the routes that travel as json",
                "an array inside json reads as a List through a Map and as an Object[] through an "
                + "Array, so a value that is itself an array — which a geo_point is — arrives in "
                + "two shapes. Code walking a nested structure meets both at the same depth.");
        }
    }

    /**
     * Whether a pair of answers is pgJDBC reading {@code int2} two ways: an
     * {@code Integer} from a column and a {@code Short} from an array element,
     * for one and the same value.
     *
     * <p>Recognised by its shape rather than listed by the type that meets it,
     * because which CrateDB type the server sends as {@code int2} is the
     * server's to change — {@code short} always, {@code byte} from the release
     * that stopped sending it as {@code char}.</p>
     */
    private static boolean isPgJdbcReadingInt2BothWays(Collection<String> answers) {
        if (answers.size() != 2) {
            return false;
        }
        String asInteger = null;
        String asShort = null;
        for (String answer : answers) {
            if (answer.startsWith("Integer(")) {
                asInteger = answer.substring("Integer(".length());
            } else if (answer.startsWith("Short(")) {
                asShort = answer.substring("Short(".length());
            }
        }
        return asInteger != null && asInteger.equals(asShort);
    }

    /**
     * Whether a pair of answers is CrateDB sending an array without the
     * escaping an array literal calls for.
     *
     * <p>A backslash or a quote inside an element has to be doubled on the
     * way out; CrateDB sends it as it stands, so the text names a different
     * value from the one the column holds — selecting the element by index
     * gives back what was stored, and reading the array parses those
     * characters as the escapes the format says they are. It is recognised by
     * that relation rather than by the values that meet it, because every
     * text carrying either character meets it, and nothing this driver can
     * see tells the two spellings apart. Stock pgJDBC parses the same text
     * the same way.</p>
     *
     * <p>The escaping is the server's to do, and it does it for one element
     * type: a json element has had its quotes and backslashes escaped since
     * the encoder was written for arrays of geo_shape, and the same fault was
     * fixed there again in 2020 after a client could not read such an array
     * back. Every other element type still goes out unescaped, so this excuse
     * is owed to CrateDB and should go when that is reported and fixed.</p>
     */
    private static boolean isCrateDbSendingAnArrayUnescaped(Collection<String> answers) {
        if (answers.size() != 2) {
            return false;
        }
        java.util.Iterator<String> reading = answers.iterator();
        String one = reading.next();
        String other = reading.next();
        return withoutEscapes(one).equals(other) || withoutEscapes(other).equals(one);
    }

    /**
     * Whether a pair of answers is a wall clock bound as though it named an
     * instant, which is what binding a series of them does.
     *
     * <p>A collection carries no type, and the column it lands in is not known
     * where the values are converted, so a series of moments is written as the
     * instants they name — the one reading that means the same thing wherever
     * the JVM stands. A column holding wall clocks then stores the reading at
     * UTC rather than the local one a single value would have given it, which
     * is documented, and which {@code createArrayOf} naming the column's type
     * is the way around.</p>
     *
     * <p>Recognised by the two answers standing exactly the JVM's offset
     * apart, so that at offset zero — where they coincide — there is nothing
     * to excuse.</p>
     */
    private static boolean isAWallClockBoundAsAnInstant(String type, Collection<String> answers) {
        if (!type.equals("a wall clock") || answers.size() != 2) {
            return false;
        }
        java.util.Iterator<String> reading = answers.iterator();
        Timestamp one = timestampIn(reading.next());
        Timestamp other = timestampIn(reading.next());
        if (one == null || other == null) {
            return false;
        }
        // Either instant's offset: across a daylight-saving change the two
        // readings sit on opposite sides of it and only one names the gap.
        long apart = Math.abs(one.getTime() - other.getTime());
        return apart == Math.abs(TimeZone.getDefault().getOffset(one.getTime()))
            || apart == Math.abs(TimeZone.getDefault().getOffset(other.getTime()));
    }

    private static Timestamp timestampIn(String answer) {
        if (!answer.startsWith("Timestamp(") || !answer.endsWith(")")) {
            return null;
        }
        return Timestamp.valueOf(answer.substring("Timestamp(".length(), answer.length() - 1));
    }

    private static String withoutEscapes(String answer) {
        return answer.replace("\\", "").replace("\"", "");
    }

    @AfterAll
    static void dropTable() throws Exception {
        dropAllUserTables();
    }

    /**
     * The four places a value of one type can be stored, and what it reads back
     * as from each. Each is written on its own, so that a route the driver
     * cannot carry the value by leaves the others still measured.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("types")
    public void aValueReadsBackTheSameThroughEveryRouteOfAKind(ValueCatalogue.Type type)
            throws Exception {
        for (Map.Entry<String, Map<String, String>> sample : carry(type).entrySet()) {
            String description = sample.getKey();
            Map<String, String> byRoute = sample.getValue();
            assertGapsAreTheKnownOnes(type.description(), description, byRoute);
            agreeUnlessKnownNotTo(type.description(), description,
                "the routes the server types", byRoute, TYPED);
            agreeUnlessKnownNotTo(type.description(), description,
                "the routes that travel as json", byRoute, AS_JSON);
        }
    }

    /**
     * Every value the server will not hold at all is one this suite has written
     * down, and every one it has written down the server still will not hold.
     *
     * <p>A value refused by every route is the server's decision, and the
     * catalogue reaches for that edge deliberately. Stepping over it silently
     * is how CrateDB narrowing {@code bigint} by two — spending both endpoints
     * as null sentinels — went unremarked in a suite that stores
     * {@link Long#MIN_VALUE} on every run. Listed instead, and checked both
     * ways, what the server refuses becomes something the suite knows.</p>
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("types")
    public void everyValueTheServerRefusesIsWrittenDown(ValueCatalogue.Type type) throws Exception {
        assumeTrue(serverAtLeast(type.major(), type.minor()),
            type.crateType() + " needs a later CrateDB");
        Refusals refusals = Refusals.load();
        List<String> unexpected = new ArrayList<>();
        Set<String> stillRefused = new java.util.LinkedHashSet<>();
        for (Map.Entry<String, Map<String, String>> sample : carry(type).entrySet()) {
            String named = Refusals.name(type.description(), sample.getKey());
            boolean everywhere = sample.getValue().values().stream()
                .allMatch(answer -> answer.startsWith("raised"));
            boolean listed = refusals.entry(named) != null;
            if (everywhere) {
                stillRefused.add(named);
                if (!listed) {
                    unexpected.add(named + " is refused by every route, and is not written down."
                        + "\n    to accept it:  {disposition} " + named + " :: <why>"
                        + "\n    the server said: " + sample.getValue().values().iterator().next());
                }
            }
        }
        for (String named : refusals.listed()) {
            if (named.startsWith(type.description() + ", ") && !stillRefused.contains(named)) {
                unexpected.add(named + " is written down as refused, and the server now takes it,"
                    + " so its entry in " + Refusals.RESOURCE + " should go.");
            }
        }
        assertThat("\n  " + String.join("\n  ", unexpected), unexpected, is(empty()));
    }

    /**
     * A value the server will not hold is refused by every route, and refused
     * as a {@link SQLException}. A conversion that gives up part way through —
     * on the width of a number, on the class of an element — reaches a caller
     * as whatever the conversion threw, which is not something a caller can
     * catch by contract.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("types")
    public void everyRefusalIsASqlExceptionOnEveryRoute(ValueCatalogue.Type type) throws Exception {
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> sample : carry(type).entrySet()) {
            sample.getValue().forEach((route, answer) -> {
                if (answer.startsWith("raised") && !answer.startsWith("raised SQLException/")) {
                    wrong.add(type.description() + ", " + sample.getKey() + ", as " + route
                        + " -> " + answer);
                }
            });
        }
        assertThat("A value that cannot be carried has to be refused as a SQLException:\n  "
            + String.join("\n  ", wrong), wrong, is(empty()));
    }

    /**
     * A column holding nothing reads back as nothing, by every route. Null is
     * the one value every type has, and the routes carry it through four
     * different shapes — a column, a member of an object, an element of an
     * array, an element of an array inside one — each of which has its own way
     * of losing it.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("types")
    public void aNullReadsBackAsNullThroughEveryRoute(ValueCatalogue.Type type) throws Exception {
        assumeTrue(serverAtLeast(type.major(), type.minor()),
            type.crateType() + " needs a later CrateDB");
        Map<String, String> byRoute;
        try (Connection conn = connect()) {
            createTable(conn, type.crateType());
            byRoute = storeNullEveryWay(conn);
        }
        List<String> wrong = new ArrayList<>();
        byRoute.forEach((route, answer) -> {
            if (!answer.equals("null")) {
                wrong.add(route + " -> " + answer);
            }
        });
        assertThat("A null " + type.description() + " did not read back as one:\n  "
            + String.join("\n  ", wrong), wrong, is(empty()));
    }

    /**
     * Every sample of one type, stored by every route and read back.
     *
     * <p>One table, one row per value per route, one refresh, one query. Each
     * of those costs more than the value that goes through it, so doing them
     * once for a type rather than once for a value is what lets the catalogue
     * hold as many values as it is worth holding.</p>
     */
    private static Map<String, Map<String, String>> carry(ValueCatalogue.Type type) throws Exception {
        assumeTrue(serverAtLeast(type.major(), type.minor()),
            type.crateType() + " needs a later CrateDB");
        Map<String, Map<String, String>> bySample = new LinkedHashMap<>();
        Map<Integer, String> sampleOfRow = new LinkedHashMap<>();
        Map<Integer, Route> routeOfRow = new LinkedHashMap<>();
        try (Connection conn = connect()) {
            createTable(conn, type.crateType());
            int id = 0;
            for (ValueCatalogue.Sample sample : type.samples()) {
                Map<String, String> byRoute = new LinkedHashMap<>();
                bySample.put(sample.description(), byRoute);
                for (Route route : ROUTES) {
                    id++;
                    try {
                        store(conn, id, route.column, route.shape.of(sample.value()));
                        sampleOfRow.put(id, sample.description());
                        routeOfRow.put(id, route);
                    } catch (SQLException | RuntimeException refused) {
                        byRoute.put(route.label, refusal(refused));
                    }
                }
            }
            refresh(conn);
            try (Statement statement = conn.createStatement();
                 ResultSet rows = statement.executeQuery(
                     "select id, scalar, member, list, nested from " + TABLE + " order by id")) {
                while (rows.next()) {
                    Route route = routeOfRow.get(rows.getInt("id"));
                    bySample.get(sampleOfRow.get(rows.getInt("id")))
                        .put(route.label, describe(() -> route.read.from(rows)));
                }
            }
        }
        return bySample;
    }

    /** A null stored by each route in a row of its own, and read back. */
    private static Map<String, String> storeNullEveryWay(Connection conn) throws SQLException {
        Map<String, String> byRoute = new LinkedHashMap<>();
        int id = 0;
        for (Route route : ROUTES) {
            store(conn, ++id, route.column, route.shape.of(null));
        }
        refresh(conn);
        id = 0;
        for (Route route : ROUTES) {
            final int row = ++id;
            byRoute.put(route.label, read(conn, row, route.read));
        }
        return byRoute;
    }

    /** One of the four places a value of a type can be stored. */
    private static final class Route {

        private final String label;
        private final String column;
        private final Shape shape;
        private final Read read;

        Route(String label, String column, Shape shape, Read read) {
            this.label = label;
            this.column = column;
            this.shape = shape;
            this.read = read;
        }
    }

    @FunctionalInterface
    private interface Shape {
        Object of(Object value);
    }

    private static final List<Route> ROUTES = List.of(
        new Route(SCALAR, "scalar", value -> value, row -> row.getObject("scalar")),
        new Route(MEMBER, "member", ValueRoundTripIT::asMember,
            row -> memberOf(row.getObject("member"))),
        new Route(ARRAY, "list", Collections::singletonList,
            row -> elementOf(row.getArray("list"))),
        new Route(NESTED, "nested", value -> Collections.singletonList(Collections.singletonList(value)),
            row -> elementOf(elementOf(row.getArray("nested")))));

    private static Map<String, Object> asMember(Object value) {
        Map<String, Object> member = new LinkedHashMap<>();
        member.put("v", value);
        return member;
    }

    /**
     * A number that travelled as json reads back as a {@code Long} if it has
     * no fraction and a {@code Double} if it has one — whatever class went in.
     * Json carries no width, so the driver picks one class per kind rather
     * than sizing it to the value, which would make what a nested column reads
     * as depend on the row. Sizing it to the value is the failure this is
     * here for: it passes every test that compares json to json, because both
     * sides narrow together.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("types")
    public void everyNumberThatTravelledAsJsonIsALongOrADouble(ValueCatalogue.Type type)
            throws Exception {
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> sample : carry(type).entrySet()) {
            for (String route : AS_JSON) {
                String answer = sample.getValue().get(route);
                if (isNumber(answer) && !answer.startsWith("Long(") && !answer.startsWith("Double(")) {
                    wrong.add(sample.getKey() + " as " + route + " -> " + answer);
                }
            }
        }
        assertThat(type.description() + " came back from json as neither a Long nor a Double:\n  "
            + String.join("\n  ", wrong), wrong, is(empty()));
    }

    /** Whether a rendered answer is a number of any class. */
    private static boolean isNumber(String answer) {
        int open = answer.indexOf('(');
        if (open < 0 || !answer.endsWith(")")) {
            return false;
        }
        try {
            Class<?> read = Class.forName("java.lang." + answer.substring(0, open));
            return Number.class.isAssignableFrom(read);
        } catch (ClassNotFoundException notAJavaLangClass) {
            try {
                return Number.class.isAssignableFrom(
                    Class.forName("java.math." + answer.substring(0, open)));
            } catch (ClassNotFoundException notANumber) {
                return false;
            }
        }
    }

    /**
     * A value written as a parameter and the same value written into the
     * statement text read back the same. The two take different paths — one is
     * bound and typed, the other is parsed by the server — and a caller has no
     * way to know which of them it is relying on.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("types")
    public void aValueBoundAndAValueWrittenOutReadBackTheSame(ValueCatalogue.Type type)
            throws Exception {
        assumeTrue(serverAtLeast(type.major(), type.minor()),
            type.crateType() + " needs a later CrateDB");
        Map<String, Map<String, String>> bySample = new LinkedHashMap<>();
        try (Connection conn = connect()) {
            createTable(conn, type.crateType());
            int id = 0;
            for (ValueCatalogue.Sample sample : type.samples()) {
                if (sample.literal() == null) {
                    continue;
                }
                int bound = ++id;
                int written = ++id;
                try {
                    store(conn, bound, "scalar", sample.value());
                    try (Statement statement = conn.createStatement()) {
                        statement.execute("insert into " + TABLE + " (id, scalar) values ("
                            + written + ", " + sample.literal() + ")");
                    }
                } catch (SQLException | RuntimeException refused) {
                    // A value the column will not hold is refused whichever
                    // way it is written, which is what the route rule covers.
                    continue;
                }
                bySample.put(sample.description(), byRow(
                    "bound as a parameter", bound, "written into the statement", written));
            }
            refresh(conn);
            readScalars(conn, bySample);
        }
        bySample.forEach((sample, byRoute) ->
            agree(type.description(), sample, "writing it two ways", byRoute,
                new ArrayList<>(byRoute.keySet())));
    }

    /**
     * Two rows to compare, named by how the value got into each and held as
     * their ids until everything has been written and the rows can be read in
     * one pass.
     */
    private static Map<String, String> byRow(String oneWay, int one, String otherWay, int other) {
        Map<String, String> rows = new LinkedHashMap<>();
        rows.put(oneWay, String.valueOf(one));
        rows.put(otherWay, String.valueOf(other));
        return rows;
    }

    /** Replaces the row ids left as placeholders with what those rows hold. */
    private static void readScalars(Connection conn, Map<String, Map<String, String>> bySample)
            throws SQLException {
        Map<String, String> byRow = new LinkedHashMap<>();
        try (Statement statement = conn.createStatement();
             ResultSet rows = statement.executeQuery(
                 "select id, scalar from " + TABLE + " order by id")) {
            while (rows.next()) {
                byRow.put(rows.getString("id"), describe(() -> rows.getObject("scalar")));
            }
        }
        bySample.values().forEach(byRoute ->
            byRoute.replaceAll((route, id) -> byRow.getOrDefault(id, "raised nothing was stored")));
    }

    /**
     * A value inserted on its own and the same value inserted in a batch read
     * back the same. A batch reuses one statement across its rows, which is
     * where a conversion carrying something over from the row before would
     * show.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("types")
    public void aValueInsertedAloneAndInABatchReadsBackTheSame(ValueCatalogue.Type type)
            throws Exception {
        assumeTrue(serverAtLeast(type.major(), type.minor()),
            type.crateType() + " needs a later CrateDB");
        Map<String, Map<String, String>> bySample = new LinkedHashMap<>();
        try (Connection conn = connect()) {
            createTable(conn, type.crateType());
            int id = 0;
            for (ValueCatalogue.Sample sample : type.samples()) {
                int alone = ++id;
                int last = id + 3;
                try {
                    store(conn, alone, "scalar", sample.value());
                    try (PreparedStatement statement = conn.prepareStatement(
                             "insert into " + TABLE + " (id, scalar) values (?, ?)")) {
                        while (id < last) {
                            statement.setInt(1, ++id);
                            statement.setObject(2, sample.value());
                            statement.addBatch();
                        }
                        statement.executeBatch();
                    }
                } catch (SQLException | RuntimeException refused) {
                    id = last;
                    continue;
                }
                bySample.put(sample.description(), byRow(
                    "inserted on its own", alone, "inserted in a batch", last));
            }
            refresh(conn);
            readScalars(conn, bySample);
        }
        bySample.forEach((sample, byRoute) ->
            agree(type.description(), sample, "inserting it two ways", byRoute,
                List.of("inserted on its own", "inserted in a batch")));
    }

    /**
     * A stored value described by the accessors that read it. What
     * {@code getObject} produces is what {@code getObject} asked for that same
     * class produces; and where a driver converts to a string at all, it does
     * so the same way through {@code getString} and through {@code getObject}.
     * Refusing a conversion is an answer JDBC allows — giving two different
     * ones is not.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("types")
    public void theAccessorsDescribeOneValue(ValueCatalogue.Type type) throws Exception {
        assumeTrue(serverAtLeast(type.major(), type.minor()),
            type.crateType() + " needs a later CrateDB");
        List<String> contradictions = new ArrayList<>();
        try (Connection conn = connect()) {
            createTable(conn, type.crateType());
            Map<Integer, String> ofRow = new LinkedHashMap<>();
            int id = 0;
            for (ValueCatalogue.Sample sample : type.samples()) {
                try {
                    store(conn, ++id, "scalar", sample.value());
                    ofRow.put(id, sample.description());
                } catch (SQLException | RuntimeException refused) {
                    ofRow.remove(id);
                }
            }
            refresh(conn);
            try (Statement statement = conn.createStatement();
                 ResultSet rows = statement.executeQuery(
                     "select id, scalar from " + TABLE + " order by id")) {
                while (rows.next()) {
                    String sample = ofRow.get(rows.getInt("id"));
                    Object read = rows.getObject("scalar");
                    if (read == null) {
                        continue;
                    }
                    String asIs = describe(() -> read);
                    String asItsOwnClass = describe(() -> rows.getObject("scalar", read.getClass()));
                    if (!asIs.equals(asItsOwnClass)) {
                        contradictions.add(sample + ": getObject gave " + asIs
                            + ", getObject asked for that same class gave " + asItsOwnClass);
                    }
                    String asString = describe(() -> rows.getString("scalar"));
                    String askedForString = describe(() -> rows.getObject("scalar", String.class));
                    if (!askedForString.startsWith("raised") && !asString.equals(askedForString)) {
                        contradictions.add(sample + ": getString gave " + asString
                            + ", getObject asked for a String gave " + askedForString);
                    }
                }
            }
        }
        assertThat(type.description() + ":\n  " + String.join("\n  ", contradictions),
            contradictions, is(empty()));
    }

    /**
     * A moment stores the instant it names, from wherever the JVM stands and
     * by whichever route it is written.
     *
     * <p>This is the one rule here that reads the stored value from the server
     * rather than through the driver, and it has to be: a conversion that
     * shifts a moment on the way in and shifts it back on the way out reads
     * correctly through any round trip. Only the epoch milliseconds the server
     * holds say what was actually stored.</p>
     *
     * <p>It stands the JVM in a zone of its own rather than trusting the one
     * the suite was launched in, because at offset zero a conversion that goes
     * through the default calendar and one that does not give the same answer,
     * and the whole class of fault is invisible.</p>
     */
    @ParameterizedTest(name = "{0} in {1}")
    @MethodSource("momentsAndZones")
    public void aMomentStoresTheInstantItNamesWhereverTheJvmStands(
            String description, String zone, Moment moment) throws Exception {
        TimeZone stood = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            Map<String, Long> byRoute = new LinkedHashMap<>();
            // After the zone is set, so that nothing the connection caches
            // about it predates the change.
            try (Connection conn = connect()) {
                createTable(conn, "timestamp with time zone");
                Object value = moment.value();
                store(conn, 1, "scalar", value);
                store(conn, 2, "list", Collections.singletonList(value));
                store(conn, 3, "list", conn.createArrayOf(
                    "timestamp with time zone", new Object[]{value}));
                store(conn, 4, "member", Collections.singletonMap("v", value));
                store(conn, 5, "nested", Collections.singletonList(
                    Collections.singletonList(value)));
                byRoute.put("a column of its own", storedMillis(conn, 1, "scalar"));
                byRoute.put("an element of a bound series", storedMillis(conn, 2, "list[1]"));
                byRoute.put("an element of a built array", storedMillis(conn, 3, "list[1]"));
                byRoute.put("a member of an OBJECT", storedMillis(conn, 4, "member['v']"));
                byRoute.put("an element of a nested array", storedMillis(conn, 5, "nested[1][1]"));
            }
            List<String> wrong = new ArrayList<>();
            byRoute.forEach((route, stored) -> {
                if (stored == null || stored != EPOCH_MILLIS) {
                    wrong.add(route + " stored " + stored + ", which is "
                        + (stored == null ? "nothing" : (stored - EPOCH_MILLIS) / 1000 + "s out"));
                }
            });
            assertThat(description + " in " + zone + " did not store the instant it names:\n  "
                + String.join("\n  ", wrong), wrong, is(empty()));
        } finally {
            TimeZone.setDefault(stood);
        }
    }

    /** What the server holds, which no conversion on the way out can disguise. */
    private static Long storedMillis(Connection conn, int id, String column) throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rows = statement.executeQuery(
                 "select " + column + "::bigint from " + TABLE + " where id = " + id)) {
            rows.next();
            long millis = rows.getLong(1);
            return rows.wasNull() ? null : millis;
        }
    }

    @FunctionalInterface
    interface Moment {
        Object value();
    }

    static Stream<Arguments> momentsAndZones() {
        Instant instant = Instant.ofEpochMilli(EPOCH_MILLIS);
        Map<String, Moment> moments = new LinkedHashMap<>();
        moments.put("a java.sql.Timestamp", () -> new Timestamp(EPOCH_MILLIS));
        moments.put("an Instant", () -> instant);
        moments.put("an OffsetDateTime", () -> instant.atOffset(ZoneOffset.UTC));
        moments.put("a LocalDateTime at UTC", () -> LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        List<Arguments> cases = new ArrayList<>();
        // Zones either side of UTC and past the whole-hour offsets a
        // conversion is most likely to get right by accident, so one that
        // leans on the default calendar lands somewhere wrong whichever way
        // it errs.
        for (String zone : List.of("UTC", "Europe/Berlin", "America/Los_Angeles",
                "Asia/Kolkata", "Pacific/Kiritimati", "Pacific/Niue")) {
            moments.forEach((description, moment) -> cases.add(Arguments.of(description, zone, moment)));
        }
        return cases.stream();
    }

    /**
     * A series of numbers binds as one array whichever boxes the caller
     * happened to hold. Java has no single class for a whole number, so a list
     * assembled from several sources carries several, and a driver reading the
     * element type off the first element alone would bind the rest wrongly or
     * refuse the lot.
     */
    @Test
    public void aSeriesOfMixedBoxesBindsAsOneArray() throws Exception {
        Map<String, String> byRoute = new LinkedHashMap<>();
        try (Connection conn = connect()) {
            createTable(conn, "bigint");
            store(conn, 1, "list", Arrays.asList((short) 1, 2, 3L));
            store(conn, 2, "list", Arrays.asList(1L, 2L, 3L));
            byRoute.put("boxed several ways", read(conn, 1, row -> row.getArray("list").getArray()));
            byRoute.put("boxed one way", read(conn, 2, row -> row.getArray("list").getArray()));
        }
        agree("a series", "1, 2, 3", "boxing it two ways", byRoute, new ArrayList<>(byRoute.keySet()));
    }

    private static void store(Connection conn, int id, String column, Object value) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                 "insert into " + TABLE + " (id, " + column + ") values (?, ?)")) {
            statement.setInt(1, id);
            statement.setObject(2, value);
            statement.execute();
        }
    }

    private static void refresh(Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.execute("refresh table " + TABLE);
        }
    }

    private static String read(Connection conn, int id, Read read) throws SQLException {
        try (ResultSet row = rowsOf(conn, id)) {
            return describe(() -> read.from(row));
        }
    }

    private static ResultSet rowsOf(Connection conn, int id) throws SQLException {
        ResultSet row = conn.createStatement().executeQuery(
            "select scalar, member, list, nested from " + TABLE + " where id = " + id);
        if (!row.next()) {
            throw new SQLException("The row that was just written is not there");
        }
        return row;
    }

    /** The columns one value is stored in, all of the type under test. */
    private static void createTable(Connection conn, String crateType) throws Exception {
        try (Statement statement = conn.createStatement()) {
            statement.execute("drop table if exists " + TABLE);
            statement.execute(
                "create table " + TABLE + " (" +
                " id integer primary key," +
                " scalar " + crateType + "," +
                " member object as (v " + crateType + ")," +
                " list array(" + crateType + ")," +
                " nested array(array(" + crateType + "))" +
                ") clustered into 1 shards with (number_of_replicas = 0)");
        }
        ensureYellow();
    }

    /**
     * Every route that could not carry the value is one this suite already
     * knows about, and every route it knows about still cannot.
     *
     * <p>A value no route can carry is the server's decision rather than a
     * gap in the driver — a number wider than the column holds, a string
     * longer than it takes — and the catalogue is meant to hold values at that
     * edge. What has to be listed is a value one route carries and another
     * does not, which is the driver reaching the same column two ways and
     * managing it only once.</p>
     */
    private static void assertGapsAreTheKnownOnes(String type, String sample,
                                                 Map<String, String> byRoute) {
        boolean refusedEverywhere = byRoute.values().stream().allMatch(a -> a.startsWith("raised"));
        List<String> unexpected = new ArrayList<>();
        byRoute.forEach((route, answer) -> {
            boolean known = KNOWN_GAPS.containsKey(type + ", " + sample + " / " + route);
            boolean carried = !answer.startsWith("raised");
            if (!carried && !known && !refusedEverywhere) {
                unexpected.add(type + ", " + sample + ", cannot be stored as " + route
                    + ": " + answer);
            }
            if (carried && known) {
                unexpected.add(type + ", " + sample + ", can now be stored as " + route
                    + ", so its entry in KNOWN_GAPS should go");
            }
        });
        assertThat(String.join("\n  ", unexpected), unexpected, is(empty()));
    }

    /**
     * The named routes answered with one value, unless they are known not to —
     * in which case they must still disagree, so that a listing outlives what
     * it describes no longer than the behavior does.
     */
    private static void agreeUnlessKnownNotTo(String type, String sample, String group,
                                              Map<String, String> byRoute, List<String> routes) {
        if (!KNOWN_DISAGREEMENTS.containsKey(type + ", " + sample + " / " + group)) {
            agree(type, sample, group, byRoute, routes);
            return;
        }
        assertThat(type + ", " + sample + ", now reads back as one value through " + group
                + ", so its entry in KNOWN_DISAGREEMENTS should go",
            distinctAnswers(byRoute, routes) > 1, is(true));
    }

    /** The named routes answered with one value, the ones that answered at all. */
    private static void agree(String type, String sample, String group,
                              Map<String, String> byRoute, List<String> routes) {
        if (isPgJdbcReadingInt2BothWays(answered(byRoute, routes).values())
            || isCrateDbSendingAnArrayUnescaped(answered(byRoute, routes).values())
            || isAWallClockBoundAsAnInstant(type, answered(byRoute, routes).values())) {
            return;
        }
        StringBuilder report = new StringBuilder(
            type + ", " + sample + ", is not one value through " + group + ":");
        answered(byRoute, routes).forEach((route, answer) ->
            report.append("\n  ").append(route).append(" -> ").append(answer));
        assertThat(report.toString(), distinctAnswers(byRoute, routes) <= 1, is(true));
    }

    private static long distinctAnswers(Map<String, String> byRoute, List<String> routes) {
        return answered(byRoute, routes).values().stream().distinct().count();
    }

    /** The routes that answered rather than raising, and what each said. */
    private static Map<String, String> answered(Map<String, String> byRoute, List<String> routes) {
        Map<String, String> answered = new LinkedHashMap<>();
        for (String route : routes) {
            String answer = byRoute.get(route);
            if (answer != null && !answer.startsWith("raised")) {
                answered.put(route, answer);
            }
        }
        return answered;
    }

    private static Object memberOf(Object object) {
        return object instanceof Map ? ((Map<?, ?>) object).get("v") : object;
    }

    private static Object elementOf(Object series) throws SQLException {
        Object elements = series instanceof Array ? ((Array) series).getArray() : series;
        return elements == null || java.lang.reflect.Array.getLength(elements) == 0
            ? null
            : java.lang.reflect.Array.get(elements, 0);
    }

    @FunctionalInterface
    private interface Read {
        Object from(ResultSet row) throws SQLException;
    }

    /**
     * A refusal, as the one thing a caller can branch on. Every
     * {@link SQLException} reads as one whatever class the layer that raised
     * it chose, so that two routes refusing for the same reason say so;
     * anything else is named by its class, because it is not a refusal at all
     * but a conversion giving up.
     */
    private static String refusal(Throwable refused) {
        return refused instanceof SQLException
            ? "raised SQLException/" + ((SQLException) refused).getSQLState()
            : "raised " + refused.getClass().getSimpleName();
    }

    @FunctionalInterface
    private interface Answer {
        Object value() throws SQLException;
    }

    /**
     * What a route answered, as the class and the value together — a whole
     * number read back one class narrower is a different answer, and rendering
     * it as text alone would hide that.
     */
    private static String describe(Answer answer) {
        Object value;
        try {
            value = answer.value();
        } catch (SQLException | RuntimeException refused) {
            return refusal(refused);
        }
        if (value == null) {
            return "null";
        }
        if (value instanceof Object[]) {
            return "Object[]" + Arrays.deepToString((Object[]) value);
        }
        if (value.getClass().isArray()) {
            int length = java.lang.reflect.Array.getLength(value);
            Object[] boxed = new Object[length];
            for (int i = 0; i < length; i++) {
                boxed[i] = java.lang.reflect.Array.get(value, i);
            }
            return value.getClass().getComponentType().getSimpleName() + Arrays.toString(boxed);
        }
        return value.getClass().getSimpleName() + "(" + value + ")";
    }

    /**
     * Every type a CrateDB column can be declared as is one the catalogue
     * carries values for, or one it says out loud it does not.
     *
     * <p>The catalogue is written out rather than read from the server, so
     * that what the suite asks is a decision rather than whatever the release
     * happens to offer. This is what keeps that decision honest: a type
     * CrateDB gains and nobody notices fails here, and the choice is then to
     * carry values through it or to say why not.</p>
     */
    @Test
    public void everyTypeTheServerOffersIsInTheCatalogue() throws Exception {
        Set<String> carried = new TreeSet<>();
        for (ValueCatalogue.Type type : ValueCatalogue.types()) {
            carried.add(type.crateType().replaceAll("\\(.*\\)", "").trim());
        }
        Set<String> missing = new TreeSet<>();
        try (Connection conn = connect();
             Statement statement = conn.createStatement();
             ResultSet rows = statement.executeQuery(
                 "select typname from pg_catalog.pg_type where typtype = 'b' order by typname")) {
            while (rows.next()) {
                // An array type is its element type, which the round trip
                // already carries every value through as an array element.
                String type = rows.getString(1).replaceFirst("^_", "");
                if (!carried.contains(type) && !LEFT_OUT.containsKey(type)) {
                    missing.add(rows.getString(1));
                }
            }
        }
        assertThat("CrateDB offers these types and the catalogue neither carries values through "
            + "them nor says why not:\n  " + String.join("\n  ", missing), missing, is(empty()));
    }

    /**
     * Types the catalogue leaves alone, and what stands in for each. A round
     * trip needs a value to carry and a column to carry it to; these have
     * neither, or have one the suite reaches by another name.
     */
    private static final Map<String, String> LEFT_OUT = new LinkedHashMap<>();

    static {
        for (String pgName : List.of("bool", "int2", "int4", "int8", "float4", "float8",
                "varchar", "bpchar", "text", "numeric", "timestamptz", "timestamp", "date", "time",
                "timetz", "point")) {
            LEFT_OUT.put(pgName, "the PostgreSQL name of a type the catalogue carries "
                + "under the CrateDB one");
        }
        LEFT_OUT.put("json", "the shape an OBJECT and a nested array arrive in, which every type "
            + "in the catalogue is already carried through as a member and as a nested element");
        LEFT_OUT.put("char", "a single byte with no CrateDB column type of its own");
        LEFT_OUT.put("oid", "a catalog identifier, which no user column holds");
        LEFT_OUT.put("regproc", "a catalog reference, which no user column holds");
        LEFT_OUT.put("bit", "a string of bits, which arrives as a PGobject rather than as a value "
            + "with a Java class of its own");
        LEFT_OUT.put("interval", "a span rather than a value, which CrateDB stores in no column");
        LEFT_OUT.put("record", "a row, which is not a column type");
        LEFT_OUT.put("void", "the absence of a value");
        LEFT_OUT.put("unknown", "a literal the server has not typed yet");
        LEFT_OUT.put("bytea", "bytes, which CrateDB has no column type for");
        LEFT_OUT.put("name", "a catalog identifier, which no user column holds");
        LEFT_OUT.put("regclass", "a catalog reference, which no user column holds");
        LEFT_OUT.put("regtype", "a catalog reference, which no user column holds");
        LEFT_OUT.put("oidvector", "a list of catalog identifiers, which no user column holds");
    }

    static Stream<ValueCatalogue.Type> types() {
        return ValueCatalogue.types().stream();
    }
}
