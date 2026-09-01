package io.crate.client.jdbc.integrationtests;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Stock pgJDBC swept against a stock PostgreSQL, so that a fault the delta
 * hands to pgJDBC can be shown to happen with no CrateDB in the picture.
 *
 * <p>An entry marked {@code pgjdbc-todo} says the behavior belongs upstream.
 * What makes it reportable there is that it reproduces against the database
 * pgJDBC was written for: a maintainer of that driver is owed a case naming
 * neither this driver nor the server it adapts. This cell is where such a case
 * comes from, run alongside the delta instead of being written out by hand for
 * each entry it covers.</p>
 *
 * <p>What it answers is never compared with what the other cells answered, and
 * a comparison would carry no meaning if it were. The catalog a column belongs
 * to is this server's own database, {@code getColumnTypeName} reads
 * {@code jsonb} where CrateDB names its own object type, and every probe column
 * is the nearest PostgreSQL type rather than the same type. What outlives the
 * change of substrate is exactly the part a comparison could never reach:
 * {@link ClosedObjectContract}, which is a rule about a closed object rather
 * than about a rendering, and {@link SqlStateContract}, which is a rule about
 * the five characters a caller branches on. Those are the only questions put to
 * it.</p>
 *
 * <p>It starts a server of its own, which nothing else in the suite has a use
 * for, so it is asked for with {@code -PtestControl=true} rather than run by
 * default.</p>
 */
final class ControlCell {

    private static final String IMAGE = "postgres:17";

    /**
     * The schema the probe table lives in. {@link JdbcSurface} hands the same
     * literal to every metadata call it lists, and {@code PROBE_SQL} names the
     * table unqualified, so the connection carries {@code currentSchema} and
     * this server keeps a schema by that name.
     */
    private static final String SCHEMA = "doc";

    private static final String QUALIFIED_TABLE = SCHEMA + "." + JdbcSurface.PROBE_TABLE;

    private static final String CREATE_SCHEMA = "create schema " + SCHEMA;

    /**
     * The probe table as PostgreSQL spells it: the same fourteen columns in the
     * same order, each the nearest native type.
     *
     * <p>{@code matrix} is nested in CrateDB and flat here. A CrateDB
     * {@code array(array(integer))} holds sub-arrays of differing lengths,
     * while a PostgreSQL array is one rectangular block — the ragged values the
     * other cells store have no spelling in this type system at all. The column
     * carries a name and a readable value; the nesting is CrateDB's and stays
     * there.</p>
     */
    private static final String CREATE_PROBE_TABLE =
        "create table " + QUALIFIED_TABLE + " (" +
        " id integer primary key," +
        " name text," +
        " amount double precision," +
        " tags text[]," +
        " details jsonb," +
        " flag boolean," +
        " count_ bigint," +
        " ratio real," +
        " stamp timestamptz," +
        " address inet," +
        " exact_ numeric(10, 2)," +
        " numbers integer[]," +
        " matrix integer[]," +
        " nothing text" +
        ")";

    /** Named from the surface's own list, so the two orders cannot drift apart. */
    private static final String PROBE_COLUMNS = String.join(", ", JdbcSurface.PROBE_COLUMN_NAMES);

    /**
     * The rows, in the shape the other cells store: one carrying values, one
     * carrying nothing but its key, and three at the edges of what the columns
     * take. Row 1 fills the columns the fixtures read from — a row to stand on,
     * and an array to hand to the {@code Array} fixture.
     *
     * <p>A moment is a number of milliseconds to CrateDB and a point in time to
     * PostgreSQL, so the same epoch millis reach {@code stamp} through
     * {@code to_timestamp}.</p>
     */
    private static final List<String> INSERT_PROBE_ROWS = List.of(
        "insert into " + QUALIFIED_TABLE + " (id, name, amount, tags, details) values "
        + "(1, 'first', 2.5, array['a', 'b'], '{\"note\": \"note\", \"count\": 3}'::jsonb)",
        "insert into " + QUALIFIED_TABLE + " (id) values (2)",
        "insert into " + QUALIFIED_TABLE + " (" + PROBE_COLUMNS + ") values "
        + "(3, '', 0.0, array[]::text[], '{\"note\": \"\", \"count\": 0}'::jsonb, false, 0, 0.0, "
        + "to_timestamp(0 / 1000.0), '0.0.0.0', 0.00, array[]::integer[], array[]::integer[], null)",
        "insert into " + QUALIFIED_TABLE + " (" + PROBE_COLUMNS + ") values "
        + "(4, 'héllo·日本', 1.7976931348623157e308, array['', null], "
        + "'{\"note\": null, \"count\": null}'::jsonb, true, 9007199254740993, 3.4028235e38, "
        + "to_timestamp(253402300799000 / 1000.0), '255.255.255.255', 99999999.99, "
        + "array[null, 2147483647], array[1, 2], null)",
        "insert into " + QUALIFIED_TABLE + " (" + PROBE_COLUMNS + ") values "
        + "(5, 'last', -1.0, array['z'], '{\"note\": \"z\", \"count\": -1}'::jsonb, false, "
        + "-9007199254740993, -1.5, to_timestamp(-2208988800000 / 1000.0), '::1', -99999999.99, "
        + "array[-2147483648], array[-1, 0], null)");

    private static PostgreSQLContainer<?> server;
    private static String url;

    private ControlCell() {
    }

    /**
     * Whether this cell was asked for. The server it needs costs more than the
     * comparison it supports, so a run that is not looking upstream does
     * without it.
     */
    static boolean asked() {
        return Boolean.getBoolean("test.control");
    }

    /**
     * Stock pgJDBC's answers to the whole surface, against a server that has
     * never heard of CrateDB. The first caller pays for the container; the
     * suite holds one and there is nothing in it worth building twice.
     */
    static synchronized Sweep sweep(List<Invocation> surface) throws SQLException {
        return Sweep.of(url(), surface);
    }

    /** Gives the container back, if one was ever started. */
    static synchronized void stop() {
        if (server != null) {
            server.stop();
            server = null;
            url = null;
        }
    }

    /**
     * The URL the sweep connects through, with the schema the surface expects
     * on the search path.
     *
     * <p>Credentials travel in the URL because {@link Fixture} opens every one
     * of its connections with {@code DriverManager.getConnection(url)} and has
     * nowhere else to put them.</p>
     */
    private static String url() throws SQLException {
        if (url == null) {
            // The sweep opens a connection for every call it makes, and
            // PostgreSQL gives each one a process of its own. Its default
            // ceiling of a hundred is reached long before the surface is,
            // and a call that could not connect is a call this cell has no
            // opinion about.
            server = new PostgreSQLContainer<>(DockerImageName.parse(IMAGE))
                .withCommand("postgres", "-c", "max_connections=800");
            server.start();
            // The container's own URL already carries a query string, so what
            // is added to it joins that one rather than opening a second.
            String given = server.getJdbcUrl();
            String credentialed = given + (given.indexOf('?') < 0 ? '?' : '&')
                + "user=" + server.getUsername() + "&password=" + server.getPassword();
            fillProbeTable(credentialed);
            url = credentialed + "&currentSchema=" + SCHEMA;
        }
        return url;
    }

    private static void fillProbeTable(String plainUrl) throws SQLException {
        try (Connection conn = DriverManager.getConnection(plainUrl);
             Statement statement = conn.createStatement()) {
            statement.execute(CREATE_SCHEMA);
            statement.execute(CREATE_PROBE_TABLE);
            for (String row : INSERT_PROBE_ROWS) {
                statement.execute(row);
            }
        }
    }
}
