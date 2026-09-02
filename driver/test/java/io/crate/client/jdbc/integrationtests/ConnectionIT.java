package io.crate.client.jdbc.integrationtests;

import io.crate.client.jdbc.CrateConnection;
import io.crate.client.jdbc.CrateVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins connection-level behavior of the crate:// driver: the schema a URL
 * naming none lands in, the several hosts one may name, the CrateDB version a
 * connection reads and keeps, and how a server error reaches the caller.
 */
public class ConnectionIT extends BaseIntegrationTest {

    @BeforeEach
    void setUpTables() throws Exception {
        dropAllUserTables();
        setUpTestTable();
    }

    @AfterEach
    void tearDownTables() {
        dropAllUserTables();
    }

    /**
     * A URL that names no schema connects to CrateDB's default one. pgJDBC
     * reads the URL's path segment as a PostgreSQL database name and fills in
     * the user name when it is missing, which for CrateDB would be a schema
     * nobody asked for.
     */
    @Test
    public void aUrlWithoutASchemaConnectsToDoc() throws Exception {
        URI address = serverAddress();
        String withoutSchema = String.format("crate://%s:%d/%s", address.getHost(), address.getPort(),
            address.getQuery() == null ? "" : "?" + address.getQuery());
        try (Connection conn = DriverManager.getConnection(withoutSchema)) {
            assertThat(conn.getSchema(), is("doc"));
        }
    }

    /**
     * Server errors carry the SQLState of their condition, and that is what
     * frameworks classify errors by — a missing table has to be
     * distinguishable from a syntax error without reading the message.
     */
    @Test
    public void serverErrorsCarryTheirSQLState() throws Exception {
        try (Connection conn = connect()) {
            SQLException missingTable = assertThrows(SQLException.class,
                () -> conn.createStatement().execute("select * from does_not_exist"));
            assertThat(missingTable.getSQLState(), is("42P01"));

            SQLException missingColumn = assertThrows(SQLException.class,
                () -> conn.createStatement().execute("select does_not_exist from test"));
            assertThat(missingColumn.getSQLState(), is("42703"));
        }
    }

    /**
     * The CrateDB version is reachable through the connection, next to the
     * PostgreSQL release CrateDB emulates that the metadata reports.
     */
    @Test
    public void connectionsReportTheCrateDbVersion() throws Exception {
        try (Connection conn = connect()) {
            CrateVersion version = conn.unwrap(CrateConnection.class).getCrateVersion();

            ResultSet reported = conn.createStatement().executeQuery(
                "select version['number'] from sys.nodes limit 1");
            assertThat(reported.next(), is(true));
            assertThat(version.toString(), is(reported.getString(1)));

            assertThat(version.major(), greaterThanOrEqualTo(6));
            assertThat(conn.getMetaData().getDatabaseProductVersion(),
                is(not(version.toString())));
        }
    }

    /**
     * The version is read from the server once and kept, and what is kept is
     * not a way to go on asking a connection that has been closed. It is read
     * first here, because answering from what was already read is the only way
     * a closed connection could answer at all.
     */
    @Test
    public void aClosedConnectionIsRefusedTheCrateDbVersion() throws Exception {
        Connection conn = connect();
        CrateConnection crateConnection = conn.unwrap(CrateConnection.class);
        crateConnection.getCrateVersion();
        conn.close();

        SQLException refused = assertThrows(SQLException.class, crateConnection::getCrateVersion);
        assertThat(refused.getSQLState(), is("08003"));
    }

    /**
     * A URL may name every node of a cluster, and {@code loadBalanceHosts},
     * on by default for this driver, spreads
     * connections over. A host that cannot be reached is passed over for the
     * next one.
     */
    @Test
    public void aUrlNamingSeveralHostsConnectsToOneThatAnswers() throws Exception {
        URI address = serverAddress();
        String hosts = "127.0.0.1:1," + address.getHost() + ":" + address.getPort();
        String url = String.format("crate://%s/doc%s", hosts,
            address.getQuery() == null ? "" : "?" + address.getQuery());

        try (Connection conn = DriverManager.getConnection(url)) {
            assertThat(conn.createStatement().execute("select 1 from sys.cluster"), is(true));
        }
    }
}
