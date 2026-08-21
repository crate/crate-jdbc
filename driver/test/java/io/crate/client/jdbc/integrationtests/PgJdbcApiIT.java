package io.crate.client.jdbc.integrationtests;

import io.crate.client.jdbc.CrateConnection;
import io.crate.client.jdbc.CrateDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGStatement;
import org.postgresql.jdbc.PgConnection;

import java.net.URI;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

/**
 * The CrateDB adaptation layer stays transparent to pgJDBC's own API:
 * connections and statements can still be cast or unwrapped to the pgJDBC
 * interfaces, navigation between JDBC objects stays inside the driver, and
 * connections obtained from a {@link CrateDataSource} carry the same
 * CrateDB behavior as those from a {@code crate://} URL.
 */
public class PgJdbcApiIT extends BaseIntegrationTest {

    @BeforeEach
    void setUpTables() throws Exception {
        dropAllUserTables();
        setUpTestTable();
        insertIntoTestTable();
    }

    @AfterEach
    void tearDownTables() {
        dropAllUserTables();
    }

    /**
     * An application that reaches for pgJDBC's own API finds it: the wrappers
     * are its interfaces, and unwrapping arrives at this driver's object or at
     * pgJDBC's, whichever was asked for.
     */
    @Test
    public void connectionsAndStatementsCarryThePgJdbcApi() throws Exception {
        try (Connection conn = connect();
             PreparedStatement stmt = conn.prepareStatement("select id from test")) {
            PGConnection pgConnection = (PGConnection) conn;
            assertThat(pgConnection.getBackendPID() > 0, is(true));

            PGStatement pgStatement = (PGStatement) stmt;
            pgStatement.setPrepareThreshold(3);
            assertThat(pgStatement.getPrepareThreshold(), is(3));

            assertThat(conn.isWrapperFor(CrateConnection.class), is(true));
            assertThat(conn.unwrap(CrateConnection.class), is(sameInstance(conn)));
            assertThat(conn.unwrap(PgConnection.class), is(instanceOf(PgConnection.class)));
        }
    }

    @Test
    public void resultSetsNavigateBackToTheWrappedConnection() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select id from test");

            assertThat(rs.getStatement(), is(sameInstance(stmt)));
            assertThat(rs.getStatement().getConnection(), is(sameInstance(conn)));
        }
    }

    @Test
    public void metadataRowsNavigateBackToTheWrappedConnection() throws Exception {
        try (Connection conn = connect()) {
            ResultSet tables = conn.getMetaData().getTables(null, "sys", "summits", null);

            assertThat(tables.getStatement().getConnection(), is(instanceOf(CrateConnection.class)));
        }
    }

    /**
     * The metadata itself navigates back too, and to the connection it was
     * asked of rather than to another wrapper around the same session.
     */
    @Test
    public void metadataNavigatesBackToTheConnectionItDescribes() throws Exception {
        try (Connection conn = connect()) {
            assertThat(conn.getMetaData().getConnection(), is(sameInstance(conn)));
        }
    }

    @Test
    public void callableStatementsCarryTheCrateBehavior() throws Exception {
        try (Connection conn = connect();
             CallableStatement call = conn.prepareCall("select object_field from test")) {
            assertThat(call.getConnection(), is(sameInstance(conn)));

            ResultSet rs = call.executeQuery();
            assertThat(rs.next(), is(true));
            assertThat(rs.getObject(1), is(instanceOf(Map.class)));
        }
    }

    /**
     * A wrapper is made once for what it wraps, so asking twice gives back the
     * same object rather than a second view of the same thing, as
     * callers that compare result sets or metadata expect, and what pgJDBC
     * does underneath.
     */
    @Test
    public void askingTwiceForTheSameThingGivesTheSameWrapper() throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("select id from test");

            assertThat(stmt.getResultSet(), is(sameInstance(stmt.getResultSet())));
            assertThat(conn.getMetaData(), is(sameInstance(conn.getMetaData())));
        }
    }

    @Test
    public void metadataReportsTheUrlTheCallerConnectedWith() throws Exception {
        try (Connection conn = connect()) {
            assertThat(conn.getMetaData().getURL(), startsWith("jdbc:crate://"));
        }
    }

    @Test
    public void dataSourceConnectionsAdaptCrateBehavior() throws Exception {
        URI address = serverAddress();
        CrateDataSource dataSource = new CrateDataSource();
        dataSource.setServerNames(new String[]{address.getHost()});
        dataSource.setPortNumbers(new int[]{address.getPort()});
        dataSource.setDatabaseName("doc");
        dataSource.setUser("crate");

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            conn.rollback();

            ResultSet rs = conn.createStatement().executeQuery("select object_field from test");
            assertThat(rs.next(), is(true));
            assertThat(rs.getObject(1), is(instanceOf(Map.class)));
        }
    }

    @Test
    public void dataSourceConfiguredWithACrateUrlConnects() throws Exception {
        CrateDataSource dataSource = new CrateDataSource();
        dataSource.setUrl(connectionUrl());

        try (Connection conn = dataSource.getConnection()) {
            assertThat(conn, is(instanceOf(CrateConnection.class)));
            assertThat(conn.isValid(2), is(true));
        }
    }
}
