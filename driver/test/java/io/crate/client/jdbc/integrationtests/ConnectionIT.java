/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.crate.client.jdbc.integrationtests;

import io.crate.client.jdbc.CrateConnection;
import io.crate.client.jdbc.CrateVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins connection-level behavior of the crate:// driver: schema selection,
 * multi-host URLs, how server errors surface, and single result set
 * semantics. Batching is {@link CrudBatchIT}'s.
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
     * {@code setSchema} moves the search path, so unqualified statements land
     * in the named schema — the JDBC way of doing what the {@code /schema}
     * segment of the URL does at connect time.
     */
    @Test
    public void customSchemaAppliesToStatements() throws SQLException, InterruptedException {
        try (Connection conn = connect()) {
            conn.setSchema("foo");
            assertThat(conn.getSchema(), is("foo"));

            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE t (name STRING) WITH (number_of_replicas=0)");
            ensureYellow();

            ResultSet rs = stmt.executeQuery(
                "SELECT table_schema " +
                "FROM information_schema.TABLES " +
                "WHERE table_name = 't'"
            );

            assertThat(rs.next(), is(true));
            assertThat(rs.getObject(1), is("foo"));
            assertThat(rs.next(), is(false));
        }
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

    @Test
    public void customSchemaAppliesToPreparedStatements() throws Exception {
        try (Connection conn = connect()) {
            conn.setSchema("bar");

            PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE t (id INTEGER) WITH (number_of_replicas=0)");
            assertThat(stmt.execute(), is(false));
            ensureYellow();

            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT table_schema " +
                "FROM information_schema.TABLES " +
                "WHERE table_name = 't'"
            );

            assertThat(rs.next(), is(true));
            assertThat(rs.getObject(1), is("bar"));
            assertThat(rs.next(), is(false));
        }
    }

    @Test
    public void preparedStatementWithoutMatchesReturnsEmptyResultSet() throws Exception {
        try (Connection conn = connect()) {
            PreparedStatement preparedStatement = conn.prepareStatement("select * from test where id = ?");
            preparedStatement.setInt(1, 2);
            ResultSet resultSet = preparedStatement.executeQuery();

            assertThat(resultSet, notNullValue());
            assertThat(resultSet.isBeforeFirst(), is(false));
        }
    }

    @Test
    public void syntaxErrorRaisesSQLException() throws Exception {
        try (Connection conn = connect()) {
            SQLException e = assertThrows(SQLException.class,
                () -> conn.createStatement().execute("ERROR"));
            // The parser message wording varies across CrateDB versions.
            assertThat(e.getMessage(), anyOf(
                containsString("line 1:1: no viable alternative at input 'ERROR'"),
                containsString("line 1:1: mismatched input 'ERROR' expecting {'SELECT', '"),
                containsString("line 1:1: extraneous input 'ERROR' expecting {")
            ));
            // CrateDB classifies a parse failure as a syntax error from 6.4
            // on; before that it reports an internal error.
            if (serverAtLeast(6, 4)) {
                assertThat(e.getSQLState(), is("42601"));
            }
        }
    }

    /**
     * Server errors carry the SQLState of their condition, which is what
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

    @Test
    public void liveConnectionsReportThemselvesValid() throws Exception {
        Connection conn = connect();
        assertThat(conn.isValid(2), is(true));
        assertThat(conn.isClosed(), is(false));

        conn.close();
        assertThat(conn.isClosed(), is(true));
        assertThat(conn.isValid(2), is(false));
    }

    /**
     * CrateDB has no read-only sessions, so the flag is remembered on the
     * connection and reported back without being enforced against the
     * server.
     */
    @Test
    public void readOnlyIsRememberedOnTheConnection() throws Exception {
        try (Connection conn = connect()) {
            conn.setReadOnly(true);
            assertThat(conn.isReadOnly(), is(true));

            conn.setReadOnly(false);
            assertThat(conn.isReadOnly(), is(false));
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

    @Test
    public void unknownUrlParametersAreAccepted() throws Exception {
        String url = connectionUrl() + "&somethingUnknown=abcd";
        try (Connection conn = DriverManager.getConnection(url)) {
            assertThat(conn.isValid(2), is(true));
        }
    }

    /**
     * A URL may name every node of a cluster, which is what
     * {@code loadBalanceHosts} — on by default for this driver — spreads
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
