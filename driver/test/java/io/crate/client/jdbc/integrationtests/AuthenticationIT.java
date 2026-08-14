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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cratedb.CrateDBContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Connecting as a user who has to prove who they are.
 *
 * <p>The rest of the suite connects as the superuser to a server that asks
 * nothing of anyone, one branch of the handshake and the one an
 * application least often takes. A server can instead be told which users may
 * connect from where and how they are to be authenticated, and this starts one
 * that has been: the superuser is trusted, and everybody else owes a
 * password.</p>
 *
 * <p>What a refusal is matters as much as that it happens: a caller that
 * cannot tell a bad password from an unreachable host retries the one it
 * should give up on. So the SQLState is pinned rather than only the failure.
 * The server is this suite's own, because asking is a setting a node is
 * started with and every other suite needs one that does not.</p>
 */
public class AuthenticationIT extends BaseIntegrationTest {

    private static final String USER = "app";
    private static final String PASSWORD = "correct horse";

    /**
     * The server asks for a password from everyone but the superuser, who is
     * left trusted so that there is someone to create the user with. An
     * unmatched request is denied outright, so the entry that names nobody has
     * to come last — the key orders them.
     */
    private static final String COMMAND = "crate"
        + " -C discovery.type=single-node"
        + " -C auth.host_based.enabled=true"
        + " -C auth.host_based.config.0.user=crate"
        + " -C auth.host_based.config.0.method=trust"
        + " -C auth.host_based.config.99.method=password";

    /** The state PostgreSQL gives a login refused over who is offering it. */
    private static final String REFUSED = "28000";

    /** The state pgJDBC gives a connection it never established. */
    private static final String NOT_ESTABLISHED = "08004";

    private static CrateDBContainer server;
    private static String url;

    @BeforeAll
    static void startAServerThatAsks() throws Exception {
        server = new CrateDBContainer(serverImage()).withCommand(COMMAND);
        server.start();
        url = String.format("crate://%s:%d/doc", server.getHost(), server.getMappedPort(5432));
        try (Connection conn = connectAs("crate", null);
             Statement statement = conn.createStatement()) {
            statement.execute("create user " + USER + " with (password = '" + PASSWORD + "')");
            statement.execute("grant all privileges to " + USER);
        }
    }

    @AfterAll
    static void stopTheServer() {
        if (server != null) {
            server.stop();
        }
    }

    private static Connection connectAs(String user, String password) throws SQLException {
        Properties credentials = new Properties();
        credentials.setProperty("user", user);
        if (password != null) {
            credentials.setProperty("password", password);
        }
        return DriverManager.getConnection(url, credentials);
    }

    /**
     * The trusted entry still works, so the refusals below
     * the server asking rather than the server being unreachable.
     */
    @Test
    public void aTrustedUserConnectsWithoutAPassword() throws Exception {
        try (Connection conn = connectAs("crate", null);
             Statement statement = conn.createStatement();
             ResultSet row = statement.executeQuery("select current_user")) {
            row.next();
            assertThat(row.getString(1), is("crate"));
        }
    }

    /** The password is carried to the server, and the session is that user's. */
    @Test
    public void theRightPasswordConnectsAsThatUser() throws Exception {
        try (Connection conn = connectAs(USER, PASSWORD);
             Statement statement = conn.createStatement();
             ResultSet row = statement.executeQuery("select current_user")) {
            row.next();
            assertThat(row.getString(1), is(USER));
        }
    }

    /**
     * A wrong password is refused as a login, not as a connection that could
     * be tried again — a caller that cannot tell the two apart retries what it
     * should give up on.
     */
    @Test
    public void aWrongPasswordIsRefused() {
        SQLException refused = assertThrows(SQLException.class, () -> connectAs(USER, "wrong"));
        assertThat(refused.getSQLState(), is(REFUSED));
    }

    /**
     * A user the server does not have is refused as the wrong password is,
     * so that a caller cannot learn which names exist.
     */
    @Test
    public void anUnknownUserIsRefused() {
        SQLException refused = assertThrows(SQLException.class, () -> connectAs("nobody", PASSWORD));
        assertThat(refused.getSQLState(), is(REFUSED));
    }

    /**
     * Offering no password where one is required never reaches the server:
     * pgJDBC is asked for one, has none, and gives up itself. So it reports a
     * connection that was not established rather than a login that was turned
     * down — the same request refused by the driver and by the server reaches
     * a caller under two different states, and code branching on one of them
     * has to know both.
     */
    @Test
    public void aMissingPasswordIsRefusedBeforeTheServerIsAnswered() {
        SQLException refused = assertThrows(SQLException.class, () -> connectAs(USER, null));
        assertThat(refused.getSQLState(), is(NOT_ESTABLISHED));
        assertThat(refused.getMessage(), containsString("no password was provided"));
    }
}
