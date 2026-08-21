package io.crate.client.jdbc.integrationtests;

import io.crate.client.jdbc.CrateDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cratedb.CrateDBContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Connecting as a user who has to prove who they are, through
 * {@link CrateDataSource}.
 *
 * <p>The rest of the suite connects as the superuser to a server that asks
 * nothing of anyone, one branch of the handshake and the one an application
 * least often takes. A server can instead be told which users may connect from
 * where and how they are to be authenticated, and this starts one that has
 * been: the superuser is trusted, and everybody else owes a password.</p>
 *
 * <p>The data source is where the credentials are this driver's to carry: it
 * builds the connection properties itself and opens the connection through
 * pgJDBC's driver rather than through {@code DriverManager}, so a password
 * dropped on that path ships broken to every authenticated deployment and to
 * every pool built on one. The server is this suite's own, because asking is a
 * setting a node is started with and every other suite needs one that does
 * not.</p>
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

    private static CrateDBContainer server;
    private static String url;

    @BeforeAll
    static void startAServerThatAsks() throws Exception {
        server = new CrateDBContainer(serverImage()).withCommand(COMMAND);
        server.start();
        url = String.format("crate://%s:%d/doc", server.getHost(), server.getMappedPort(5432));
        Properties trusted = new Properties();
        trusted.setProperty("user", "crate");
        try (Connection conn = DriverManager.getConnection(url, trusted);
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

    /** The password is carried to the server, and the session is that user's. */
    @Test
    public void theRightPasswordConnectsAsThatUser() throws Exception {
        CrateDataSource dataSource = new CrateDataSource();
        dataSource.setUrl(url);

        try (Connection conn = dataSource.getConnection(USER, PASSWORD);
             Statement statement = conn.createStatement();
             ResultSet row = statement.executeQuery("select current_user")) {
            row.next();
            assertThat(row.getString(1), is(USER));
        }
    }
}
