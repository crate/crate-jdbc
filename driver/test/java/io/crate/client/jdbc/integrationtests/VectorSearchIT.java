package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.core.Is.is;

/**
 * A {@code float_vector} column, which is CrateDB's own type and reaches the
 * server as an array of {@code real}: what
 * {@code createArrayOf("float_vector", ...)} writes is what {@code getArray}
 * reads back. {@code README.rst} ships that round trip as the quick start for
 * vector search, and what a query then does with the column — {@code knn_match}
 * and {@code vector_similarity} — is the server's.
 */
public class VectorSearchIT extends BaseIntegrationTest {

    private static Connection conn;

    @BeforeAll
    static void setUpVectors() throws Exception {
        dropAllUserTables();
        conn = connect();
        conn.createStatement().execute(
            "create table embeddings (" +
            " id integer primary key," +
            " label string," +
            " embedding float_vector(3)" +
            ") clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();

        try (PreparedStatement insert = conn.prepareStatement(
            "insert into embeddings (id, label, embedding) values (?, ?, ?)")) {
            insert(insert, 1, "origin", 0.0f, 0.0f, 0.0f);
            insert(insert, 2, "near", 1.0f, 1.0f, 1.0f);
        }
        conn.createStatement().execute("refresh table embeddings");
    }

    private static void insert(PreparedStatement insert, int id, String label, float... embedding)
        throws SQLException {
        Float[] boxed = new Float[embedding.length];
        for (int i = 0; i < embedding.length; i++) {
            boxed[i] = embedding[i];
        }
        insert.setInt(1, id);
        insert.setString(2, label);
        insert.setArray(3, conn.createArrayOf("float_vector", boxed));
        insert.execute();
    }

    @AfterAll
    static void tearDownVectors() throws Exception {
        if (conn != null) {
            conn.close();
        }
        dropAllUserTables();
    }

    @Test
    public void embeddingsRoundTripThroughGetArray() throws Exception {
        try (ResultSet resultSet = conn.createStatement().executeQuery(
            "select embedding from embeddings where id = 2")) {
            assertThat(resultSet.next(), is(true));
            assertThat((Object[]) resultSet.getArray(1).getArray(),
                arrayContaining((Object) 1.0f, 1.0f, 1.0f));
        }
    }
}
