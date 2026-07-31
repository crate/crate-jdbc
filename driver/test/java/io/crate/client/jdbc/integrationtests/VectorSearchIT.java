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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

/**
 * Pins k-nearest-neighbour search over {@code float_vector} columns: writing
 * embeddings through {@link Connection#createArrayOf}, restricting a query
 * with {@code knn_match}, and ranking with {@code vector_similarity}.
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
            insert(insert, 3, "far", 9.0f, 9.0f, 9.0f);
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

    /**
     * {@code knn_match} restricts a query to the k nearest rows.
     */
    @Test
    public void knnMatchRestrictsToTheNearestRows() throws Exception {
        try (ResultSet resultSet = conn.createStatement().executeQuery(
            "select label from embeddings where knn_match(embedding, [0.5, 0.5, 0.5], 2) " +
            "order by _score desc")) {
            List<String> labels = new ArrayList<>();
            while (resultSet.next()) {
                labels.add(resultSet.getString(1));
            }
            assertThat(labels, contains("origin", "near"));
        }
    }

    /**
     * The query vector binds as a parameter, so an embedding computed at
     * runtime does not have to be spliced into the statement text.
     */
    @Test
    public void queryVectorsBindAsParameters() throws Exception {
        try (PreparedStatement search = conn.prepareStatement(
            "select label from embeddings where knn_match(embedding, ?, 1)")) {
            search.setArray(1, conn.createArrayOf("float_vector", new Float[]{9.0f, 9.0f, 9.0f}));
            try (ResultSet resultSet = search.executeQuery()) {
                assertThat(resultSet.next(), is(true));
                assertThat(resultSet.getString(1), is("far"));
            }
        }
    }

    @Test
    public void vectorSimilarityRanksRowsAgainstAQueryVector() throws Exception {
        try (PreparedStatement scoring = conn.prepareStatement(
            "select label, vector_similarity(embedding, ?) as score " +
            "from embeddings order by score desc")) {
            scoring.setArray(1, conn.createArrayOf("float_vector", new Float[]{1.0f, 1.0f, 1.0f}));
            try (ResultSet resultSet = scoring.executeQuery()) {
                List<String> labels = new ArrayList<>();
                double previous = Double.MAX_VALUE;
                while (resultSet.next()) {
                    labels.add(resultSet.getString("label"));
                    double score = resultSet.getDouble("score");
                    assertThat(previous, is(greaterThan(score)));
                    previous = score;
                }
                assertThat(labels, contains("near", "origin", "far"));
            }
        }
    }

    /**
     * An embedding round-trips as an array of {@code real}: what
     * {@code createArrayOf("float_vector", ...)} writes is what
     * {@code getArray} reads back.
     */
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
