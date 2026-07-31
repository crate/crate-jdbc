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

import io.crate.client.jdbc.CrateResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the everyday statement workflows an application relies on: CRUD
 * through statements and prepared statements, batching including what a
 * batch does when it cannot run, and {@code prepareCall} used as a plain
 * parameterized statement.
 */
public class CrudBatchIT extends BaseIntegrationTest {

    @AfterEach
    void tearDownTables() {
        dropAllUserTables();
    }

    @Test
    public void crudOperationsCanBeUsed() throws Exception {
        try (var conn = connect()) {
            var stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS tbl");
            var hasResultSet = stmt.execute("CREATE TABLE tbl (x int, y int)");
            assertThat(hasResultSet, is(false));
            assertThat(stmt.getUpdateCount(), is(1));

            var insertStmt = conn.prepareStatement("INSERT INTO tbl (x, y) VALUES (?, ?)");
            insertStmt.setInt(1, 1);
            insertStmt.setInt(2, 10);
            insertStmt.execute();

            stmt.execute("REFRESH TABLE tbl");

            var results = conn.createStatement().executeQuery("SELECT x, y FROM tbl ORDER BY 1");
            assertThat(results.next(), is(true));
            assertThat(results.getInt(1), is(1));
            assertThat(results.getInt(2), is(10));
            assertThat(results.next(), is(false));

            stmt.execute("UPDATE tbl SET y = y + 10");
            stmt.execute("REFRESH TABLE tbl");

            var resultsAfterUpdate = conn.createStatement().executeQuery("SELECT x, y FROM tbl ORDER BY 1");
            assertThat(resultsAfterUpdate.next(), is(true));
            assertThat(resultsAfterUpdate.getInt(1), is(1));
            assertThat(resultsAfterUpdate.getInt(2), is(20));
            assertThat(resultsAfterUpdate.next(), is(false));
        }
    }

    @Test
    public void prepareCallCanBeUsedToInsertRecords() throws Exception {
        try (var conn = connect()) {
            var stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS tbl");
            stmt.execute("CREATE TABLE tbl (x int, y int)");

            try (var call = conn.prepareCall("INSERT INTO tbl (x, y) VALUES (?, ?)")) {
                call.setInt(1, 1);
                call.setInt(2, 15);
                call.execute();
            }

            stmt.execute("REFRESH TABLE tbl");

            var results = conn.createStatement().executeQuery("SELECT x, y FROM tbl ORDER BY 1");
            assertThat(results.next(), is(true));
            assertThat(results.getInt(1), is(1));
            assertThat(results.getInt(2), is(15));
            assertThat(results.next(), is(false));
        }
    }

    @Test
    public void preparedStatementBatchInsertsEveryRow() throws Exception {
        try (var conn = connect()) {
            var stmt = conn.createStatement();
            stmt.execute("CREATE TABLE tbl (x int, y int)");

            try (var insert = conn.prepareStatement("INSERT INTO tbl (x, y) VALUES (?, ?)")) {
                for (int i = 0; i < 20; i++) {
                    insert.setInt(1, i);
                    insert.setInt(2, i * 10);
                    insert.addBatch();
                }
                assertThat(insert.executeBatch(), is(oneRowEach(20)));
            }

            stmt.execute("REFRESH TABLE tbl");

            var results = conn.createStatement().executeQuery("SELECT count(*) FROM tbl");
            assertThat(results.next(), is(true));
            assertThat(results.getInt(1), is(20));
            assertThat(results.next(), is(false));
        }
    }

    @Test
    public void statementBatchExecutes() throws Exception {
        try (var conn = connect()) {
            var stmt = conn.createStatement();
            stmt.execute("CREATE TABLE tbl (x int, y int)");
            stmt.addBatch("insert into tbl (x) values (3)");
            stmt.addBatch("insert into tbl (x) values (4)");

            assertThat(stmt.executeBatch(), is(oneRowEach(2)));
            assertThat(rowCount(conn), is(2L));
        }
    }

    /**
     * A batch is all-or-nothing: whatever the batch trips over, nothing is
     * written and the update counts say every entry failed rather than
     * reporting a partial success.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("batchesThatCannotRun")
    public void aBatchThatCannotRunWritesNothing(String description, int entries, BatchBuilder batch)
            throws Exception {
        try (var conn = connect()) {
            conn.createStatement().execute("CREATE TABLE tbl (x int, y int)");

            try (Statement statement = batch.build(conn)) {
                assertThat(failedBatch(statement), is(allFailed(entries)));
            }
            assertThat(rowCount(conn), is(0L));
        }
    }

    static Stream<Arguments> batchesThatCannotRun() {
        return Stream.of(
            // A query has no update count, so it cannot be part of a batch.
            Arguments.of("a query among the statements", 3, batch(conn -> {
                Statement stmt = conn.createStatement();
                stmt.addBatch("insert into tbl (x) values (3)");
                stmt.addBatch("insert into tbl (x) values (5)");
                stmt.addBatch("select * from sys.cluster");
                return stmt;
            })),
            // A parameter of a type the column cannot hold.
            Arguments.of("a parameter the column cannot hold", 1, batch(conn -> {
                PreparedStatement insert = conn.prepareStatement("insert into tbl (x) values (?)");
                insert.setObject(1, new HashMap<>());
                insert.addBatch();
                return insert;
            })),
            // A statement the server cannot parse.
            Arguments.of("a statement the server cannot parse", 1, batch(conn -> {
                PreparedStatement broken = conn.prepareStatement("insert tbl (x) values (?)");
                broken.setInt(1, 2);
                broken.addBatch();
                return broken;
            }))
        );
    }

    @FunctionalInterface
    interface BatchBuilder {
        Statement build(Connection conn) throws SQLException;
    }

    private static BatchBuilder batch(BatchBuilder builder) {
        return builder;
    }

    /**
     * A batch cleared before it runs has nothing to execute, and one asked for
     * its counts as longs answers in the same shape.
     */
    @Test
    public void aBatchCanBeClearedAndCanBeCountedAsLongs() throws Exception {
        try (var conn = connect()) {
            conn.createStatement().execute("CREATE TABLE tbl (x int, y int)");

            try (var insert = conn.prepareStatement("insert into tbl (x) values (?)")) {
                insert.setInt(1, 1);
                insert.addBatch();
                insert.clearBatch();
                assertThat(insert.executeBatch().length, is(0));

                insert.setInt(1, 2);
                insert.addBatch();
                insert.setInt(1, 3);
                insert.addBatch();
                assertThat(insert.executeLargeBatch(), is(new long[]{1L, 1L}));
            }
            assertThat(rowCount(conn), is(2L));
        }
    }

    private static int[] failedBatch(Statement statement) {
        return assertThrows(BatchUpdateException.class, statement::executeBatch).getUpdateCounts();
    }

    private static int[] oneRowEach(int statements) {
        int[] counts = new int[statements];
        Arrays.fill(counts, 1);
        return counts;
    }

    private static int[] allFailed(int statements) {
        int[] counts = new int[statements];
        Arrays.fill(counts, Statement.EXECUTE_FAILED);
        return counts;
    }

    private static long rowCount(java.sql.Connection conn) throws SQLException {
        conn.createStatement().execute("REFRESH TABLE tbl");
        ResultSet counted = conn.createStatement().executeQuery("SELECT count(*) FROM tbl");
        assertThat(counted.next(), is(true));
        return counted.getLong(1);
    }

    /**
     * Every way of asking for generated keys reaches the driver's own
     * result set, whether the statement names the columns or asks for all
     * of them.
     */
    @Test
    public void generatedKeysAreReadThroughTheDriver() throws Exception {
        try (var conn = connect()) {
            var stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS tbl");
            stmt.execute("CREATE TABLE tbl (x int, y int)");

            try (var insert = conn.prepareStatement(
                "INSERT INTO tbl (x, y) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                insert.setInt(1, 1);
                insert.setInt(2, 10);
                insert.execute();
                assertThat(insert.getGeneratedKeys(), is(instanceOf(CrateResultSet.class)));
            }

            try (var insert = conn.prepareStatement(
                "INSERT INTO tbl (x, y) VALUES (?, ?)", new String[]{"x"})) {
                insert.setInt(1, 2);
                insert.setInt(2, 20);
                insert.execute();

                var keys = insert.getGeneratedKeys();
                assertThat(keys.next(), is(true));
                assertThat(keys.getInt("x"), is(2));
            }

            assertThat(stmt.executeUpdate(
                "INSERT INTO tbl (x, y) VALUES (3, 30)", new String[]{"x"}), is(1));
            var keys = stmt.getGeneratedKeys();
            assertThat(keys.next(), is(true));
            assertThat(keys.getInt("x"), is(3));
        }
    }

    @Test
    public void prepareCallAcceptsResultSetOptions() throws Exception {
        try (var conn = connect()) {
            var stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS tbl");
            stmt.execute("CREATE TABLE tbl (x int, y int)");
            stmt.execute("INSERT INTO tbl (x, y) VALUES (1, 10)");
            stmt.execute("REFRESH TABLE tbl");

            try (var call = conn.prepareCall("SELECT x FROM tbl WHERE x = ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                call.setInt(1, 1);
                var results = call.executeQuery();
                assertThat(results.next(), is(true));
                assertThat(results.getInt(1), is(1));
            }

            try (var call = conn.prepareCall("SELECT x FROM tbl",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
                assertThat(call.execute(), is(true));
            }
        }
    }
}
