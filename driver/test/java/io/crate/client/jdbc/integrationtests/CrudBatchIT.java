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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Pins the everyday statement workflows an application relies on: CRUD
 * through statements and prepared statements, batch inserts, and
 * {@code prepareCall} used as a plain parameterized statement.
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
    public void batchInsertIsSupported() throws Exception {
        try (var conn = connect()) {
            var stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS tbl");
            stmt.execute("CREATE TABLE tbl (x int, y int)");

            try (var insert = conn.prepareStatement("INSERT INTO tbl (x, y) VALUES (?, ?)")) {
                for (int i = 0; i < 20; i++) {
                    insert.setInt(1, i);
                    insert.setInt(2, i * 10);
                    insert.addBatch();
                }
                int[] results = insert.executeBatch();
                assertThat(results.length, is(20));
            }

            stmt.execute("REFRESH TABLE tbl");

            var results = conn.createStatement().executeQuery("SELECT count(*) FROM tbl");
            assertThat(results.next(), is(true));
            assertThat(results.getInt(1), is(20));
            assertThat(results.next(), is(false));
        }
    }
}
