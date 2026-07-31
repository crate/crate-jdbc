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

import java.sql.Connection;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Pins {@code executeUpdate} row counts: DDL reports a single affected
 * row, and operations for which CrateDB cannot determine an exact count
 * (such as deleting from a partitioned table by partition) report zero.
 */
public class RowCountIT extends BaseIntegrationTest {

    @AfterEach
    void tearDownTables() {
        dropAllUserTables();
    }

    @Test
    public void ddlReportsOneAffectedRow() throws Exception {
        try (Connection conn = connect()) {
            Statement statement = conn.createStatement();
            assertThat(statement.executeUpdate("create table t (id int)"), is(1));
        }
    }

    @Test
    public void unknownRowCountReportsZero() throws Exception {
        try (Connection conn = connect()) {
            Statement statement = conn.createStatement();
            statement.execute("create table t (p string, x int) partitioned by (p)");
            statement.execute("insert into t (p, x) values ('a', 1)");
            statement.execute("refresh table t");

            assertThat(statement.executeUpdate("delete from t where p = 'a'"), is(0));
        }
    }
}
