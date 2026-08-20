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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What {@code preferQueryMode=simple} costs, which {@code docs/limitations.rst}
 * states to users. The mode writes parameters into the statement text instead
 * of sending them beside it, so a value the server has to type for itself
 * arrives as text with nothing to be typed from.
 */
public class SimpleQueryModeIT extends BaseIntegrationTest {

    @BeforeEach
    void setUpTable() throws Exception {
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute("drop table if exists simple_mode");
            statement.execute(
                "create table simple_mode ("
                + " id integer,"
                + " flags array(array(boolean))"
                + ") clustered into 1 shards with (number_of_replicas=0)");
        }
        ensureYellow();
    }

    @AfterEach
    void dropTable() throws Exception {
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute("drop table if exists simple_mode");
        }
    }

    /**
     * Under this mode the driver binds what the server can type and refuses
     * what it cannot. A sub-array has no type the server can take from the
     * text it arrives as, and the column it lands in is reached too late to
     * supply one; a plain integer needs none.
     */
    @Test
    public void simpleQueryModeBindsWhatTheServerCanTypeAndRefusesTheRest() throws Exception {
        try (Connection conn = connectWith("preferQueryMode", "simple")) {
            try (PreparedStatement insert =
                     conn.prepareStatement("insert into simple_mode (id) values (?)")) {
                insert.setInt(1, 1);
                insert.execute();
            }
            try (PreparedStatement insert = conn.prepareStatement(
                     "insert into simple_mode (id, flags) values (?, ?)")) {
                insert.setInt(1, 2);
                insert.setObject(2, new Object[][]{{true, false}, {true}});
                assertThrows(SQLException.class, insert::execute);
            }
        }
        assertThat(idsInTable(), contains(1));
    }

    private static List<Integer> idsInTable() throws SQLException {
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute("refresh table simple_mode");
            List<Integer> ids = new ArrayList<>();
            try (ResultSet resultSet = statement.executeQuery(
                    "select id from simple_mode order by id")) {
                while (resultSet.next()) {
                    ids.add(resultSet.getInt(1));
                }
            }
            return ids;
        }
    }
}
