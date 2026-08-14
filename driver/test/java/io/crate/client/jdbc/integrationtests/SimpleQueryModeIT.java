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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What {@code preferQueryMode=simple} costs. The mode writes parameters into
 * the statement text instead of sending them beside it, so values the server
 * has to type for itself arrive as text, and a request to describe a
 * statement has no message to travel in.
 *
 * <p>The second of those has two outcomes, and which one a caller meets is
 * decided by a JVM flag rather than by anything in the driver: an assertion
 * inside pgJDBC catches the describe request, and without assertions the
 * request falls through into the simple-query path and executes the
 * statement. Both are pinned here, each under an assumption naming the
 * setting it holds for, so the class covers one outcome under
 * {@code integrationTest} and the other under
 * {@code integrationTestNoAssertions}.</p>
 */
public class SimpleQueryModeIT extends BaseIntegrationTest {

    /**
     * Whether this JVM was started with {@code -ea}. The assignment runs only
     * when assertions do.
     */
    private static boolean assertionsEnabled() {
        boolean enabled = false;
        assert enabled = true;
        return enabled;
    }

    private static Connection simpleMode() throws SQLException {
        return connectWith("preferQueryMode", "simple");
    }

    @BeforeEach
    void setUpTable() throws Exception {
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute("drop table if exists simple_mode");
            statement.execute(
                "create table simple_mode ("
                + " id integer,"
                + " tags array(text),"
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

    /**
     * A value the server can read from the statement text binds as it does
     * under the default mode. Without this the failures below would say
     * nothing about arrays in particular.
     */
    @Test
    public void valuesTheServerCanTypeBindUnderSimpleQueryMode() throws Exception {
        try (Connection conn = simpleMode();
             PreparedStatement insert =
                 conn.prepareStatement("insert into simple_mode (id) values (?)")) {
            insert.setInt(1, 1);
            insert.execute();
        }
        assertThat(idsInTable(), contains(1));
    }

    /**
     * A sub-array has no type the server can take from the text it arrives
     * as, and the column it lands in is reached too late to supply one.
     */
    @Test
    public void aNestedArrayCannotBindUnderSimpleQueryMode() throws Exception {
        try (Connection conn = simpleMode();
             PreparedStatement insert =
                 conn.prepareStatement("insert into simple_mode (id, flags) values (?, ?)")) {
            insert.setInt(1, 2);
            insert.setObject(2, new Object[][]{{true, false}, {true}});
            assertThrows(SQLException.class, insert::execute);
        }
        assertThat(idsInTable(), is(empty()));
    }

    /** An empty array names no type either, for the same reason. */
    @Test
    public void anEmptyArrayCannotBindUnderSimpleQueryMode() throws Exception {
        try (Connection conn = simpleMode();
             PreparedStatement insert =
                 conn.prepareStatement("insert into simple_mode (id, tags) values (?, ?)")) {
            insert.setInt(1, 3);
            insert.setObject(2, new String[]{});
            assertThrows(SQLException.class, insert::execute);
        }
        assertThat(idsInTable(), is(empty()));
    }

    /**
     * With assertions on, pgJDBC catches the describe request itself, so the
     * statement is not executed and nothing is written.
     */
    @Test
    public void describingAnUnexecutedStatementIsCaughtByAnAssertion() throws Exception {
        assumeTrue(assertionsEnabled(), "pgJDBC's own assertion needs -ea");
        try (Connection conn = simpleMode()) {
            try (PreparedStatement insert =
                     conn.prepareStatement("insert into simple_mode (id) values (?)")) {
                insert.setInt(1, 4);
                assertThrows(AssertionError.class, insert::getMetaData);
            }
            try (PreparedStatement insert =
                     conn.prepareStatement("insert into simple_mode (id) values (?)")) {
                insert.setInt(1, 5);
                assertThrows(AssertionError.class, insert::getParameterMetaData);
            }
        }
        assertThat(idsInTable(), is(empty()));
    }

    /**
     * Without assertions, so in every JVM not started with {@code -ea},
     * the describe request falls through into the simple-query path and the
     * statement runs. Asking a statement to describe itself writes to the
     * database, and {@code getMetaData()} reports nothing for the trouble.
     */
    @Test
    public void describingAnUnexecutedStatementExecutesItWithoutAssertions() throws Exception {
        assumeFalse(assertionsEnabled(), "the fall-through only happens without -ea");
        try (Connection conn = simpleMode()) {
            try (PreparedStatement insert =
                     conn.prepareStatement("insert into simple_mode (id) values (?)")) {
                insert.setInt(1, 4);
                assertThat(insert.getMetaData(), is(nullValue()));
            }
            try (PreparedStatement insert =
                     conn.prepareStatement("insert into simple_mode (id) values (?)")) {
                insert.setInt(1, 5);
                assertThat(insert.getParameterMetaData(), is(notNullValue()));
            }
        }
        assertThat(idsInTable(), contains(4, 5));
    }

    /**
     * A query that has run carries its own result description, so
     * {@code getMetaData()} answers from it without asking the server. That
     * is the one metadata call this mode leaves usable.
     */
    @Test
    public void describingAnExecutedQueryIsSafeUnderSimpleQueryMode() throws Exception {
        try (Connection conn = simpleMode()) {
            try (PreparedStatement insert =
                     conn.prepareStatement("insert into simple_mode (id) values (?)")) {
                insert.setInt(1, 6);
                insert.execute();
            }
            try (PreparedStatement select =
                     conn.prepareStatement("select id from simple_mode where id = ?")) {
                select.setInt(1, 6);
                select.executeQuery();
                assertThat(select.getMetaData(), is(notNullValue()));
            }
        }
        assertThat(idsInTable(), contains(6));
    }

    /**
     * {@code getParameterMetaData()} asks the server every time, executed
     * statement or not — parameters are described by a request of their own,
     * which this mode cannot carry. So the hazard has no safe side: on a
     * statement that has already run, the fall-through runs it a second time.
     */
    @Test
    public void parameterMetadataAlwaysAsksTheServerUnderSimpleQueryMode() throws Exception {
        try (Connection conn = simpleMode();
             PreparedStatement insert =
                 conn.prepareStatement("insert into simple_mode (id) values (?)")) {
            insert.setInt(1, 7);
            insert.execute();

            if (assertionsEnabled()) {
                assertThrows(AssertionError.class, insert::getParameterMetaData);
                assertThat(idsInTable(), contains(7));
            } else {
                assertThat(insert.getParameterMetaData(), is(notNullValue()));
                assertThat(idsInTable(), contains(7, 7));
            }
        }
    }
}
