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

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Calling a CrateDB user-defined function through JDBC's {@code {call f(?)}}
 * syntax, how a framework invokes one without writing CrateDB's own
 * SQL.
 *
 * <p>CrateDB's user-defined routines are functions, not stored procedures:
 * they return a value rather than filling in output parameters. pgJDBC turns
 * the call into a {@code SELECT}, so the answer arrives as a row, and the
 * output-parameter half of the API has nothing behind it. Both halves are
 * pinned here — the second because "not supported" is a promise to callers as
 * much as the first is, and because the day either layer implements it is a
 * day this driver has a conversion to get right.</p>
 */
public class FunctionsIT extends BaseIntegrationTest {

    private static Connection conn;

    @BeforeAll
    static void createFunctions() throws Exception {
        dropAllUserTables();
        conn = connect();
        try (Statement statement = conn.createStatement()) {
            statement.execute(
                "create or replace function doubled(bigint) returns bigint"
                + " language javascript as 'function doubled(a) { return a * 2; }'");
            statement.execute(
                "create or replace function boxed(bigint) returns object"
                + " language javascript as 'function boxed(a) { return {\"n\": a}; }'");
            statement.execute(
                "create or replace function listed(bigint) returns array(bigint)"
                + " language javascript as 'function listed(a) { return [a, a + 1]; }'");
        }
    }

    @AfterAll
    static void dropFunctions() throws Exception {
        try (Statement statement = conn.createStatement()) {
            statement.execute("drop function if exists doubled(bigint)");
            statement.execute("drop function if exists boxed(bigint)");
            statement.execute("drop function if exists listed(bigint)");
        } finally {
            conn.close();
        }
    }

    /** The call runs and its answer arrives as the single row of a result. */
    @Test
    public void aFunctionCalledThroughTheEscapeAnswersWithARow() throws Exception {
        try (CallableStatement call = conn.prepareCall("{call doubled(?)}")) {
            call.setLong(1, 21);
            try (ResultSet resultSet = call.executeQuery()) {
                assertThat(resultSet.next(), is(true));
                assertThat(resultSet.getLong(1), is(42L));
                assertThat(resultSet.next(), is(false));
            }
        }
    }

    /**
     * A function returning an OBJECT reads back as a {@code Map}, the way an
     * OBJECT column does. Coming through a call changes nothing about it.
     */
    @Test
    public void aFunctionReturningAnObjectAnswersWithAMap() throws Exception {
        try (CallableStatement call = conn.prepareCall("{call boxed(?)}")) {
            call.setLong(1, 7);
            try (ResultSet resultSet = call.executeQuery()) {
                assertThat(resultSet.next(), is(true));
                Object value = resultSet.getObject(1);
                assertThat(value, is(instanceOf(Map.class)));
                assertThat(((Map<?, ?>) value).get("n"), is(7L));
            }
        }
    }

    /** A function returning an array reads back as a {@link Array}, likewise. */
    @Test
    public void aFunctionReturningAnArrayAnswersWithAnArray() throws Exception {
        try (CallableStatement call = conn.prepareCall("{call listed(?)}")) {
            call.setLong(1, 4);
            try (ResultSet resultSet = call.executeQuery()) {
                assertThat(resultSet.next(), is(true));
                Array array = resultSet.getArray(1);
                assertThat((Object[]) array.getArray(),
                    org.hamcrest.Matchers.<Object>arrayContaining(4L, 5L));
            }
        }
    }

    /**
     * The form that asks for the return value as an output parameter is
     * refused by the server: describing it needs a PostgreSQL type CrateDB has
     * no equivalent for.
     */
    @Test
    public void askingForTheReturnValueAsAnOutputParameterIsRefused() throws Exception {
        try (CallableStatement call = conn.prepareCall("{? = call doubled(?)}")) {
            call.registerOutParameter(1, Types.BIGINT);
            call.setLong(2, 21);
            assertThrows(SQLException.class, call::execute);
        }
    }

    /**
     * Reading an output parameter from a call that declared none is refused
     * before the driver's own conversion is reached, so a caller gets the
     * complaint rather than a null.
     */
    @Test
    public void readingAnOutputParameterThatWasNeverDeclaredIsRefused() throws Exception {
        try (CallableStatement call = conn.prepareCall("{call doubled(?)}")) {
            call.setLong(1, 21);
            call.execute();
            assertThrows(SQLException.class, () -> call.getObject(1));
            assertThrows(SQLException.class, () -> call.getObject(1, Map.class));
            assertThrows(SQLException.class, () -> call.getArray(1));
        }
    }

    /**
     * Addressing an output parameter by name is unsupported one layer down —
     * pgJDBC implements none of those forms. CrateDB addresses a function's
     * arguments by position anyway.
     */
    @Test
    public void addressingAnOutputParameterByNameIsUnsupported() throws Exception {
        try (CallableStatement call = conn.prepareCall("{call doubled(?)}")) {
            call.setLong(1, 21);
            call.execute();
            assertThrows(SQLFeatureNotSupportedException.class, () -> call.getObject("a"));
            assertThrows(SQLFeatureNotSupportedException.class, () -> call.getObject("a", Map.class));
            assertThrows(SQLFeatureNotSupportedException.class, () -> call.getArray("a"));
        }
    }
}
