/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ArrayITest extends BaseIntegrationTest {

    private static Connection conn;

    private static Array objects;

    @BeforeAll
    static void setUpArray() throws Exception {
        dropAllUserTables();
        conn = connect();
        try (Statement statement = conn.createStatement()) {
            statement.execute(
                "create table arrays (objs array(object as (n integer)))"
                + " clustered into 1 shards with (number_of_replicas=0)");
            statement.execute("insert into arrays (objs) values ([{n=1}, {n=2}, {n=3}])");
            statement.execute("refresh table arrays");
        }
        try (Statement statement = conn.createStatement()) {
            ResultSet row = statement.executeQuery("select objs from arrays");
            row.next();
            objects = row.getArray(1);
        }
    }

    @AfterAll
    static void dropTable() throws Exception {
        if (conn != null) {
            conn.close();
        }
        dropAllUserTables();
    }

    private static List<Object> valuesOf(Object slice) {
        List<Object> values = new ArrayList<>();
        for (Object element : (Object[]) slice) {
            values.add(((Map<?, ?>) element).get("n"));
        }
        return values;
    }

    private static List<Object> rowValues(ResultSet rows, int column) throws SQLException {
        List<Object> values = new ArrayList<>();
        while (rows.next()) {
            values.add(rows.getObject(column));
        }
        return values;
    }

    @Test
    public void testGetArraySlice() throws Exception {
        assertThat(valuesOf(objects.getArray(1, 2)), is(List.of(1, 2)));
        assertThat(valuesOf(objects.getArray(3, 1)), is(List.of(3)));
        assertThat(valuesOf(objects.getArray(1, 0)), is(List.of(1, 2, 3)));
    }

    @Test
    public void testGetArraySliceOutOfBounds() {
        assertThrows(SQLException.class, () -> objects.getArray(0, 1));
        assertThrows(SQLException.class, () -> objects.getArray(1, 99));
    }

    @Test
    public void testGetArrayWithEmptyTypeMap() throws Exception {
        assertThat(valuesOf(objects.getArray(Map.of())), is(List.of(1, 2, 3)));
        assertThat(valuesOf(objects.getArray(1, 2, Map.of())), is(List.of(1, 2)));
    }

    @Test
    public void testGetResultSet() throws Exception {
        ResultSet rows = objects.getResultSet();

        assertThat(rows.next(), is(true));
        assertThat(rows.getInt(1), is(1));
        assertThat(rows.getObject(2), is(instanceOf(Map.class)));
        assertThat(((Map<?, ?>) rows.getObject(2)).get("n"), is(1));
        assertThat(rowValues(rows, 1), is(List.of(2, 3)));
    }

    @Test
    public void testGetResultSetSlice() throws Exception {
        assertThat(rowValues(objects.getResultSet(2, 2), 1), is(List.of(2, 3)));
        assertThat(rowValues(objects.getResultSet(1, 1, Map.of()), 1), is(List.of(1)));
    }

    @Test
    public void testGetBaseType() throws Exception {
        assertThat(objects.getBaseTypeName(), is("json"));
        assertThat(objects.getBaseType(), is(Types.JAVA_OBJECT));
    }

    @Test
    public void testToString() {
        assertThat(objects.toString(), containsString("\\\"n\\\":1"));
    }
}
