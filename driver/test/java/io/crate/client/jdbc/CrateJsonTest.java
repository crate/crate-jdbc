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

package io.crate.client.jdbc;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrateJsonTest {

    @Test
    public void mapRoundTripsThroughJson() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Trillian");
        map.put("age", 32);
        map.put("registered", true);

        assertThat(CrateJson.parse(CrateJson.write(map)), is(map));
    }

    @Test
    public void nestedMapsRoundTripThroughJson() throws SQLException {
        Map<String, Object> inner = new HashMap<>();
        inner.put("street", "Guildford");
        inner.put("number", 42);
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Arthur");
        map.put("address", inner);

        assertThat(CrateJson.parse(CrateJson.write(map)), is(map));
    }

    @Test
    public void listsRoundTripThroughJson() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("tags", Arrays.asList("a", "b", "c"));
        map.put("scores", Arrays.asList(1, 2, 3));

        assertThat(CrateJson.parse(CrateJson.write(map)), is(map));

        Object parsed = CrateJson.parse("[1, 2, 3]");
        assertThat(parsed, instanceOf(List.class));
        assertThat(parsed, is(Arrays.asList(1, 2, 3)));
    }

    @Test
    public void nullValuesArePreserved() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("present", "yes");
        map.put("absent", null);

        assertThat(CrateJson.parse(CrateJson.write(map)), is(map));
        assertThat(CrateJson.parse("null"), nullValue());
    }

    @Test
    public void invalidJsonRaisesSQLException() {
        assertThrows(SQLException.class, () -> CrateJson.parse("{not json"));
    }

    @Test
    public void toJsonObjectProducesJsonTypedPgObject() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("inner", "Zoon");

        PGobject pgObject = CrateJson.toJsonObject(map);
        assertThat(pgObject.getType(), is("json"));
        assertThat(CrateJson.parse(pgObject.getValue()), is(map));
    }

    @Test
    public void jsonAndJsonbAreRecognizedAsJsonTypes() {
        assertThat(CrateJson.isJsonType("json"), is(true));
        assertThat(CrateJson.isJsonType("jsonb"), is(true));
        assertThat(CrateJson.isJsonType("varchar"), is(false));
    }
}
