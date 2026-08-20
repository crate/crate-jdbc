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

import com.fasterxml.jackson.core.StreamReadConstraints;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrateJsonTest {

    @Test
    public void mapRoundTripsThroughJson() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Trillian");
        map.put("age", 32L);
        map.put("registered", true);

        assertThat(CrateJson.parse(CrateJson.write(map)), is(map));
    }

    @Test
    public void nestedMapsRoundTripThroughJson() throws SQLException {
        Map<String, Object> inner = new HashMap<>();
        inner.put("street", "Guildford");
        inner.put("number", 42L);
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Arthur");
        map.put("address", inner);

        assertThat(CrateJson.parse(CrateJson.write(map)), is(map));
    }

    @Test
    public void listsRoundTripThroughJson() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("tags", Arrays.asList("a", "b", "c"));
        map.put("scores", Arrays.asList(1L, 2L, 3L));

        assertThat(CrateJson.parse(CrateJson.write(map)), is(map));

        Object parsed = CrateJson.parse("[1, 2, 3]");
        assertThat(parsed, instanceOf(List.class));
        assertThat(parsed, is(Arrays.asList(1L, 2L, 3L)));
    }

    /**
     * A whole number in a CrateDB OBJECT is a {@code bigint} whatever its
     * magnitude, so reading one back must not size the Java type to the value.
     */
    @Test
    public void wholeNumbersReadBackAsLong() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("small", 5);
        map.put("large", 5_000_000_000L);

        Map<?, ?> parsed = (Map<?, ?>) CrateJson.parse(CrateJson.write(map));
        assertThat(parsed.get("small"), is(5L));
        assertThat(parsed.get("large"), is(5_000_000_000L));
    }

    /** CrateDB reads a timestamp from ISO-8601 text. */
    @Test
    public void javaTimeValuesAreWrittenAsIsoText() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("local", LocalDateTime.of(2026, 8, 5, 12, 0));
        map.put("offset", OffsetDateTime.parse("2026-08-05T12:00+02:00"));
        map.put("instant", Instant.parse("2026-08-05T10:00:00Z"));
        map.put("date", LocalDate.of(2026, 8, 5));

        Map<?, ?> written = (Map<?, ?>) CrateJson.parse(CrateJson.write(map));
        assertThat(written.get("local"), is("2026-08-05T12:00:00"));
        assertThat(written.get("offset"), is("2026-08-05T12:00:00+02:00"));
        assertThat(written.get("instant"), is("2026-08-05T10:00:00Z"));
        assertThat(written.get("date"), is("2026-08-05"));
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

    /**
     * A value that cannot be rendered is still a value the failure has to be
     * reported for, so building the message must not become the failure.
     */
    @Test
    public void aSelfReferencingValueIsReportedAsASQLException() {
        List<Object> outer = new ArrayList<>();
        List<Object> inner = new ArrayList<>();
        outer.add(inner);
        inner.add(outer);

        assertThrows(SQLException.class, () -> CrateJson.write(outer));
    }

    /** A message quotes back enough of a value to identify it, and no more. */
    @Test
    public void aLargeValueIsNotQuotedBackInFull() {
        Map<String, Object> map = new HashMap<>();
        map.put("blob", "x".repeat(100_000));
        map.put("unserializable", new Object() {
            @Override
            public String toString() {
                return "x".repeat(100_000);
            }
        });

        SQLException raised = assertThrows(SQLException.class, () -> CrateJson.write(map));
        assertThat(raised.getMessage().length(), lessThan(1_000));
    }

    /**
     * Jackson's ceiling on how long one string may be describes a document
     * arriving from somewhere untrusted. An OBJECT column's value is neither:
     * the server accepted it and this driver has it buffered already, so a
     * text field past that ceiling still reads.
     */
    @Test
    public void aStringPastJacksonsOwnCeilingIsStillRead() throws SQLException {
        String text = "x".repeat(StreamReadConstraints.DEFAULT_MAX_STRING_LEN + 1);

        Map<?, ?> read = CrateJson.parse("{\"text\":\"" + text + "\"}", Map.class);

        assertThat(read.get("text"), is(text));
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
