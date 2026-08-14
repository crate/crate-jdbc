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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;

/**
 * JSON bridge between CrateDB's OBJECT type and java.util collections. OBJECT
 * columns travel as json, which this driver reads as {@code Map<String,
 * Object>} and accepts as {@code Map}.
 */
final class CrateJson {

    private static final ObjectMapper MAPPER = mapper();

    /**
     * How values cross between Java and CrateDB's json.
     *
     * <ul>
     * <li>{@code java.time} values travel as the ISO-8601 text CrateDB reads a
     *     timestamp from. Under Jackson's other option, epoch numbers, a value
     *     without a zone has no epoch to write and comes out as an array of its
     *     fields.</li>
     * <li>A whole number is read back as a {@code Long}, because a whole number
     *     in a CrateDB OBJECT is a {@code bigint} whatever its magnitude.
     *     Sizing the Java type to the value instead would make the type of what
     *     a column reads as depend on the row.</li>
     * <li>No ceiling on how long a single value may be. Jackson's guards
     *     against a hostile document (20 million characters to a string, 1000
     *     digits to a number) describe input arriving from somewhere untrusted.
     *     What is read here is a column value the server accepted and this
     *     driver has already buffered, so refusing to parse it would only make
     *     data other clients can read unreadable through this one.</li>
     * <li>The nesting ceiling stays, being about the stack instead of trust.
     *     Reading and writing json recurse, and a deep enough structure would
     *     exhaust the stack instead of raising something a caller can act
     *     on.</li>
     * </ul>
     */
    private static ObjectMapper mapper() {
        JsonFactory factory = JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder()
                .maxStringLength(Integer.MAX_VALUE)
                .maxNumberLength(Integer.MAX_VALUE)
                .build())
            .build();
        return JsonMapper.builder(factory)
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(DeserializationFeature.USE_LONG_FOR_INTS)
            .build();
    }

    private CrateJson() {
    }

    static Object parse(String json) throws SQLException {
        return parse(json, Object.class);
    }

    static <T> T parse(String json, Class<T> type) throws SQLException {
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw new SQLDataException("Cannot parse json value: " + describe(json, e),
                PSQLState.DATA_ERROR.getState(), e);
        }
    }

    static String write(Object value) throws SQLException {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new SQLDataException("Cannot serialize value to json: " + describe(value, e),
                PSQLState.INVALID_PARAMETER_VALUE.getState(), e);
        }
    }

    /** How much of a value a message quotes back. */
    private static final int DESCRIPTION_LIMIT = 200;

    /**
     * A value as a message can afford to show it. What failed to convert can
     * be a whole row, and building a second copy of one while reporting that it
     * was too much to handle is the wrong move twice over: an OBJECT column's
     * contents do not belong in a log either.
     *
     * <p>Rendering it whole is unsafe as well. A collection that holds itself
     * recurses until the stack is gone, replacing the failure being reported
     * with a {@code StackOverflowError}.</p>
     */
    private static String describe(Object value, Exception failure) {
        String limit = failure instanceof StreamConstraintsException
            ? " (" + failure.getMessage() + ")"
            : "";
        String text;
        try {
            text = String.valueOf(value);
        } catch (Throwable unprintable) {
            return "a " + value.getClass().getName() + " that cannot be printed" + limit;
        }
        return (text.length() <= DESCRIPTION_LIMIT
            ? text
            : text.substring(0, DESCRIPTION_LIMIT) + "… (" + text.length() + " characters)") + limit;
    }

    static PGobject toJsonObject(Object value) throws SQLException {
        PGobject pgObject = new PGobject();
        pgObject.setType("json");
        pgObject.setValue(write(value));
        return pgObject;
    }

    static boolean isJsonType(String pgTypeName) {
        return "json".equals(pgTypeName) || "jsonb".equals(pgTypeName);
    }
}
