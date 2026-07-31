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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;

import java.sql.SQLException;

/**
 * JSON bridge between CrateDB's OBJECT type and java.util collections:
 * OBJECT columns travel over the PostgreSQL wire protocol as json, which
 * this driver surfaces as {@code Map<String, Object>} on reads and accepts
 * as {@code Map} on writes.
 */
final class CrateJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CrateJson() {
    }

    static Object parse(String json) throws SQLException {
        try {
            return MAPPER.readValue(json, Object.class);
        } catch (Exception e) {
            throw new SQLException("Cannot parse json value: " + json, e);
        }
    }

    static String write(Object value) throws SQLException {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new SQLException("Cannot serialize value to json: " + value, e);
        }
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
