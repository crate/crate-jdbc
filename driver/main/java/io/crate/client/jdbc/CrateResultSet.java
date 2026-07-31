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

import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Surfaces CrateDB OBJECT columns (json on the wire) as
 * {@code Map<String, Object>}, matching what applications coded against
 * this driver expect from {@link #getObject}. All other behavior is stock
 * pgjdbc.
 */
public class CrateResultSet extends ForwardingResultSet {

    CrateResultSet(ResultSet delegate) {
        super(delegate);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return fromPg(delegate.getObject(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return fromPg(delegate.getObject(columnLabel));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == Map.class) {
            return (T) fromPg(delegate.getObject(columnIndex));
        }
        return delegate.getObject(columnIndex, type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        if (type == Map.class) {
            return (T) fromPg(delegate.getObject(columnLabel));
        }
        return delegate.getObject(columnLabel, type);
    }

    @Override
    public java.sql.Array getArray(int columnIndex) throws SQLException {
        java.sql.Array array = delegate.getArray(columnIndex);
        return array == null ? null : new CrateArray(array);
    }

    @Override
    public java.sql.Array getArray(String columnLabel) throws SQLException {
        java.sql.Array array = delegate.getArray(columnLabel);
        return array == null ? null : new CrateArray(array);
    }

    static Object fromPg(Object value) throws SQLException {
        if (value instanceof PGobject) {
            PGobject pgObject = (PGobject) value;
            if (CrateJson.isJsonType(pgObject.getType())) {
                String json = pgObject.getValue();
                return json == null ? null : CrateJson.parse(json);
            }
        }
        return value;
    }
}
