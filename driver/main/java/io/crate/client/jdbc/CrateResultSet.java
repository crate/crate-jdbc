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
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;

/**
 * Rows as this driver reads them: a CrateDB OBJECT column, json on the wire,
 * comes back as a {@code Map<String, Object>}, and a column of nested arrays
 * as a {@link java.sql.Array}. Those are the two shapes pgJDBC has no decoder
 * for; every other column is its.
 */
public class CrateResultSet extends ForwardingResultSet {

    private final Statement statement;

    private CrateResultSetMetaData metaData;

    CrateResultSet(ResultSet delegate, Statement statement) {
        super(delegate);
        this.statement = statement;
    }

    /**
     * What the columns hold, in the terms this result set reads them in — one
     * object per result set, as pgJDBC hands it out.
     */
    @Override
    public CrateResultSetMetaData getMetaData() throws SQLException {
        if (metaData == null) {
            metaData = new CrateResultSetMetaData(delegate.getMetaData());
        }
        return metaData;
    }

    /**
     * The statement that produced this result set, as a wrapper: navigating
     * from a result set back to its connection stays inside this driver.
     *
     * <p>The wrapper is held rather than read from the delegate, so the
     * delegate is asked whether the result set is still open — a closed one
     * has nothing to navigate from, and answering would hand out a way around
     * that.</p>
     */
    @Override
    public Statement getStatement() throws SQLException {
        delegate.getStatement();
        return statement;
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
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return fromPg(delegate.getObject(columnIndex, map));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return fromPg(delegate.getObject(columnLabel, map));
    }

    /**
     * The type a caller asks for is pgJDBC's to answer for every column but a
     * json one, whose value it would hand over as text. Which column is which
     * is decided before the value is read, so that a column pgJDBC can only
     * decode into the requested type is never read untyped first.
     *
     * <p>An array is this driver's to answer whatever the column: asking for
     * one by type reads the same array as {@link #getArray(int)}, down to the
     * conversion of its elements and to the json columns pgJDBC has no array
     * decoder for.</p>
     */
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == java.sql.Array.class) {
            return type.cast(getArray(columnIndex));
        }
        if (isJsonColumn(columnIndex)) {
            T converted = asType(delegate.getObject(columnIndex), type);
            if (converted != null) {
                return converted;
            }
        }
        return delegate.getObject(columnIndex, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(delegate.findColumn(columnLabel), type);
    }

    /**
     * A column of nested arrays arrives as json, which pgJDBC has no array
     * decoder for; it is read here instead. Every other array column is
     * pgJDBC's, wrapped so its elements convert like the rest.
     */
    @Override
    public java.sql.Array getArray(int columnIndex) throws SQLException {
        if (isJsonColumn(columnIndex)) {
            String json = delegate.getString(columnIndex);
            return json == null ? null : CrateJsonArray.of(json);
        }
        java.sql.Array array = delegate.getArray(columnIndex);
        return array == null ? null : new CrateArray(array);
    }

    @Override
    public java.sql.Array getArray(String columnLabel) throws SQLException {
        return getArray(delegate.findColumn(columnLabel));
    }

    /** Whether a column carries json: an OBJECT, a geo_shape, nested arrays. */
    private boolean isJsonColumn(int columnIndex) throws SQLException {
        return getMetaData().isJson(columnIndex);
    }

    /**
     * A value as this driver surfaces it: an OBJECT as a {@code Map}, an
     * array as a {@link CrateArray} so its elements are converted in turn.
     * Everything else is what pgJDBC read.
     */
    static Object fromPg(Object value) throws SQLException {
        if (value instanceof PGobject) {
            PGobject pgObject = (PGobject) value;
            if (CrateJson.isJsonType(pgObject.getType())) {
                String json = pgObject.getValue();
                return json == null ? null : CrateJson.parse(json);
            }
        }
        if (value instanceof java.sql.Array) {
            return new CrateArray((java.sql.Array) value);
        }
        return value;
    }

    /**
     * A json value as the requested Java type, or null when the value is not
     * one this driver converts — pgJDBC answers those itself. An OBJECT reads
     * into any {@code Map} type and a column of nested arrays into any
     * {@code Collection} type.
     */
    @SuppressWarnings("unchecked")
    static <T> T asType(Object value, Class<T> type) throws SQLException {
        if (type == null || !(value instanceof PGobject)) {
            return null;
        }
        PGobject pgObject = (PGobject) value;
        if (!CrateJson.isJsonType(pgObject.getType())) {
            return null;
        }
        String json = pgObject.getValue();
        if (json == null) {
            return null;
        }
        if (Map.class.isAssignableFrom(type) || Collection.class.isAssignableFrom(type)) {
            return CrateJson.parse(json, type);
        }
        if (type == Object.class) {
            return (T) CrateJson.parse(json);
        }
        return null;
    }
}
