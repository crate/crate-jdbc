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

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * CrateDB-aware connection behavior on top of stock pgjdbc:
 *
 * <ul>
 * <li>{@link #rollback()} is a client-side no-op. CrateDB has no
 *     transactions; it accepts {@code BEGIN}/{@code COMMIT} as no-ops but
 *     {@code ROLLBACK} is not part of its SQL grammar, so forwarding it
 *     would raise a server error for every framework that calls
 *     {@code rollback()} as part of routine cleanup.</li>
 * <li>{@link #createArrayOf} accepts CrateDB type names ({@code string},
 *     {@code long}, {@code object}, ...) in addition to the PostgreSQL
 *     names pgjdbc resolves against the server.</li>
 * <li>Statements, prepared statements and metadata are handed out as their
 *     Crate* wrappers, which provide OBJECT&harr;{@code Map} conversion and
 *     metadata catalog tolerance.</li>
 * </ul>
 */
public class CrateConnection extends ForwardingConnection {

    /**
     * CrateDB type names mapped to array element type names pgjdbc can
     * resolve against CrateDB's pg_catalog.pg_type.
     */
    private static final Map<String, String> ARRAY_TYPE_ALIASES = new HashMap<>();

    static {
        ARRAY_TYPE_ALIASES.put("string", "varchar");
        ARRAY_TYPE_ALIASES.put("text", "text");
        ARRAY_TYPE_ALIASES.put("ip", "varchar");
        ARRAY_TYPE_ALIASES.put("boolean", "bool");
        ARRAY_TYPE_ALIASES.put("byte", "int2");
        ARRAY_TYPE_ALIASES.put("short", "int2");
        ARRAY_TYPE_ALIASES.put("integer", "int4");
        ARRAY_TYPE_ALIASES.put("long", "int8");
        ARRAY_TYPE_ALIASES.put("float", "float4");
        ARRAY_TYPE_ALIASES.put("real", "float4");
        ARRAY_TYPE_ALIASES.put("double", "float8");
        ARRAY_TYPE_ALIASES.put("timestamp", "timestamptz");
        ARRAY_TYPE_ALIASES.put("timestamptz", "timestamptz");
        ARRAY_TYPE_ALIASES.put("object", "json");
        ARRAY_TYPE_ALIASES.put("geo_point", "float8");
        ARRAY_TYPE_ALIASES.put("geo_shape", "json");
    }

    public CrateConnection(Connection delegate) {
        super(delegate);
    }

    @Override
    public void rollback() {
        // ROLLBACK is not in CrateDB's grammar; there is nothing to roll back.
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("CrateDB does not support savepoints");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        String pgTypeName = ARRAY_TYPE_ALIASES.getOrDefault(typeName, typeName);
        Object[] pgElements = elements;
        if ("json".equals(pgTypeName) && elements != null) {
            pgElements = new Object[elements.length];
            for (int i = 0; i < elements.length; i++) {
                Object element = elements[i];
                pgElements[i] = element instanceof Map ? CrateJson.write(element) : element;
            }
        }
        return delegate.createArrayOf(pgTypeName, pgElements);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new CrateStatement(delegate.createStatement(), this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new CrateStatement(delegate.createStatement(resultSetType, resultSetConcurrency), this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new CrateStatement(delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql, autoGeneratedKeys), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql, columnIndexes), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql, columnNames), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql, resultSetType, resultSetConcurrency), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new CratePreparedStatement(
            delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new CrateDatabaseMetaData(delegate.getMetaData(), this);
    }
}
