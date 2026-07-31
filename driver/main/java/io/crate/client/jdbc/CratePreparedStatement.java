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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Map;

/**
 * Binds {@code Map} parameters as json so they land in CrateDB OBJECT
 * columns (stock pgjdbc would try the PostgreSQL hstore extension, which
 * CrateDB does not provide), and hands out {@link CrateResultSet}s.
 */
public class CratePreparedStatement extends ForwardingPreparedStatement {

    private final CrateConnection connection;

    CratePreparedStatement(PreparedStatement delegate, CrateConnection connection) {
        super(delegate);
        this.connection = connection;
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x instanceof Map) {
            delegate.setObject(parameterIndex, CrateJson.toJsonObject(x));
        } else {
            delegate.setObject(parameterIndex, x);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (x instanceof Map) {
            delegate.setObject(parameterIndex, CrateJson.toJsonObject(x));
        } else {
            delegate.setObject(parameterIndex, x, targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        if (x instanceof Map) {
            delegate.setObject(parameterIndex, CrateJson.toJsonObject(x));
        } else {
            delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        if (x instanceof Map) {
            delegate.setObject(parameterIndex, CrateJson.toJsonObject(x));
        } else {
            delegate.setObject(parameterIndex, x, targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        if (x instanceof Map) {
            delegate.setObject(parameterIndex, CrateJson.toJsonObject(x));
        } else {
            delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return new CrateResultSet(delegate.executeQuery());
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return new CrateResultSet(delegate.executeQuery(sql));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet rs = delegate.getResultSet();
        return rs == null ? null : new CrateResultSet(rs);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new CrateResultSet(delegate.getGeneratedKeys());
    }

    @Override
    public Connection getConnection() {
        return connection;
    }
}
