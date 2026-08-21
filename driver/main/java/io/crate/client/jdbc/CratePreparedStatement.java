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
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */
package io.crate.client.jdbc;

import java.sql.Array;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;

/**
 * Adds binding parameters the way CrateDB expects them (see
 * {@link CrateParameters}) to what {@link CrateStatement} already does with
 * running them.
 */
public class CratePreparedStatement extends ForwardingPreparedStatement {

    CratePreparedStatement(PreparedStatement delegate, CrateConnection connection) throws SQLException {
        super(delegate, connection);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x, targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x, targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        CrateParameters.bindArray(preparedDelegate, parameterIndex, x);
    }

    /**
     * What the rows this statement would produce hold, in the terms this
     * driver reads them in, matching what the rows themselves report.
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSetMetaData metaData = preparedDelegate.getMetaData();
        return metaData == null ? null : new CrateResultSetMetaData(metaData);
    }

    /**
     * What this statement's parameters take, in the terms this driver binds
     * them in, matching the forms {@link #setObject} accepts.
     */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        ParameterMetaData metaData = preparedDelegate.getParameterMetaData();
        return metaData == null ? null : new CrateParameterMetaData(metaData);
    }

    /**
     * Binds the parameter when CrateDB needs it in a form of its own, and
     * reports whether it did. A value that needs no conversion is left to the
     * caller, which hands it to pgJDBC with the target type it was given.
     */
    private boolean bound(int parameterIndex, Object x) throws SQLException {
        Object converted = CrateParameters.toPg(x, getConnection());
        if (converted == x) {
            return false;
        }
        CrateParameters.bind(preparedDelegate, parameterIndex, converted);
        return true;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ResultSet rows;
        try (CrateQueryTimeout bound = bounded()) {
            rows = preparedDelegate.executeQuery();
        }
        return resultSet(rows);
    }

    @Override
    public boolean execute() throws SQLException {
        try (CrateQueryTimeout bound = bounded()) {
            return preparedDelegate.execute();
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        try (CrateQueryTimeout bound = bounded()) {
            return preparedDelegate.executeUpdate();
        }
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        try (CrateQueryTimeout bound = bounded()) {
            return preparedDelegate.executeLargeUpdate();
        }
    }
}
