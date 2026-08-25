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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Adds binding parameters the way CrateDB expects them (see
 * {@link CrateParameters}) to what {@link CrateStatement} already does with
 * running them.
 */
@SuppressWarnings("deprecation")
public class CratePreparedStatement extends CrateStatement implements PreparedStatement {

    protected final PreparedStatement preparedDelegate;

    CratePreparedStatement(PreparedStatement delegate, CrateConnection connection) throws SQLException {
        super(delegate, connection);
        this.preparedDelegate = delegate;
    }

    @Adapted
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x);
        }
    }

    @Adapted
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x, targetSqlType);
        }
    }

    @Adapted
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    @Adapted
    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x, targetSqlType);
        }
    }

    @Adapted
    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        if (!bound(parameterIndex, x)) {
            preparedDelegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    @Adapted
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        CrateParameters.bindArray(preparedDelegate, parameterIndex, x);
    }

    /**
     * What the rows this statement would produce hold, in the terms this
     * driver reads them in, matching what the rows themselves report.
     */
    @Adapted
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSetMetaData metaData = preparedDelegate.getMetaData();
        return metaData == null ? null : new CrateResultSetMetaData(metaData);
    }

    /**
     * What this statement's parameters take, in the terms this driver binds
     * them in, matching the forms {@link #setObject} accepts.
     */
    @Adapted
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

    @Adapted
    @Override
    public ResultSet executeQuery() throws SQLException {
        ResultSet rows;
        try (CrateQueryTimeout bound = bounded()) {
            rows = preparedDelegate.executeQuery();
        }
        return resultSet(rows);
    }

    @Adapted
    @Override
    public boolean execute() throws SQLException {
        try (CrateQueryTimeout bound = bounded()) {
            return preparedDelegate.execute();
        }
    }

    @Adapted
    @Override
    public int executeUpdate() throws SQLException {
        try (CrateQueryTimeout bound = bounded()) {
            return preparedDelegate.executeUpdate();
        }
    }

    @Adapted
    @Override
    public long executeLargeUpdate() throws SQLException {
        try (CrateQueryTimeout bound = bounded()) {
            return preparedDelegate.executeLargeUpdate();
        }
    }

    // <editor-fold defaultstate="collapsed" desc="Delegation to pgJDBC (46 methods)">

    @Override
    public void addBatch() throws SQLException {
        preparedDelegate.addBatch();
    }

    @Override
    public void clearParameters() throws SQLException {
        preparedDelegate.clearParameters();
    }

    @Override
    public void setAsciiStream(int p0, InputStream p1) throws SQLException {
        preparedDelegate.setAsciiStream(p0, p1);
    }

    @Override
    public void setAsciiStream(int p0, InputStream p1, int p2) throws SQLException {
        preparedDelegate.setAsciiStream(p0, p1, p2);
    }

    @Override
    public void setAsciiStream(int p0, InputStream p1, long p2) throws SQLException {
        preparedDelegate.setAsciiStream(p0, p1, p2);
    }

    @Override
    public void setBigDecimal(int p0, BigDecimal p1) throws SQLException {
        preparedDelegate.setBigDecimal(p0, p1);
    }

    @Override
    public void setBinaryStream(int p0, InputStream p1) throws SQLException {
        preparedDelegate.setBinaryStream(p0, p1);
    }

    @Override
    public void setBinaryStream(int p0, InputStream p1, int p2) throws SQLException {
        preparedDelegate.setBinaryStream(p0, p1, p2);
    }

    @Override
    public void setBinaryStream(int p0, InputStream p1, long p2) throws SQLException {
        preparedDelegate.setBinaryStream(p0, p1, p2);
    }

    @Override
    public void setBlob(int p0, InputStream p1) throws SQLException {
        preparedDelegate.setBlob(p0, p1);
    }

    @Override
    public void setBlob(int p0, Blob p1) throws SQLException {
        preparedDelegate.setBlob(p0, p1);
    }

    @Override
    public void setBlob(int p0, InputStream p1, long p2) throws SQLException {
        preparedDelegate.setBlob(p0, p1, p2);
    }

    @Override
    public void setBoolean(int p0, boolean p1) throws SQLException {
        preparedDelegate.setBoolean(p0, p1);
    }

    @Override
    public void setByte(int p0, byte p1) throws SQLException {
        preparedDelegate.setByte(p0, p1);
    }

    @Override
    public void setBytes(int p0, byte[] p1) throws SQLException {
        preparedDelegate.setBytes(p0, p1);
    }

    @Override
    public void setCharacterStream(int p0, Reader p1) throws SQLException {
        preparedDelegate.setCharacterStream(p0, p1);
    }

    @Override
    public void setCharacterStream(int p0, Reader p1, int p2) throws SQLException {
        preparedDelegate.setCharacterStream(p0, p1, p2);
    }

    @Override
    public void setCharacterStream(int p0, Reader p1, long p2) throws SQLException {
        preparedDelegate.setCharacterStream(p0, p1, p2);
    }

    @Override
    public void setClob(int p0, Reader p1) throws SQLException {
        preparedDelegate.setClob(p0, p1);
    }

    @Override
    public void setClob(int p0, Clob p1) throws SQLException {
        preparedDelegate.setClob(p0, p1);
    }

    @Override
    public void setClob(int p0, Reader p1, long p2) throws SQLException {
        preparedDelegate.setClob(p0, p1, p2);
    }

    @Override
    public void setDate(int p0, Date p1) throws SQLException {
        preparedDelegate.setDate(p0, p1);
    }

    @Override
    public void setDate(int p0, Date p1, Calendar p2) throws SQLException {
        preparedDelegate.setDate(p0, p1, p2);
    }

    @Override
    public void setDouble(int p0, double p1) throws SQLException {
        preparedDelegate.setDouble(p0, p1);
    }

    @Override
    public void setFloat(int p0, float p1) throws SQLException {
        preparedDelegate.setFloat(p0, p1);
    }

    @Override
    public void setInt(int p0, int p1) throws SQLException {
        preparedDelegate.setInt(p0, p1);
    }

    @Override
    public void setLong(int p0, long p1) throws SQLException {
        preparedDelegate.setLong(p0, p1);
    }

    @Override
    public void setNCharacterStream(int p0, Reader p1) throws SQLException {
        preparedDelegate.setNCharacterStream(p0, p1);
    }

    @Override
    public void setNCharacterStream(int p0, Reader p1, long p2) throws SQLException {
        preparedDelegate.setNCharacterStream(p0, p1, p2);
    }

    @Override
    public void setNClob(int p0, Reader p1) throws SQLException {
        preparedDelegate.setNClob(p0, p1);
    }

    @Override
    public void setNClob(int p0, NClob p1) throws SQLException {
        preparedDelegate.setNClob(p0, p1);
    }

    @Override
    public void setNClob(int p0, Reader p1, long p2) throws SQLException {
        preparedDelegate.setNClob(p0, p1, p2);
    }

    @Override
    public void setNString(int p0, String p1) throws SQLException {
        preparedDelegate.setNString(p0, p1);
    }

    @Override
    public void setNull(int p0, int p1) throws SQLException {
        preparedDelegate.setNull(p0, p1);
    }

    @Override
    public void setNull(int p0, int p1, String p2) throws SQLException {
        preparedDelegate.setNull(p0, p1, p2);
    }

    @Override
    public void setRef(int p0, Ref p1) throws SQLException {
        preparedDelegate.setRef(p0, p1);
    }

    @Override
    public void setRowId(int p0, RowId p1) throws SQLException {
        preparedDelegate.setRowId(p0, p1);
    }

    @Override
    public void setSQLXML(int p0, SQLXML p1) throws SQLException {
        preparedDelegate.setSQLXML(p0, p1);
    }

    @Override
    public void setShort(int p0, short p1) throws SQLException {
        preparedDelegate.setShort(p0, p1);
    }

    @Override
    public void setString(int p0, String p1) throws SQLException {
        preparedDelegate.setString(p0, p1);
    }

    @Override
    public void setTime(int p0, Time p1) throws SQLException {
        preparedDelegate.setTime(p0, p1);
    }

    @Override
    public void setTime(int p0, Time p1, Calendar p2) throws SQLException {
        preparedDelegate.setTime(p0, p1, p2);
    }

    @Override
    public void setTimestamp(int p0, Timestamp p1) throws SQLException {
        preparedDelegate.setTimestamp(p0, p1);
    }

    @Override
    public void setTimestamp(int p0, Timestamp p1, Calendar p2) throws SQLException {
        preparedDelegate.setTimestamp(p0, p1, p2);
    }

    @Override
    public void setURL(int p0, URL p1) throws SQLException {
        preparedDelegate.setURL(p0, p1);
    }

    @Override
    public void setUnicodeStream(int p0, InputStream p1, int p2) throws SQLException {
        preparedDelegate.setUnicodeStream(p0, p1, p2);
    }
    // </editor-fold>
}
