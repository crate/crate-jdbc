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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

/**
 * Adds reading a call's parameters to the binding {@link CratePreparedStatement}
 * already does. An OBJECT comes back as a {@code Map} and an array as a
 * {@link CrateArray}, as everywhere else in this driver.
 *
 * <p>No parameter of a call reaches those conversions against CrateDB.
 * CrateDB has no stored procedures, so a call is an ordinary parameterized
 * statement whose parameters are addressed by position; declaring an output
 * parameter needs the {@code {? = call f(?)}} form, which the server refuses
 * because describing it would need a PostgreSQL type CrateDB has no
 * equivalent for. Reading a parameter that was never declared is refused a
 * layer down, in pgJDBC, as is every by-name form. What the overrides below
 * settle is therefore what a parameter would read as for the day CrateDB
 * grows output parameters, not anything a caller meets now.</p>
 */
@SuppressWarnings("deprecation")
public class CrateCallableStatement extends CratePreparedStatement implements CallableStatement {

    protected final CallableStatement callableDelegate;

    CrateCallableStatement(CallableStatement delegate, CrateConnection connection) throws SQLException {
        super(delegate, connection);
        this.callableDelegate = delegate;
    }

    @Adapted
    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return CrateResultSet.fromPg(callableDelegate.getObject(parameterIndex));
    }

    @Adapted
    @Override
    public Object getObject(String parameterName) throws SQLException {
        return CrateResultSet.fromPg(callableDelegate.getObject(parameterName));
    }

    @Adapted
    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return CrateResultSet.fromPg(callableDelegate.getObject(parameterIndex, map));
    }

    @Adapted
    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return CrateResultSet.fromPg(callableDelegate.getObject(parameterName, map));
    }

    /**
     * The type a caller asks for is pgJDBC's to answer unless json came back,
     * which it would hand over as text. The untyped read that settles which of
     * the two it is runs only for the types json can be read into, so a
     * parameter pgJDBC can only decode into the type asked for is never read
     * untyped first.
     */
    @Adapted
    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        if (type == Array.class) {
            return type.cast(getArray(parameterIndex));
        }
        if (readableFromJson(type)) {
            T asked = CrateResultSet.asType(callableDelegate.getObject(parameterIndex), type);
            if (asked != null) {
                return asked;
            }
        }
        return callableDelegate.getObject(parameterIndex, type);
    }

    @Adapted
    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        if (type == Array.class) {
            return type.cast(getArray(parameterName));
        }
        if (readableFromJson(type)) {
            T asked = CrateResultSet.asType(callableDelegate.getObject(parameterName), type);
            if (asked != null) {
                return asked;
            }
        }
        return callableDelegate.getObject(parameterName, type);
    }

    /**
     * Whether a json value can be read as this type: an OBJECT into any
     * {@code Map}, a series of arrays into any {@code Collection}, either into
     * {@code Object}. {@link CrateResultSet#asType} answers for exactly these,
     * and asking it about another type would cost a read of the parameter for
     * nothing.
     */
    private static boolean readableFromJson(Class<?> type) {
        return type != null
            && (type == Object.class
                || Map.class.isAssignableFrom(type)
                || Collection.class.isAssignableFrom(type));
    }

    @Adapted
    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return wrap(callableDelegate.getArray(parameterIndex));
    }

    @Adapted
    @Override
    public Array getArray(String parameterName) throws SQLException {
        return wrap(callableDelegate.getArray(parameterName));
    }

    private static Array wrap(Array array) {
        return array == null ? null : new CrateArray(array);
    }

    // <editor-fold defaultstate="collapsed" desc="Delegation to pgJDBC (113 methods)">

    @Override
    public BigDecimal getBigDecimal(int p0) throws SQLException {
        return callableDelegate.getBigDecimal(p0);
    }

    @Override
    public BigDecimal getBigDecimal(String p0) throws SQLException {
        return callableDelegate.getBigDecimal(p0);
    }

    @Override
    public BigDecimal getBigDecimal(int p0, int p1) throws SQLException {
        return callableDelegate.getBigDecimal(p0, p1);
    }

    @Override
    public Blob getBlob(int p0) throws SQLException {
        return callableDelegate.getBlob(p0);
    }

    @Override
    public Blob getBlob(String p0) throws SQLException {
        return callableDelegate.getBlob(p0);
    }

    @Override
    public boolean getBoolean(int p0) throws SQLException {
        return callableDelegate.getBoolean(p0);
    }

    @Override
    public boolean getBoolean(String p0) throws SQLException {
        return callableDelegate.getBoolean(p0);
    }

    @Override
    public byte getByte(int p0) throws SQLException {
        return callableDelegate.getByte(p0);
    }

    @Override
    public byte getByte(String p0) throws SQLException {
        return callableDelegate.getByte(p0);
    }

    @Override
    public byte[] getBytes(int p0) throws SQLException {
        return callableDelegate.getBytes(p0);
    }

    @Override
    public byte[] getBytes(String p0) throws SQLException {
        return callableDelegate.getBytes(p0);
    }

    @Override
    public Reader getCharacterStream(int p0) throws SQLException {
        return callableDelegate.getCharacterStream(p0);
    }

    @Override
    public Reader getCharacterStream(String p0) throws SQLException {
        return callableDelegate.getCharacterStream(p0);
    }

    @Override
    public Clob getClob(int p0) throws SQLException {
        return callableDelegate.getClob(p0);
    }

    @Override
    public Clob getClob(String p0) throws SQLException {
        return callableDelegate.getClob(p0);
    }

    @Override
    public Date getDate(int p0) throws SQLException {
        return callableDelegate.getDate(p0);
    }

    @Override
    public Date getDate(String p0) throws SQLException {
        return callableDelegate.getDate(p0);
    }

    @Override
    public Date getDate(int p0, Calendar p1) throws SQLException {
        return callableDelegate.getDate(p0, p1);
    }

    @Override
    public Date getDate(String p0, Calendar p1) throws SQLException {
        return callableDelegate.getDate(p0, p1);
    }

    @Override
    public double getDouble(int p0) throws SQLException {
        return callableDelegate.getDouble(p0);
    }

    @Override
    public double getDouble(String p0) throws SQLException {
        return callableDelegate.getDouble(p0);
    }

    @Override
    public float getFloat(int p0) throws SQLException {
        return callableDelegate.getFloat(p0);
    }

    @Override
    public float getFloat(String p0) throws SQLException {
        return callableDelegate.getFloat(p0);
    }

    @Override
    public int getInt(int p0) throws SQLException {
        return callableDelegate.getInt(p0);
    }

    @Override
    public int getInt(String p0) throws SQLException {
        return callableDelegate.getInt(p0);
    }

    @Override
    public long getLong(int p0) throws SQLException {
        return callableDelegate.getLong(p0);
    }

    @Override
    public long getLong(String p0) throws SQLException {
        return callableDelegate.getLong(p0);
    }

    @Override
    public Reader getNCharacterStream(int p0) throws SQLException {
        return callableDelegate.getNCharacterStream(p0);
    }

    @Override
    public Reader getNCharacterStream(String p0) throws SQLException {
        return callableDelegate.getNCharacterStream(p0);
    }

    @Override
    public NClob getNClob(int p0) throws SQLException {
        return callableDelegate.getNClob(p0);
    }

    @Override
    public NClob getNClob(String p0) throws SQLException {
        return callableDelegate.getNClob(p0);
    }

    @Override
    public String getNString(int p0) throws SQLException {
        return callableDelegate.getNString(p0);
    }

    @Override
    public String getNString(String p0) throws SQLException {
        return callableDelegate.getNString(p0);
    }

    @Override
    public Ref getRef(int p0) throws SQLException {
        return callableDelegate.getRef(p0);
    }

    @Override
    public Ref getRef(String p0) throws SQLException {
        return callableDelegate.getRef(p0);
    }

    @Override
    public RowId getRowId(int p0) throws SQLException {
        return callableDelegate.getRowId(p0);
    }

    @Override
    public RowId getRowId(String p0) throws SQLException {
        return callableDelegate.getRowId(p0);
    }

    @Override
    public SQLXML getSQLXML(int p0) throws SQLException {
        return callableDelegate.getSQLXML(p0);
    }

    @Override
    public SQLXML getSQLXML(String p0) throws SQLException {
        return callableDelegate.getSQLXML(p0);
    }

    @Override
    public short getShort(int p0) throws SQLException {
        return callableDelegate.getShort(p0);
    }

    @Override
    public short getShort(String p0) throws SQLException {
        return callableDelegate.getShort(p0);
    }

    @Override
    public String getString(int p0) throws SQLException {
        return callableDelegate.getString(p0);
    }

    @Override
    public String getString(String p0) throws SQLException {
        return callableDelegate.getString(p0);
    }

    @Override
    public Time getTime(int p0) throws SQLException {
        return callableDelegate.getTime(p0);
    }

    @Override
    public Time getTime(String p0) throws SQLException {
        return callableDelegate.getTime(p0);
    }

    @Override
    public Time getTime(int p0, Calendar p1) throws SQLException {
        return callableDelegate.getTime(p0, p1);
    }

    @Override
    public Time getTime(String p0, Calendar p1) throws SQLException {
        return callableDelegate.getTime(p0, p1);
    }

    @Override
    public Timestamp getTimestamp(int p0) throws SQLException {
        return callableDelegate.getTimestamp(p0);
    }

    @Override
    public Timestamp getTimestamp(String p0) throws SQLException {
        return callableDelegate.getTimestamp(p0);
    }

    @Override
    public Timestamp getTimestamp(int p0, Calendar p1) throws SQLException {
        return callableDelegate.getTimestamp(p0, p1);
    }

    @Override
    public Timestamp getTimestamp(String p0, Calendar p1) throws SQLException {
        return callableDelegate.getTimestamp(p0, p1);
    }

    @Override
    public URL getURL(int p0) throws SQLException {
        return callableDelegate.getURL(p0);
    }

    @Override
    public URL getURL(String p0) throws SQLException {
        return callableDelegate.getURL(p0);
    }

    @Override
    public void registerOutParameter(int p0, int p1) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1);
    }

    @Override
    public void registerOutParameter(String p0, int p1) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1);
    }

    @Override
    public void registerOutParameter(int p0, SQLType p1) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1);
    }

    @Override
    public void registerOutParameter(String p0, SQLType p1) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1);
    }

    @Override
    public void registerOutParameter(int p0, int p1, int p2) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1, p2);
    }

    @Override
    public void registerOutParameter(int p0, int p1, String p2) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1, p2);
    }

    @Override
    public void registerOutParameter(String p0, int p1, int p2) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1, p2);
    }

    @Override
    public void registerOutParameter(String p0, int p1, String p2) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1, p2);
    }

    @Override
    public void registerOutParameter(int p0, SQLType p1, int p2) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1, p2);
    }

    @Override
    public void registerOutParameter(int p0, SQLType p1, String p2) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1, p2);
    }

    @Override
    public void registerOutParameter(String p0, SQLType p1, int p2) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1, p2);
    }

    @Override
    public void registerOutParameter(String p0, SQLType p1, String p2) throws SQLException {
        callableDelegate.registerOutParameter(p0, p1, p2);
    }

    @Override
    public void setAsciiStream(String p0, InputStream p1) throws SQLException {
        callableDelegate.setAsciiStream(p0, p1);
    }

    @Override
    public void setAsciiStream(String p0, InputStream p1, int p2) throws SQLException {
        callableDelegate.setAsciiStream(p0, p1, p2);
    }

    @Override
    public void setAsciiStream(String p0, InputStream p1, long p2) throws SQLException {
        callableDelegate.setAsciiStream(p0, p1, p2);
    }

    @Override
    public void setBigDecimal(String p0, BigDecimal p1) throws SQLException {
        callableDelegate.setBigDecimal(p0, p1);
    }

    @Override
    public void setBinaryStream(String p0, InputStream p1) throws SQLException {
        callableDelegate.setBinaryStream(p0, p1);
    }

    @Override
    public void setBinaryStream(String p0, InputStream p1, int p2) throws SQLException {
        callableDelegate.setBinaryStream(p0, p1, p2);
    }

    @Override
    public void setBinaryStream(String p0, InputStream p1, long p2) throws SQLException {
        callableDelegate.setBinaryStream(p0, p1, p2);
    }

    @Override
    public void setBlob(String p0, InputStream p1) throws SQLException {
        callableDelegate.setBlob(p0, p1);
    }

    @Override
    public void setBlob(String p0, Blob p1) throws SQLException {
        callableDelegate.setBlob(p0, p1);
    }

    @Override
    public void setBlob(String p0, InputStream p1, long p2) throws SQLException {
        callableDelegate.setBlob(p0, p1, p2);
    }

    @Override
    public void setBoolean(String p0, boolean p1) throws SQLException {
        callableDelegate.setBoolean(p0, p1);
    }

    @Override
    public void setByte(String p0, byte p1) throws SQLException {
        callableDelegate.setByte(p0, p1);
    }

    @Override
    public void setBytes(String p0, byte[] p1) throws SQLException {
        callableDelegate.setBytes(p0, p1);
    }

    @Override
    public void setCharacterStream(String p0, Reader p1) throws SQLException {
        callableDelegate.setCharacterStream(p0, p1);
    }

    @Override
    public void setCharacterStream(String p0, Reader p1, int p2) throws SQLException {
        callableDelegate.setCharacterStream(p0, p1, p2);
    }

    @Override
    public void setCharacterStream(String p0, Reader p1, long p2) throws SQLException {
        callableDelegate.setCharacterStream(p0, p1, p2);
    }

    @Override
    public void setClob(String p0, Reader p1) throws SQLException {
        callableDelegate.setClob(p0, p1);
    }

    @Override
    public void setClob(String p0, Clob p1) throws SQLException {
        callableDelegate.setClob(p0, p1);
    }

    @Override
    public void setClob(String p0, Reader p1, long p2) throws SQLException {
        callableDelegate.setClob(p0, p1, p2);
    }

    @Override
    public void setDate(String p0, Date p1) throws SQLException {
        callableDelegate.setDate(p0, p1);
    }

    @Override
    public void setDate(String p0, Date p1, Calendar p2) throws SQLException {
        callableDelegate.setDate(p0, p1, p2);
    }

    @Override
    public void setDouble(String p0, double p1) throws SQLException {
        callableDelegate.setDouble(p0, p1);
    }

    @Override
    public void setFloat(String p0, float p1) throws SQLException {
        callableDelegate.setFloat(p0, p1);
    }

    @Override
    public void setInt(String p0, int p1) throws SQLException {
        callableDelegate.setInt(p0, p1);
    }

    @Override
    public void setLong(String p0, long p1) throws SQLException {
        callableDelegate.setLong(p0, p1);
    }

    @Override
    public void setNCharacterStream(String p0, Reader p1) throws SQLException {
        callableDelegate.setNCharacterStream(p0, p1);
    }

    @Override
    public void setNCharacterStream(String p0, Reader p1, long p2) throws SQLException {
        callableDelegate.setNCharacterStream(p0, p1, p2);
    }

    @Override
    public void setNClob(String p0, Reader p1) throws SQLException {
        callableDelegate.setNClob(p0, p1);
    }

    @Override
    public void setNClob(String p0, NClob p1) throws SQLException {
        callableDelegate.setNClob(p0, p1);
    }

    @Override
    public void setNClob(String p0, Reader p1, long p2) throws SQLException {
        callableDelegate.setNClob(p0, p1, p2);
    }

    @Override
    public void setNString(String p0, String p1) throws SQLException {
        callableDelegate.setNString(p0, p1);
    }

    @Override
    public void setNull(String p0, int p1) throws SQLException {
        callableDelegate.setNull(p0, p1);
    }

    @Override
    public void setNull(String p0, int p1, String p2) throws SQLException {
        callableDelegate.setNull(p0, p1, p2);
    }

    @Override
    public void setObject(String p0, Object p1) throws SQLException {
        callableDelegate.setObject(p0, p1);
    }

    @Override
    public void setObject(String p0, Object p1, int p2) throws SQLException {
        callableDelegate.setObject(p0, p1, p2);
    }

    @Override
    public void setObject(String p0, Object p1, SQLType p2) throws SQLException {
        callableDelegate.setObject(p0, p1, p2);
    }

    @Override
    public void setObject(String p0, Object p1, int p2, int p3) throws SQLException {
        callableDelegate.setObject(p0, p1, p2, p3);
    }

    @Override
    public void setObject(String p0, Object p1, SQLType p2, int p3) throws SQLException {
        callableDelegate.setObject(p0, p1, p2, p3);
    }

    @Override
    public void setRowId(String p0, RowId p1) throws SQLException {
        callableDelegate.setRowId(p0, p1);
    }

    @Override
    public void setSQLXML(String p0, SQLXML p1) throws SQLException {
        callableDelegate.setSQLXML(p0, p1);
    }

    @Override
    public void setShort(String p0, short p1) throws SQLException {
        callableDelegate.setShort(p0, p1);
    }

    @Override
    public void setString(String p0, String p1) throws SQLException {
        callableDelegate.setString(p0, p1);
    }

    @Override
    public void setTime(String p0, Time p1) throws SQLException {
        callableDelegate.setTime(p0, p1);
    }

    @Override
    public void setTime(String p0, Time p1, Calendar p2) throws SQLException {
        callableDelegate.setTime(p0, p1, p2);
    }

    @Override
    public void setTimestamp(String p0, Timestamp p1) throws SQLException {
        callableDelegate.setTimestamp(p0, p1);
    }

    @Override
    public void setTimestamp(String p0, Timestamp p1, Calendar p2) throws SQLException {
        callableDelegate.setTimestamp(p0, p1, p2);
    }

    @Override
    public void setURL(String p0, URL p1) throws SQLException {
        callableDelegate.setURL(p0, p1);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return callableDelegate.wasNull();
    }
    // </editor-fold>
}
