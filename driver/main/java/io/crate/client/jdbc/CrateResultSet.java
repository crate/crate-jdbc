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

import org.postgresql.util.PGobject;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

/**
 * Rows as this driver reads them: a CrateDB OBJECT column, json on the wire,
 * comes back as a {@code Map<String, Object>}, and a column of nested arrays
 * as a {@link java.sql.Array}. Those are the two shapes pgJDBC has no decoder
 * for; every other column is its.
 */
@SuppressWarnings("deprecation")
public class CrateResultSet implements ResultSet {

    protected final ResultSet delegate;

    private final Statement statement;

    private CrateResultSetMetaData metaData;

    CrateResultSet(ResultSet delegate, Statement statement) {
        this.delegate = delegate;
        this.statement = statement;
    }

    /**
     * What the columns hold, in the terms this result set reads them in. One
     * object per result set, as pgJDBC hands it out.
     */
    @Adapted
    @Override
    public CrateResultSetMetaData getMetaData() throws SQLException {
        if (metaData == null) {
            metaData = new CrateResultSetMetaData(delegate.getMetaData());
        }
        return metaData;
    }

    /**
     * The statement that produced this result set, as a wrapper, so that
     * navigating back to the connection stays inside this driver.
     *
     * <p>The wrapper is held here instead of read from the delegate, so the
     * delegate is asked whether the result set is still open. A closed one has
     * nothing to navigate from, and answering would hand out a way around
     * that.</p>
     */
    @Adapted
    @Override
    public Statement getStatement() throws SQLException {
        delegate.getStatement();
        return statement;
    }

    @Adapted
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return fromPg(delegate.getObject(columnIndex));
    }

    @Adapted
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return fromPg(delegate.getObject(columnLabel));
    }

    @Adapted
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return fromPg(delegate.getObject(columnIndex, map));
    }

    @Adapted
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return fromPg(delegate.getObject(columnLabel, map));
    }

    /**
     * The type a caller asks for is pgJDBC's to answer for every column but a
     * json one, whose value it would hand over as text. Which column is which
     * is settled before the value is read, so a column pgJDBC can only decode
     * into the requested type is never read untyped first.
     *
     * <p>An array is this driver's to answer whatever the column. Asking for
     * one by type reads the same array as {@link #getArray(int)} does, down to
     * the conversion of its elements and to the json columns pgJDBC has no
     * array decoder for.</p>
     */
    @Adapted
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

    @Adapted
    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(delegate.findColumn(columnLabel), type);
    }

    /**
     * A column of nested arrays arrives as json, which pgJDBC has no array
     * decoder for, so it is read here. Every other array column is pgJDBC's,
     * wrapped so its elements convert like the rest.
     */
    @Adapted
    @Override
    public java.sql.Array getArray(int columnIndex) throws SQLException {
        if (isJsonColumn(columnIndex)) {
            String json = delegate.getString(columnIndex);
            return json == null ? null : CrateJsonArray.of(json);
        }
        java.sql.Array array = delegate.getArray(columnIndex);
        return array == null ? null : new CrateArray(array);
    }

    @Adapted
    @Override
    public java.sql.Array getArray(String columnLabel) throws SQLException {
        return getArray(delegate.findColumn(columnLabel));
    }

    /** Whether a column carries json: an OBJECT, a geo_shape, nested arrays. */
    private boolean isJsonColumn(int columnIndex) throws SQLException {
        return getMetaData().isJson(columnIndex);
    }

    /**
     * A value as this driver surfaces it: an OBJECT as a {@code Map}, an array
     * as a {@link CrateArray} converting its elements in turn, everything else
     * as pgJDBC read it.
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
     * A json value as the requested Java type, or null for a value this driver
     * does not convert and pgJDBC answers itself. An OBJECT reads into any
     * {@code Map} type, and a column of nested arrays into any
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

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.isInstance(this) ? iface.cast(this) : delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }

    // <editor-fold defaultstate="collapsed" desc="Delegation to pgJDBC (183 methods)">

    @Override
    public boolean absolute(int p0) throws SQLException {
        return delegate.absolute(p0);
    }

    @Override
    public void afterLast() throws SQLException {
        delegate.afterLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        delegate.beforeFirst();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        delegate.cancelRowUpdates();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public void deleteRow() throws SQLException {
        delegate.deleteRow();
    }

    @Override
    public int findColumn(String p0) throws SQLException {
        return delegate.findColumn(p0);
    }

    @Override
    public boolean first() throws SQLException {
        return delegate.first();
    }

    @Override
    public InputStream getAsciiStream(int p0) throws SQLException {
        return delegate.getAsciiStream(p0);
    }

    @Override
    public InputStream getAsciiStream(String p0) throws SQLException {
        return delegate.getAsciiStream(p0);
    }

    @Override
    public BigDecimal getBigDecimal(int p0) throws SQLException {
        return delegate.getBigDecimal(p0);
    }

    @Override
    public BigDecimal getBigDecimal(String p0) throws SQLException {
        return delegate.getBigDecimal(p0);
    }

    @Override
    public BigDecimal getBigDecimal(int p0, int p1) throws SQLException {
        return delegate.getBigDecimal(p0, p1);
    }

    @Override
    public BigDecimal getBigDecimal(String p0, int p1) throws SQLException {
        return delegate.getBigDecimal(p0, p1);
    }

    @Override
    public InputStream getBinaryStream(int p0) throws SQLException {
        return delegate.getBinaryStream(p0);
    }

    @Override
    public InputStream getBinaryStream(String p0) throws SQLException {
        return delegate.getBinaryStream(p0);
    }

    @Override
    public Blob getBlob(int p0) throws SQLException {
        return delegate.getBlob(p0);
    }

    @Override
    public Blob getBlob(String p0) throws SQLException {
        return delegate.getBlob(p0);
    }

    @Override
    public boolean getBoolean(int p0) throws SQLException {
        return delegate.getBoolean(p0);
    }

    @Override
    public boolean getBoolean(String p0) throws SQLException {
        return delegate.getBoolean(p0);
    }

    @Override
    public byte getByte(int p0) throws SQLException {
        return delegate.getByte(p0);
    }

    @Override
    public byte getByte(String p0) throws SQLException {
        return delegate.getByte(p0);
    }

    @Override
    public byte[] getBytes(int p0) throws SQLException {
        return delegate.getBytes(p0);
    }

    @Override
    public byte[] getBytes(String p0) throws SQLException {
        return delegate.getBytes(p0);
    }

    @Override
    public Reader getCharacterStream(int p0) throws SQLException {
        return delegate.getCharacterStream(p0);
    }

    @Override
    public Reader getCharacterStream(String p0) throws SQLException {
        return delegate.getCharacterStream(p0);
    }

    @Override
    public Clob getClob(int p0) throws SQLException {
        return delegate.getClob(p0);
    }

    @Override
    public Clob getClob(String p0) throws SQLException {
        return delegate.getClob(p0);
    }

    @Override
    public int getConcurrency() throws SQLException {
        return delegate.getConcurrency();
    }

    @Override
    public String getCursorName() throws SQLException {
        return delegate.getCursorName();
    }

    @Override
    public Date getDate(int p0) throws SQLException {
        return delegate.getDate(p0);
    }

    @Override
    public Date getDate(String p0) throws SQLException {
        return delegate.getDate(p0);
    }

    @Override
    public Date getDate(int p0, Calendar p1) throws SQLException {
        return delegate.getDate(p0, p1);
    }

    @Override
    public Date getDate(String p0, Calendar p1) throws SQLException {
        return delegate.getDate(p0, p1);
    }

    @Override
    public double getDouble(int p0) throws SQLException {
        return delegate.getDouble(p0);
    }

    @Override
    public double getDouble(String p0) throws SQLException {
        return delegate.getDouble(p0);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    @Override
    public float getFloat(int p0) throws SQLException {
        return delegate.getFloat(p0);
    }

    @Override
    public float getFloat(String p0) throws SQLException {
        return delegate.getFloat(p0);
    }

    @Override
    public int getHoldability() throws SQLException {
        return delegate.getHoldability();
    }

    @Override
    public int getInt(int p0) throws SQLException {
        return delegate.getInt(p0);
    }

    @Override
    public int getInt(String p0) throws SQLException {
        return delegate.getInt(p0);
    }

    @Override
    public long getLong(int p0) throws SQLException {
        return delegate.getLong(p0);
    }

    @Override
    public long getLong(String p0) throws SQLException {
        return delegate.getLong(p0);
    }

    @Override
    public Reader getNCharacterStream(int p0) throws SQLException {
        return delegate.getNCharacterStream(p0);
    }

    @Override
    public Reader getNCharacterStream(String p0) throws SQLException {
        return delegate.getNCharacterStream(p0);
    }

    @Override
    public NClob getNClob(int p0) throws SQLException {
        return delegate.getNClob(p0);
    }

    @Override
    public NClob getNClob(String p0) throws SQLException {
        return delegate.getNClob(p0);
    }

    @Override
    public String getNString(int p0) throws SQLException {
        return delegate.getNString(p0);
    }

    @Override
    public String getNString(String p0) throws SQLException {
        return delegate.getNString(p0);
    }

    @Override
    public Ref getRef(int p0) throws SQLException {
        return delegate.getRef(p0);
    }

    @Override
    public Ref getRef(String p0) throws SQLException {
        return delegate.getRef(p0);
    }

    @Override
    public int getRow() throws SQLException {
        return delegate.getRow();
    }

    @Override
    public RowId getRowId(int p0) throws SQLException {
        return delegate.getRowId(p0);
    }

    @Override
    public RowId getRowId(String p0) throws SQLException {
        return delegate.getRowId(p0);
    }

    @Override
    public SQLXML getSQLXML(int p0) throws SQLException {
        return delegate.getSQLXML(p0);
    }

    @Override
    public SQLXML getSQLXML(String p0) throws SQLException {
        return delegate.getSQLXML(p0);
    }

    @Override
    public short getShort(int p0) throws SQLException {
        return delegate.getShort(p0);
    }

    @Override
    public short getShort(String p0) throws SQLException {
        return delegate.getShort(p0);
    }

    @Override
    public String getString(int p0) throws SQLException {
        return delegate.getString(p0);
    }

    @Override
    public String getString(String p0) throws SQLException {
        return delegate.getString(p0);
    }

    @Override
    public Time getTime(int p0) throws SQLException {
        return delegate.getTime(p0);
    }

    @Override
    public Time getTime(String p0) throws SQLException {
        return delegate.getTime(p0);
    }

    @Override
    public Time getTime(int p0, Calendar p1) throws SQLException {
        return delegate.getTime(p0, p1);
    }

    @Override
    public Time getTime(String p0, Calendar p1) throws SQLException {
        return delegate.getTime(p0, p1);
    }

    @Override
    public Timestamp getTimestamp(int p0) throws SQLException {
        return delegate.getTimestamp(p0);
    }

    @Override
    public Timestamp getTimestamp(String p0) throws SQLException {
        return delegate.getTimestamp(p0);
    }

    @Override
    public Timestamp getTimestamp(int p0, Calendar p1) throws SQLException {
        return delegate.getTimestamp(p0, p1);
    }

    @Override
    public Timestamp getTimestamp(String p0, Calendar p1) throws SQLException {
        return delegate.getTimestamp(p0, p1);
    }

    @Override
    public int getType() throws SQLException {
        return delegate.getType();
    }

    @Override
    public URL getURL(int p0) throws SQLException {
        return delegate.getURL(p0);
    }

    @Override
    public URL getURL(String p0) throws SQLException {
        return delegate.getURL(p0);
    }

    @Override
    public InputStream getUnicodeStream(int p0) throws SQLException {
        return delegate.getUnicodeStream(p0);
    }

    @Override
    public InputStream getUnicodeStream(String p0) throws SQLException {
        return delegate.getUnicodeStream(p0);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void insertRow() throws SQLException {
        delegate.insertRow();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return delegate.isAfterLast();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return delegate.isBeforeFirst();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return delegate.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return delegate.isLast();
    }

    @Override
    public boolean last() throws SQLException {
        return delegate.last();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        delegate.moveToCurrentRow();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        delegate.moveToInsertRow();
    }

    @Override
    public boolean next() throws SQLException {
        return delegate.next();
    }

    @Override
    public boolean previous() throws SQLException {
        return delegate.previous();
    }

    @Override
    public void refreshRow() throws SQLException {
        delegate.refreshRow();
    }

    @Override
    public boolean relative(int p0) throws SQLException {
        return delegate.relative(p0);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return delegate.rowDeleted();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return delegate.rowInserted();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return delegate.rowUpdated();
    }

    @Override
    public void setFetchDirection(int p0) throws SQLException {
        delegate.setFetchDirection(p0);
    }

    @Override
    public void setFetchSize(int p0) throws SQLException {
        delegate.setFetchSize(p0);
    }

    @Override
    public void updateArray(int p0, Array p1) throws SQLException {
        delegate.updateArray(p0, p1);
    }

    @Override
    public void updateArray(String p0, Array p1) throws SQLException {
        delegate.updateArray(p0, p1);
    }

    @Override
    public void updateAsciiStream(int p0, InputStream p1) throws SQLException {
        delegate.updateAsciiStream(p0, p1);
    }

    @Override
    public void updateAsciiStream(String p0, InputStream p1) throws SQLException {
        delegate.updateAsciiStream(p0, p1);
    }

    @Override
    public void updateAsciiStream(int p0, InputStream p1, int p2) throws SQLException {
        delegate.updateAsciiStream(p0, p1, p2);
    }

    @Override
    public void updateAsciiStream(int p0, InputStream p1, long p2) throws SQLException {
        delegate.updateAsciiStream(p0, p1, p2);
    }

    @Override
    public void updateAsciiStream(String p0, InputStream p1, int p2) throws SQLException {
        delegate.updateAsciiStream(p0, p1, p2);
    }

    @Override
    public void updateAsciiStream(String p0, InputStream p1, long p2) throws SQLException {
        delegate.updateAsciiStream(p0, p1, p2);
    }

    @Override
    public void updateBigDecimal(int p0, BigDecimal p1) throws SQLException {
        delegate.updateBigDecimal(p0, p1);
    }

    @Override
    public void updateBigDecimal(String p0, BigDecimal p1) throws SQLException {
        delegate.updateBigDecimal(p0, p1);
    }

    @Override
    public void updateBinaryStream(int p0, InputStream p1) throws SQLException {
        delegate.updateBinaryStream(p0, p1);
    }

    @Override
    public void updateBinaryStream(String p0, InputStream p1) throws SQLException {
        delegate.updateBinaryStream(p0, p1);
    }

    @Override
    public void updateBinaryStream(int p0, InputStream p1, int p2) throws SQLException {
        delegate.updateBinaryStream(p0, p1, p2);
    }

    @Override
    public void updateBinaryStream(int p0, InputStream p1, long p2) throws SQLException {
        delegate.updateBinaryStream(p0, p1, p2);
    }

    @Override
    public void updateBinaryStream(String p0, InputStream p1, int p2) throws SQLException {
        delegate.updateBinaryStream(p0, p1, p2);
    }

    @Override
    public void updateBinaryStream(String p0, InputStream p1, long p2) throws SQLException {
        delegate.updateBinaryStream(p0, p1, p2);
    }

    @Override
    public void updateBlob(int p0, InputStream p1) throws SQLException {
        delegate.updateBlob(p0, p1);
    }

    @Override
    public void updateBlob(int p0, Blob p1) throws SQLException {
        delegate.updateBlob(p0, p1);
    }

    @Override
    public void updateBlob(String p0, InputStream p1) throws SQLException {
        delegate.updateBlob(p0, p1);
    }

    @Override
    public void updateBlob(String p0, Blob p1) throws SQLException {
        delegate.updateBlob(p0, p1);
    }

    @Override
    public void updateBlob(int p0, InputStream p1, long p2) throws SQLException {
        delegate.updateBlob(p0, p1, p2);
    }

    @Override
    public void updateBlob(String p0, InputStream p1, long p2) throws SQLException {
        delegate.updateBlob(p0, p1, p2);
    }

    @Override
    public void updateBoolean(int p0, boolean p1) throws SQLException {
        delegate.updateBoolean(p0, p1);
    }

    @Override
    public void updateBoolean(String p0, boolean p1) throws SQLException {
        delegate.updateBoolean(p0, p1);
    }

    @Override
    public void updateByte(int p0, byte p1) throws SQLException {
        delegate.updateByte(p0, p1);
    }

    @Override
    public void updateByte(String p0, byte p1) throws SQLException {
        delegate.updateByte(p0, p1);
    }

    @Override
    public void updateBytes(int p0, byte[] p1) throws SQLException {
        delegate.updateBytes(p0, p1);
    }

    @Override
    public void updateBytes(String p0, byte[] p1) throws SQLException {
        delegate.updateBytes(p0, p1);
    }

    @Override
    public void updateCharacterStream(int p0, Reader p1) throws SQLException {
        delegate.updateCharacterStream(p0, p1);
    }

    @Override
    public void updateCharacterStream(String p0, Reader p1) throws SQLException {
        delegate.updateCharacterStream(p0, p1);
    }

    @Override
    public void updateCharacterStream(int p0, Reader p1, int p2) throws SQLException {
        delegate.updateCharacterStream(p0, p1, p2);
    }

    @Override
    public void updateCharacterStream(int p0, Reader p1, long p2) throws SQLException {
        delegate.updateCharacterStream(p0, p1, p2);
    }

    @Override
    public void updateCharacterStream(String p0, Reader p1, int p2) throws SQLException {
        delegate.updateCharacterStream(p0, p1, p2);
    }

    @Override
    public void updateCharacterStream(String p0, Reader p1, long p2) throws SQLException {
        delegate.updateCharacterStream(p0, p1, p2);
    }

    @Override
    public void updateClob(int p0, Reader p1) throws SQLException {
        delegate.updateClob(p0, p1);
    }

    @Override
    public void updateClob(int p0, Clob p1) throws SQLException {
        delegate.updateClob(p0, p1);
    }

    @Override
    public void updateClob(String p0, Reader p1) throws SQLException {
        delegate.updateClob(p0, p1);
    }

    @Override
    public void updateClob(String p0, Clob p1) throws SQLException {
        delegate.updateClob(p0, p1);
    }

    @Override
    public void updateClob(int p0, Reader p1, long p2) throws SQLException {
        delegate.updateClob(p0, p1, p2);
    }

    @Override
    public void updateClob(String p0, Reader p1, long p2) throws SQLException {
        delegate.updateClob(p0, p1, p2);
    }

    @Override
    public void updateDate(int p0, Date p1) throws SQLException {
        delegate.updateDate(p0, p1);
    }

    @Override
    public void updateDate(String p0, Date p1) throws SQLException {
        delegate.updateDate(p0, p1);
    }

    @Override
    public void updateDouble(int p0, double p1) throws SQLException {
        delegate.updateDouble(p0, p1);
    }

    @Override
    public void updateDouble(String p0, double p1) throws SQLException {
        delegate.updateDouble(p0, p1);
    }

    @Override
    public void updateFloat(int p0, float p1) throws SQLException {
        delegate.updateFloat(p0, p1);
    }

    @Override
    public void updateFloat(String p0, float p1) throws SQLException {
        delegate.updateFloat(p0, p1);
    }

    @Override
    public void updateInt(int p0, int p1) throws SQLException {
        delegate.updateInt(p0, p1);
    }

    @Override
    public void updateInt(String p0, int p1) throws SQLException {
        delegate.updateInt(p0, p1);
    }

    @Override
    public void updateLong(int p0, long p1) throws SQLException {
        delegate.updateLong(p0, p1);
    }

    @Override
    public void updateLong(String p0, long p1) throws SQLException {
        delegate.updateLong(p0, p1);
    }

    @Override
    public void updateNCharacterStream(int p0, Reader p1) throws SQLException {
        delegate.updateNCharacterStream(p0, p1);
    }

    @Override
    public void updateNCharacterStream(String p0, Reader p1) throws SQLException {
        delegate.updateNCharacterStream(p0, p1);
    }

    @Override
    public void updateNCharacterStream(int p0, Reader p1, long p2) throws SQLException {
        delegate.updateNCharacterStream(p0, p1, p2);
    }

    @Override
    public void updateNCharacterStream(String p0, Reader p1, long p2) throws SQLException {
        delegate.updateNCharacterStream(p0, p1, p2);
    }

    @Override
    public void updateNClob(int p0, Reader p1) throws SQLException {
        delegate.updateNClob(p0, p1);
    }

    @Override
    public void updateNClob(int p0, NClob p1) throws SQLException {
        delegate.updateNClob(p0, p1);
    }

    @Override
    public void updateNClob(String p0, Reader p1) throws SQLException {
        delegate.updateNClob(p0, p1);
    }

    @Override
    public void updateNClob(String p0, NClob p1) throws SQLException {
        delegate.updateNClob(p0, p1);
    }

    @Override
    public void updateNClob(int p0, Reader p1, long p2) throws SQLException {
        delegate.updateNClob(p0, p1, p2);
    }

    @Override
    public void updateNClob(String p0, Reader p1, long p2) throws SQLException {
        delegate.updateNClob(p0, p1, p2);
    }

    @Override
    public void updateNString(int p0, String p1) throws SQLException {
        delegate.updateNString(p0, p1);
    }

    @Override
    public void updateNString(String p0, String p1) throws SQLException {
        delegate.updateNString(p0, p1);
    }

    @Override
    public void updateNull(int p0) throws SQLException {
        delegate.updateNull(p0);
    }

    @Override
    public void updateNull(String p0) throws SQLException {
        delegate.updateNull(p0);
    }

    @Override
    public void updateObject(int p0, Object p1) throws SQLException {
        delegate.updateObject(p0, p1);
    }

    @Override
    public void updateObject(String p0, Object p1) throws SQLException {
        delegate.updateObject(p0, p1);
    }

    @Override
    public void updateObject(int p0, Object p1, int p2) throws SQLException {
        delegate.updateObject(p0, p1, p2);
    }

    @Override
    public void updateObject(String p0, Object p1, int p2) throws SQLException {
        delegate.updateObject(p0, p1, p2);
    }

    @Override
    public void updateObject(int p0, Object p1, SQLType p2) throws SQLException {
        delegate.updateObject(p0, p1, p2);
    }

    @Override
    public void updateObject(String p0, Object p1, SQLType p2) throws SQLException {
        delegate.updateObject(p0, p1, p2);
    }

    @Override
    public void updateObject(int p0, Object p1, SQLType p2, int p3) throws SQLException {
        delegate.updateObject(p0, p1, p2, p3);
    }

    @Override
    public void updateObject(String p0, Object p1, SQLType p2, int p3) throws SQLException {
        delegate.updateObject(p0, p1, p2, p3);
    }

    @Override
    public void updateRef(int p0, Ref p1) throws SQLException {
        delegate.updateRef(p0, p1);
    }

    @Override
    public void updateRef(String p0, Ref p1) throws SQLException {
        delegate.updateRef(p0, p1);
    }

    @Override
    public void updateRow() throws SQLException {
        delegate.updateRow();
    }

    @Override
    public void updateRowId(int p0, RowId p1) throws SQLException {
        delegate.updateRowId(p0, p1);
    }

    @Override
    public void updateRowId(String p0, RowId p1) throws SQLException {
        delegate.updateRowId(p0, p1);
    }

    @Override
    public void updateSQLXML(int p0, SQLXML p1) throws SQLException {
        delegate.updateSQLXML(p0, p1);
    }

    @Override
    public void updateSQLXML(String p0, SQLXML p1) throws SQLException {
        delegate.updateSQLXML(p0, p1);
    }

    @Override
    public void updateShort(int p0, short p1) throws SQLException {
        delegate.updateShort(p0, p1);
    }

    @Override
    public void updateShort(String p0, short p1) throws SQLException {
        delegate.updateShort(p0, p1);
    }

    @Override
    public void updateString(int p0, String p1) throws SQLException {
        delegate.updateString(p0, p1);
    }

    @Override
    public void updateString(String p0, String p1) throws SQLException {
        delegate.updateString(p0, p1);
    }

    @Override
    public void updateTime(int p0, Time p1) throws SQLException {
        delegate.updateTime(p0, p1);
    }

    @Override
    public void updateTime(String p0, Time p1) throws SQLException {
        delegate.updateTime(p0, p1);
    }

    @Override
    public void updateTimestamp(int p0, Timestamp p1) throws SQLException {
        delegate.updateTimestamp(p0, p1);
    }

    @Override
    public void updateTimestamp(String p0, Timestamp p1) throws SQLException {
        delegate.updateTimestamp(p0, p1);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return delegate.wasNull();
    }
    // </editor-fold>
}
