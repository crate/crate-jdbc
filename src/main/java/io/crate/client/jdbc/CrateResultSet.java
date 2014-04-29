/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.client.jdbc;

import io.crate.action.sql.SQLResponse;
import org.elasticsearch.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class CrateResultSet implements ResultSet {

    private final Statement statement;
    private final SQLResponse sqlResponse;
    private boolean closed = false;
    private ArrayIterator rowsIt;
    private List<Object> currentRow;
    private int rowIdx = -1;
    private List<String> columns;


    static class ArrayIterator implements Iterator<Object[]> {

        private final Object[][] array;
        private final int end;
        private int idx;

        public ArrayIterator(Object[][] array, int start, int end) {
            this.array = array;
            this.end = end;
            this.idx = start;
        }

        @Override
        public boolean hasNext() {
            return idx < end;
        }

        @Override
        public Object[] next() {
            return array[idx++];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }
    }


    public CrateResultSet(Statement statement, SQLResponse sqlResponse) {
        this.statement = statement;
        this.sqlResponse = sqlResponse;
        columns = Lists.newArrayList(sqlResponse.cols());
        rowsIt = new ArrayIterator(sqlResponse.rows(), 0, sqlResponse.rows().length);
    }

    @Override
    public boolean next() throws SQLException {
        if (!rowsIt.hasNext()) {
            return false;
        }
        currentRow = Lists.newArrayList(rowsIt.next());
        rowIdx++;
        return true;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getField(columnIndex);
        if (value != null) {
            return value.toString();
        }
        return null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object bool = getField(columnIndex);
        return bool == null ? false : (Boolean) bool;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return number.byteValue();
        }
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return number.shortValue();
        }
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return number.intValue();
        }
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return number.longValue();
        }
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return number.floatValue();
        }
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return number.doubleValue();
        }
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: getBigDecimal(int columnIndex, int scale) not supported");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: getBytes(int columnIndex) not supported");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return new Date(number.longValue());
        }
        return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return new Time(number.longValue());
        }
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Number number =  getNumber(columnIndex);
        if (number != null) {
            return new Timestamp(number.longValue());
        }
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public String getCursorName() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new CrateResultSetMetaData(columns);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getField(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return columns.indexOf(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(columnIndex, 0);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return rowIdx == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return rowIdx >= sqlResponse.rowCount();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return rowIdx == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return rowIdx == sqlResponse.rows().length-1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        rowsIt = new ArrayIterator(sqlResponse.rows(), 0, sqlResponse.rows().length);
        rowIdx = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        rowIdx = sqlResponse.rows().length;
        while (rowsIt.hasNext()) {
            rowsIt.next();
        }
    }

    @Override
    public boolean first() throws SQLException {
        if (sqlResponse.rows().length > 0) {
            rowsIt = new ArrayIterator(sqlResponse.rows(), 0, sqlResponse.rows().length);
            return next();
        }
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        if (sqlResponse.rows().length > 0 && rowIdx < sqlResponse.rows().length) {
            while (rowsIt.hasNext()) {
                next();
            }
            return true;
        }
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return rowIdx+1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (sqlResponse.rows().length > 0 && rowIdx < sqlResponse.rows().length) {
            while (getRow() != row) {
                next();
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        Object a = getField(columnIndex);
        return a == null ? null : (Array) a;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        }
        try {
            return new URL(string);
        } catch (MalformedURLException e) {
            throw new SQLException("Malformed url", e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(findColumn(columnLabel), x);
    }

    @Override
    public int getHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
   }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet: " + Utilities.getCurrentMethodName() + " not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass()))
        {
            return (T) this;
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    public long getCount() {
        return sqlResponse.rowCount();
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private Object getField(int columnIndex) throws SQLException {
        checkClosed();
        return currentRow.get(columnIndex - 1);
    }

    private Number getNumber(int columnIndex) throws SQLException {
        Object number = getField(columnIndex);
        return number == null ? 0 : (Number) number;
    }

    public static class Utilities {
        public static String getCurrentMethodName() {
            ByteArrayOutputStream
                    byteArrayOutputStream = new ByteArrayOutputStream();
            PrintWriter
                    printWriter = new PrintWriter(byteArrayOutputStream);
            (new Throwable()).printStackTrace(printWriter);
            printWriter.flush();
            String stackTrace = byteArrayOutputStream.toString();
            printWriter.close();

            StringTokenizer
                    stringTokenizer = new StringTokenizer(stackTrace, "\n");

            // Line 1 -- java.lang.Throwable
            stringTokenizer.nextToken();

            // Line 2 -- "at thisClass.thisMethod(file.java:line)"
            stringTokenizer.nextToken();

            // Line 3 -- "at callingClass.callingMethod(file.java:line)"
            String methodName = stringTokenizer.nextToken();
            stringTokenizer = new StringTokenizer(methodName.trim(), " (");
            stringTokenizer.nextToken();
            methodName = stringTokenizer.nextToken();

            // Return callingClass.callingMethod
            return methodName;
        }
    }
}
