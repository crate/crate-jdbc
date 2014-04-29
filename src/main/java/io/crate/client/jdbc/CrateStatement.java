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

import java.sql.*;

public class CrateStatement implements Statement {

    protected CrateConnection connection;
    protected SQLResponse sqlResponse;
    protected ResultSet resultSet;

    public CrateStatement(CrateConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        execute(sql);
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement executeUpdate not supported");
    }

    @Override
    public void close() throws SQLException {
        connection = null;
        if (resultSet != null) {
            resultSet.close();
            resultSet = null;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement: setMaxFieldSize not supported");
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        //throw new SQLFeatureNotSupportedException("Statement: setMaxRows not supported");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // no-op
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement setQueryTimeout not supported");
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: cancel not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        // no-op
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        sqlResponse = connection.client().sql(sql).actionGet();
        if (sqlResponse.rowCount() <= 0) {
            return false;
        }
        resultSet = new CrateResultSet(this, sqlResponse);
        return true; // TODO: fix return value
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return (int) sqlResponse.rowCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement setFetchDirection not supported");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement: setFetchSize not supported");
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement: addBatch not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        // no-op
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: getMoreResults not supported");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: getGeneratedKeys not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: executeUpdate(String sql, int autoGeneratedKeys) not supported");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: executeUpdate(String sql, int[] columnIndexes) not supported");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: executeUpdate(String sql, String[] columnNames) not supported");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: execute(String sql, int autoGeneratedKeys) not supported");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: execute(String sql, int[] columnIndexes) not supported");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement: execute(String sql, String[] columnNames) not supported");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection == null;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement: setPoolable not supported");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        if (resultSet.isClosed()) {
            close();
        }
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return true;
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

    protected void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement is closed");
        }
    }
}
