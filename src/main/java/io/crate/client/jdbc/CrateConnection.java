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

import io.crate.client.CrateClient;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class CrateConnection implements Connection {

    private CrateClient crateClient;
    private String url;

    public CrateConnection(CrateClient crateClient, String url) {
        this.crateClient = crateClient;
        this.url = url;
    }

    public CrateClient client() {
        return crateClient;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new CrateStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new CratePreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: prepareCall not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException("AutoCommit must be enabled, this driver does not support manual commit");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        // no-op
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Rollback is not supported");
    }

    @Override
    public void close() throws SQLException {
        crateClient = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return crateClient == null;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new CrateDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        if (!readOnly) {
            throw new SQLFeatureNotSupportedException("Write mode is not supported");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Catalog is not supported");
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        //throw new SQLFeatureNotSupportedException();
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: getTransactionIsolation not supported");
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: createStatement(int resultSetType, int resultSetConcurrency) not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: prepareStatement(String sql, int resultSetType, int resultSetConcurrency) not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: prepareCall(String sql, int resultSetType, int resultSetConcurrency) not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: getTypeMap not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoint is not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoint is not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Rollback is not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoint is not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: prepareStatement(String sql, int autoGeneratedKeys) not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: prepareStatement(String sql, int[] columnIndexes) not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: prepareStatement(String sql, String[] columnNames) not supported");
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return crateClient != null;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Connection: abort not supported");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
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

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    public String getUrl() {
        return url;
    }
}
