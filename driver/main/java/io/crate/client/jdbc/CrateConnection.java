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

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.copy.CopyManager;
import org.postgresql.fastpath.Fastpath;
import org.postgresql.jdbc.AutoSave;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A connection to CrateDB: pgJDBC's, plus the behaviors CrateDB needs of its
 * own. It has no transactions to roll back or set an isolation level on, and
 * its own names for the types {@link #createArrayOf} builds.
 *
 * <p>Statements, calls and metadata are handed out as their Crate* wrappers,
 * so OBJECT&harr;{@code Map} conversion and the CrateDB metadata answers hold
 * wherever an application reaches them from.</p>
 */
@SuppressWarnings("deprecation")
public class CrateConnection implements Connection, PGConnection {

    protected final Connection delegate;
    protected final PGConnection pgDelegate;

    /**
     * CrateDB type names that differ from the {@code pg_catalog.pg_type} name
     * pgJDBC resolves an array element type by. A name absent here is looked
     * up lower-cased, which carries every type the two spell alike.
     *
     * <p>{@code float} is deliberately the odd one out. pgJDBC's own alias
     * table reads a PostgreSQL {@code float} as {@code float8}, while a CrateDB
     * {@code float} is the four-byte one.</p>
     */
    private static final Map<String, String> ARRAY_TYPE_ALIASES = Map.ofEntries(
        Map.entry("string", "varchar"),
        Map.entry("ip", "varchar"),
        Map.entry("character", "bpchar"),
        Map.entry("char", "bpchar"),
        Map.entry("boolean", "bool"),
        Map.entry("byte", "int2"),
        Map.entry("short", "int2"),
        Map.entry("integer", "int4"),
        Map.entry("long", "int8"),
        Map.entry("float", "float4"),
        Map.entry("real", "float4"),
        Map.entry("double", "float8"),
        Map.entry("float_vector", "float4"),
        Map.entry("object", "json"),
        Map.entry("geo_shape", "json"),
        // A geo_point value is the pair of doubles [lon, lat].
        Map.entry("geo_point", "float8"));

    /**
     * The levels {@link #setTransactionIsolation} takes. JDBC reserves
     * {@link Connection#TRANSACTION_NONE} for {@link #getTransactionIsolation}
     * to report and has no caller name it. It is taken here because a framework
     * that reads the level in order to put it back would otherwise be refused
     * the answer this connection just gave it.
     */
    private static final Set<Integer> ISOLATION_LEVELS = Set.of(
        Connection.TRANSACTION_NONE,
        Connection.TRANSACTION_READ_UNCOMMITTED,
        Connection.TRANSACTION_READ_COMMITTED,
        Connection.TRANSACTION_REPEATABLE_READ,
        Connection.TRANSACTION_SERIALIZABLE);

    /**
     * Whether a column of this type holds moments instead of wall clocks,
     * which decides what a {@link java.sql.Timestamp} in an array means.
     *
     * <p>A timestamp with a zone holds an instant, so its elements are written
     * carrying their offset. Left to pgJDBC each would go out as a wall clock
     * in the JVM's zone, and the server would read it as UTC and store a moment
     * the caller never named. A timestamp without a zone holds the wall clock
     * itself, which JDBC reads in the JVM's zone, as the single-value path
     * does.</p>
     */
    private static boolean holdsMoments(String pgTypeName) {
        return pgTypeName.equals("timestamptz")
            || pgTypeName.equals("timestamp with time zone");
    }

    /**
     * A type name as {@code pg_type} spells it. Names are matched the way SQL
     * matches them. Every PostgreSQL type name is lower case, so a name that
     * resolves at all resolves lower-cased, whatever the caller wrote.
     */
    private static String pgTypeName(String typeName) throws SQLException {
        if (typeName == null) {
            throw new PSQLException("An array needs the name of its element type",
                PSQLState.INVALID_PARAMETER_VALUE);
        }
        String lowerCased = typeName.toLowerCase(Locale.ENGLISH);
        return ARRAY_TYPE_ALIASES.getOrDefault(lowerCased, lowerCased);
    }

    private CrateDatabaseMetaData metaData;
    private CrateVersion crateVersion;

    /**
     * Wraps a connection pgJDBC opened. Only this driver builds one. The
     * wrapper resolves pgJDBC's own interface from the delegate once and holds
     * it, so a delegate that is itself a wrapper (a pool's handle) would leave
     * the two reaching different objects.
     */
    CrateConnection(Connection delegate) throws SQLException {
        this.delegate = delegate;
        this.pgDelegate = delegate.unwrap(PGConnection.class);
    }

    /**
     * Holds a statement's query timeout as the session's
     * {@code statement_timeout} until the returned handle is closed, giving the
     * session's own value back afterwards. A connection whose statements set no
     * timeout never touches the setting. {@link CrateQueryTimeout} says why the
     * setting is used at all.
     */
    CrateQueryTimeout appliedQueryTimeout(int seconds) throws SQLException {
        if (seconds == 0) {
            return CrateQueryTimeout.NONE;
        }
        long sessionMillis;
        try (Statement statement = delegate.createStatement()) {
            sessionMillis = CrateQueryTimeout.readMillis(statement);
            CrateQueryTimeout.applyMillis(statement, TimeUnit.SECONDS.toMillis(seconds));
        }
        return () -> {
            try (Statement statement = delegate.createStatement()) {
                CrateQueryTimeout.applyMillis(statement, sessionMillis);
            }
        };
    }

    /**
     * There is nothing to undo: CrateDB has no {@code ROLLBACK} statement, and
     * a statement is durable by the time it returns. This ends the transaction
     * block pgJDBC opens under manual commit mode, and does nothing else.
     *
     * <p>Leaving that block open strands the connection. Until it ends, pgJDBC
     * refuses to change the read-only flag or the isolation level, and a server
     * error marks the block failed. {@code COMMIT} ends it and CrateDB parses
     * and ignores it; pgJDBC sends nothing when no block is open.</p>
     */
    @Adapted
    @Override
    public void rollback() throws SQLException {
        checkOpen();
        if (delegate.getAutoCommit()) {
            throw new PSQLException("Cannot rollback when autoCommit is enabled.",
                PSQLState.NO_ACTIVE_SQL_TRANSACTION);
        }
        delegate.commit();
    }

    /**
     * CrateDB runs every statement on its own, so there is one isolation level
     * to be in: {@link Connection#TRANSACTION_NONE}, which
     * {@link #getTransactionIsolation} reports and
     * {@link java.sql.DatabaseMetaData#supportsTransactionIsolationLevel}
     * alone accepts. A framework asking for one of PostgreSQL's levels is
     * still obliged, since the server does nothing differently either way.
     */
    @Adapted
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        if (!ISOLATION_LEVELS.contains(level)) {
            throw new PSQLException("Unknown transaction isolation level: " + level,
                PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    @Adapted
    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return Connection.TRANSACTION_NONE;
    }

    @Adapted
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw savepointsUnsupported();
    }

    @Adapted
    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw savepointsUnsupported();
    }

    @Adapted
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw savepointsUnsupported();
    }

    @Adapted
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw savepointsUnsupported();
    }

    /**
     * Savepoints are no more part of CrateDB's SQL grammar than
     * {@code ROLLBACK} is. Refusing them here makes them the unsupported
     * feature {@link java.sql.DatabaseMetaData#supportsSavepoints} announces,
     * instead of a syntax error from the server.
     */
    private static SQLFeatureNotSupportedException savepointsUnsupported() {
        return new SQLFeatureNotSupportedException("CrateDB does not support savepoints",
            PSQLState.NOT_IMPLEMENTED.getState());
    }

    private void checkOpen() throws SQLException {
        if (delegate.isClosed()) {
            throw new PSQLException("This connection has been closed.",
                PSQLState.CONNECTION_DOES_NOT_EXIST);
        }
    }

    @Adapted
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return createArrayOf(typeName, (Object) elements);
    }

    /**
     * pgJDBC's overload, which also takes an array of primitives. Elements that
     * are themselves arrays make a {@link CrateJsonArray} whatever element type
     * was named: CrateDB reads a column of nested arrays from json, having no
     * PostgreSQL array form to send it in.
     */
    @Adapted
    @Override
    public Array createArrayOf(String typeName, Object elements) throws SQLException {
        CrateJsonArray nested = CrateJsonArray.ofNested(elements);
        if (nested != null) {
            return nested;
        }
        String pgTypeName = pgTypeName(typeName);
        Object pgElements = elements;
        if (elements instanceof Object[]) {
            if (CrateJson.isJsonType(pgTypeName)) {
                pgElements = toJson((Object[]) elements);
            } else if (holdsMoments(pgTypeName)) {
                pgElements = CrateParameters.atUtc((Object[]) elements);
            }
        }
        return new CrateArray(pgDelegate.createArrayOf(pgTypeName, pgElements));
    }

    /** OBJECT elements as json text, down through nested arrays. */
    private static Object[] toJson(Object[] elements) throws SQLException {
        Object[] json = new Object[elements.length];
        for (int i = 0; i < elements.length; i++) {
            Object element = elements[i];
            if (element instanceof Object[]) {
                json[i] = toJson((Object[]) element);
            } else if (element instanceof Map) {
                json[i] = CrateJson.write(element);
            } else {
                json[i] = element;
            }
        }
        return json;
    }

    @Adapted
    @Override
    public Statement createStatement() throws SQLException {
        return new CrateStatement(delegate.createStatement(), this);
    }

    @Adapted
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new CrateStatement(delegate.createStatement(resultSetType, resultSetConcurrency), this);
    }

    @Adapted
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new CrateStatement(delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    @Adapted
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql), this);
    }

    @Adapted
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql, autoGeneratedKeys), this);
    }

    @Adapted
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql, columnIndexes), this);
    }

    @Adapted
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql, columnNames), this);
    }

    @Adapted
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new CratePreparedStatement(delegate.prepareStatement(sql, resultSetType, resultSetConcurrency), this);
    }

    @Adapted
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new CratePreparedStatement(
            delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    @Adapted
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new CrateCallableStatement(delegate.prepareCall(sql), this);
    }

    @Adapted
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new CrateCallableStatement(delegate.prepareCall(sql, resultSetType, resultSetConcurrency), this);
    }

    @Adapted
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new CrateCallableStatement(
            delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    /**
     * The version of the CrateDB server this connection is talking to, read
     * once and kept, on the terms {@link #getMetaData} describes.
     */
    public synchronized CrateVersion getCrateVersion() throws SQLException {
        checkOpen();
        if (crateVersion == null) {
            try (Statement statement = delegate.createStatement();
                 ResultSet rs = statement.executeQuery("select version()")) {
                if (!rs.next()) {
                    throw new PSQLException("The server reported no version",
                        PSQLState.DATA_ERROR);
                }
                crateVersion = new CrateVersion(rs.getString(1));
            }
        }
        return crateVersion;
    }

    /**
     * The metadata of the database behind this connection: one object per
     * connection, as pgJDBC hands it out, however many callers ask at once.
     * The check and the assignment are two steps, so without the lock a pool
     * validating a connection while an application reads from it would hand
     * each of them its own.
     *
     * <p>The connection is checked before the kept object is given back.
     * Holding one makes the second call free, and would otherwise also let it
     * answer where the first call could not.</p>
     */
    @Adapted
    @Override
    public synchronized DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        if (metaData == null) {
            metaData = new CrateDatabaseMetaData(delegate.getMetaData(), this);
        }
        return metaData;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.isInstance(this) ? iface.cast(this) : delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }

    // The driver compiles for Java 11, where Connection has no enquoteLiteral,
    // enquoteIdentifier, enquoteNCharLiteral or isSimpleIdentifier: java.sql
    // gained those in Java 25. They are absent below on purpose, and adding
    // them fails the --release 11 compile. A connection answers them with the
    // body the interface gives them, which is all a driver built for Java 11
    // can do about a method Java 11 does not have.
    // <editor-fold defaultstate="collapsed" desc="Delegation to pgJDBC (63 methods)">

    @Override
    public void abort(Executor p0) throws SQLException {
        delegate.abort(p0);
    }

    @Override
    public void addDataType(String type, Class<? extends PGobject> klass) throws SQLException {
        pgDelegate.addDataType(type, klass);
    }

    @Override
    public void addDataType(String type, String className) {
        pgDelegate.addDataType(type, className);
    }

    @Override
    public void alterUserPassword(String user, char[] newPassword, String encryptionType) throws SQLException {
        pgDelegate.alterUserPassword(user, newPassword, encryptionType);
    }

    @Override
    public void beginRequest() throws SQLException {
        delegate.beginRequest();
    }

    @Override
    public void cancelQuery() throws SQLException {
        pgDelegate.cancelQuery();
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
    public void commit() throws SQLException {
        delegate.commit();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return delegate.createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return delegate.createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return delegate.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return delegate.createSQLXML();
    }

    @Override
    public Struct createStruct(String p0, Object[] p1) throws SQLException {
        return delegate.createStruct(p0, p1);
    }

    @Override
    public void endRequest() throws SQLException {
        delegate.endRequest();
    }

    @Override
    public String escapeIdentifier(String identifier) throws SQLException {
        return pgDelegate.escapeIdentifier(identifier);
    }

    @Override
    public String escapeLiteral(String literal) throws SQLException {
        return pgDelegate.escapeLiteral(literal);
    }

    @Override
    public boolean getAdaptiveFetch() {
        return pgDelegate.getAdaptiveFetch();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return delegate.getAutoCommit();
    }

    @Override
    public AutoSave getAutosave() {
        return pgDelegate.getAutosave();
    }

    @Override
    public int getBackendPID() {
        return pgDelegate.getBackendPID();
    }

    @Override
    public String getCatalog() throws SQLException {
        return delegate.getCatalog();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return delegate.getClientInfo();
    }

    @Override
    public String getClientInfo(String p0) throws SQLException {
        return delegate.getClientInfo(p0);
    }

    @Override
    public CopyManager getCopyAPI() throws SQLException {
        return pgDelegate.getCopyAPI();
    }

    @Override
    public int getDefaultFetchSize() {
        return pgDelegate.getDefaultFetchSize();
    }

    @Override
    public Fastpath getFastpathAPI() throws SQLException {
        return pgDelegate.getFastpathAPI();
    }

    @Override
    public int getHoldability() throws SQLException {
        return delegate.getHoldability();
    }

    @Override
    public LargeObjectManager getLargeObjectAPI() throws SQLException {
        return pgDelegate.getLargeObjectAPI();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return delegate.getNetworkTimeout();
    }

    @Override
    public PGNotification[] getNotifications() throws SQLException {
        return pgDelegate.getNotifications();
    }

    @Override
    public PGNotification[] getNotifications(int timeoutMillis) throws SQLException {
        return pgDelegate.getNotifications(timeoutMillis);
    }

    @Override
    public String getParameterStatus(String parameterName) {
        return pgDelegate.getParameterStatus(parameterName);
    }

    @Override
    public Map<String, String> getParameterStatuses() {
        return pgDelegate.getParameterStatuses();
    }

    @Override
    public PreferQueryMode getPreferQueryMode() {
        return pgDelegate.getPreferQueryMode();
    }

    @Override
    public int getPrepareThreshold() {
        return pgDelegate.getPrepareThreshold();
    }

    @Override
    public int getQueryTimeout() {
        return pgDelegate.getQueryTimeout();
    }

    @Override
    public PGReplicationConnection getReplicationAPI() {
        return pgDelegate.getReplicationAPI();
    }

    @Override
    public String getSchema() throws SQLException {
        return delegate.getSchema();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return delegate.getTypeMap();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return delegate.isReadOnly();
    }

    @Override
    public boolean isValid(int p0) throws SQLException {
        return delegate.isValid(p0);
    }

    @Override
    public String nativeSQL(String p0) throws SQLException {
        return delegate.nativeSQL(p0);
    }

    @Override
    public void setAdaptiveFetch(boolean adaptiveFetch) {
        pgDelegate.setAdaptiveFetch(adaptiveFetch);
    }

    @Override
    public void setAutoCommit(boolean p0) throws SQLException {
        delegate.setAutoCommit(p0);
    }

    @Override
    public void setAutosave(AutoSave autoSave) {
        pgDelegate.setAutosave(autoSave);
    }

    @Override
    public void setCatalog(String p0) throws SQLException {
        delegate.setCatalog(p0);
    }

    @Override
    public void setClientInfo(Properties p0) throws SQLClientInfoException {
        delegate.setClientInfo(p0);
    }

    @Override
    public void setClientInfo(String p0, String p1) throws SQLClientInfoException {
        delegate.setClientInfo(p0, p1);
    }

    @Override
    public void setDefaultFetchSize(int fetchSize) throws SQLException {
        pgDelegate.setDefaultFetchSize(fetchSize);
    }

    @Override
    public void setHoldability(int p0) throws SQLException {
        delegate.setHoldability(p0);
    }

    @Override
    public void setNetworkTimeout(Executor p0, int p1) throws SQLException {
        delegate.setNetworkTimeout(p0, p1);
    }

    @Override
    public void setPrepareThreshold(int threshold) {
        pgDelegate.setPrepareThreshold(threshold);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        pgDelegate.setQueryTimeout(seconds);
    }

    @Override
    public void setReadOnly(boolean p0) throws SQLException {
        delegate.setReadOnly(p0);
    }

    @Override
    public void setSchema(String p0) throws SQLException {
        delegate.setSchema(p0);
    }

    @Override
    public void setShardingKey(ShardingKey p0) throws SQLException {
        delegate.setShardingKey(p0);
    }

    @Override
    public void setShardingKey(ShardingKey p0, ShardingKey p1) throws SQLException {
        delegate.setShardingKey(p0, p1);
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey p0, int p1) throws SQLException {
        return delegate.setShardingKeyIfValid(p0, p1);
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey p0, ShardingKey p1, int p2) throws SQLException {
        return delegate.setShardingKeyIfValid(p0, p1, p2);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> p0) throws SQLException {
        delegate.setTypeMap(p0);
    }
    // </editor-fold>
}
