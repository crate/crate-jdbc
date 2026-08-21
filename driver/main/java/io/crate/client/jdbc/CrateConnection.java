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

import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
public class CrateConnection extends ForwardingConnection {

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
        super(delegate);
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
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        if (!ISOLATION_LEVELS.contains(level)) {
            throw new PSQLException("Unknown transaction isolation level: " + level,
                PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw savepointsUnsupported();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw savepointsUnsupported();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw savepointsUnsupported();
    }

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
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new CrateCallableStatement(delegate.prepareCall(sql), this);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new CrateCallableStatement(delegate.prepareCall(sql, resultSetType, resultSetConcurrency), this);
    }

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
    @Override
    public synchronized DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        if (metaData == null) {
            metaData = new CrateDatabaseMetaData(delegate.getMetaData(), this);
        }
        return metaData;
    }
}
