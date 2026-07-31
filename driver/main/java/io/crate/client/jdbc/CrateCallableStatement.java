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

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

/**
 * Adds reading a call's parameters to what {@link CratePreparedStatement}
 * already does with binding them: an OBJECT comes back as a {@code Map} and
 * an array as a {@link CrateArray}, as they do everywhere else in this
 * driver.
 *
 * <p>CrateDB has no stored procedures, so a call is an ordinary
 * parameterized statement, and its parameters are addressed by position.
 * pgJDBC implements none of the by-name forms — it answers every one of them
 * with {@code SQLFeatureNotSupportedException} — so the overrides for those
 * here settle what a parameter converts to for the day it does.</p>
 */
public class CrateCallableStatement extends ForwardingCallableStatement {

    CrateCallableStatement(CallableStatement delegate, CrateConnection connection) throws SQLException {
        super(delegate, connection);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return CrateResultSet.fromPg(callableDelegate.getObject(parameterIndex));
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return CrateResultSet.fromPg(callableDelegate.getObject(parameterName));
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return CrateResultSet.fromPg(callableDelegate.getObject(parameterIndex, map));
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return CrateResultSet.fromPg(callableDelegate.getObject(parameterName, map));
    }

    /**
     * The type a caller asks for is pgJDBC's to answer except where json is
     * what came back, which it would hand over as text. The untyped read that
     * decides which of the two it is happens only for the types json can be
     * read into, so a parameter pgJDBC can only decode into the type asked for
     * is never read untyped first.
     */
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
     * {@code Map}, a series of arrays into any {@code Collection}, and either
     * into {@code Object}. These are the types
     * {@link CrateResultSet#asType} has an answer for, and asking it about any
     * other only costs a read of the parameter.
     */
    private static boolean readableFromJson(Class<?> type) {
        return type != null
            && (type == Object.class
                || Map.class.isAssignableFrom(type)
                || Collection.class.isAssignableFrom(type));
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return wrap(callableDelegate.getArray(parameterIndex));
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return wrap(callableDelegate.getArray(parameterName));
    }

    private static Array wrap(Array array) {
        return array == null ? null : new CrateArray(array);
    }
}
