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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Hands out {@link CrateResultSet}s and keeps statements attached to their
 * owning {@link CrateConnection}.
 */
public class CrateStatement extends ForwardingStatement {

    private final CrateConnection connection;

    CrateStatement(Statement delegate, CrateConnection connection) {
        super(delegate);
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return new CrateResultSet(delegate.executeQuery(sql));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet rs = delegate.getResultSet();
        return rs == null ? null : new CrateResultSet(rs);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new CrateResultSet(delegate.getGeneratedKeys());
    }

    @Override
    public Connection getConnection() {
        return connection;
    }
}
