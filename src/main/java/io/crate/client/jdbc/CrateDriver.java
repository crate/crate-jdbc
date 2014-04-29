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
import java.util.Properties;
import java.util.logging.Logger;

public class CrateDriver implements Driver {

    public static final String PREFIX = "crate://";

    static {
        try {
            DriverManager.registerDriver(new CrateDriver());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public CrateDriver() {
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (info != null && info.size() > 0) {
            // just ignore them for now
            // throw new UnsupportedOperationException("Properties are not supported yet");
        }

        if (url.startsWith(PREFIX)) {
            url = url.substring(PREFIX.length());
        }

        return new CrateConnection(new CrateClient(url), url);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("No parent logger");
    }

}
