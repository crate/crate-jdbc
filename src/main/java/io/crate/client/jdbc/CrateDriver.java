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

    private static final String PROTOCOL = "jdbc";
    private static final String SUB_PROTOCOL = "crate";
    public static final String PREFIX = SUB_PROTOCOL + ":" + "//";
    public static final String LONG_PREFIX = PROTOCOL + ":" + SUB_PROTOCOL + ":" + "//";

    static {
        try {
            DriverManager.registerDriver(new CrateDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public CrateDriver() {
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url.startsWith(PROTOCOL+":")) {
            url = url.substring(LONG_PREFIX.length());
        } else if (url.startsWith(SUB_PROTOCOL+":")) {
            url = url.substring(PREFIX.length());
        } else {
            String[] parts = url.split("//");
            throw new SQLException(String.format("Protocol url %s not supported. Must be one of %s or %s", parts[0]+"//", PREFIX, LONG_PREFIX));
        }

        CrateConnection connection;
        if (url.equals("/")) {
            connection = new CrateConnection(new CrateClient(), url);
            connection.connect();
        } else {
            String[] urlParts = url.split("/");
            String hosts = urlParts[0];

            connection = new CrateConnection(new CrateClient(hosts.split(",")), url);
            connection.connect();

            if (urlParts.length == 2) {
                connection.setSchema(urlParts[1]);
            } else if (urlParts.length > 2) {
                throw new SQLException("URL format is invalid. " +
                        "Valid format is: [jdbc:]crate://[host1:port1][, host2:port2 ...]/[schema]");
            }
        }
        return connection;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(PREFIX) || url.startsWith(LONG_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return CrateDriverVersion.CURRENT.major;
    }

    @Override
    public int getMinorVersion() {
        return CrateDriverVersion.CURRENT.minor;
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
