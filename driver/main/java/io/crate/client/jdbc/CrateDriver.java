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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CrateDriver extends org.postgresql.Driver {

    private static final String PROTOCOL = "jdbc";

    private static final String CRATE_PROTOCOL = "crate";
    private static final String CRATE_PREFIX = CRATE_PROTOCOL + ":" + "//";
    private static final String CRATE_PREFIX_LONG = PROTOCOL + ":" + CRATE_PREFIX;

    private static final String PSQL_PROTOCOL = "postgresql";
    private static final String PSQL_PREFIX = PSQL_PROTOCOL + ":" + "//";
    private static final String PSQL_PREFIX_LONG = PROTOCOL + ":" + PSQL_PREFIX;

    private static Driver registeredDriver;

    /**
     * Taken from {@link org.postgresql.Driver}
     */
    static {
        try {
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        String psqlUrl = processURL(url);
        if (psqlUrl == null) {
            return null;
        }
        return super.connect(psqlUrl, info);
    }

    /*
     * Convert crate:// or jdbc:crate:// URL to jdbc:postgresql:// URL
     * Returns null if URL is invalid.
     */
    static String processURL(String url) {
        if (url.startsWith(CRATE_PREFIX)) {
            url = String.format("%s:%s", PROTOCOL, url);
        } else if (!url.startsWith(CRATE_PREFIX_LONG)) {
            return null;
        }
        return url.replace(CRATE_PREFIX_LONG, PSQL_PREFIX_LONG);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(CRATE_PREFIX) || url.startsWith(CRATE_PREFIX_LONG);
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

    /**
     * Copied from {@link org.postgresql.Driver#register()}.
     */
    public static void register() throws SQLException {
        if (isRegistered()) {
            throw new IllegalStateException(
                    "Driver is already registered. It can only be registered once.");
        }
        Driver crateDriver = new CrateDriver();
        DriverManager.registerDriver(crateDriver);
        registeredDriver = crateDriver;
    }

    /**
     * Copied from {@link org.postgresql.Driver#deregister()}.
     */
    public static void deregister() throws SQLException {
        if (!isRegistered()) {
            throw new IllegalStateException(
                    "Driver is not registered (or it has not been registered using Driver.register() method)");
        }
        DriverManager.deregisterDriver(registeredDriver);
        registeredDriver = null;
    }

    /**
     * Copied from {@link org.postgresql.Driver#isRegistered()}.
     */
    public static boolean isRegistered() {
        return registeredDriver != null;
    }
}
