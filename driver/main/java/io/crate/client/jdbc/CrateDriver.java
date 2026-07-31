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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

/**
 * JDBC driver for CrateDB: accepts {@code crate://} and
 * {@code jdbc:crate://} URLs, connects through stock pgjdbc over the
 * PostgreSQL wire protocol, and hands out {@link CrateConnection}s that
 * adapt the few behaviors where CrateDB differs from PostgreSQL.
 *
 * <p>{@code jdbc:postgresql://} URLs are deliberately not accepted; they
 * remain the province of a (possibly co-installed) PostgreSQL driver.</p>
 */
public class CrateDriver extends org.postgresql.Driver {

    private static final String CRATE_PREFIX = "crate://";
    private static final String CRATE_PREFIX_LONG = "jdbc:" + CRATE_PREFIX;
    private static final String PSQL_PREFIX_LONG = "jdbc:postgresql://";

    /**
     * Connection defaults suited to a multi-node CrateDB cluster; each
     * applies only when the caller does not set the property in the URL or
     * the {@link Properties}.
     */
    private static final String[][] DEFAULT_PROPERTIES = {
        {"loadBalanceHosts", "true"},
        {"assumeMinServerVersion", "9.5"},
    };

    private static CrateDriver registeredDriver;

    static {
        try {
            register();
            deregisterBundledPgjdbc();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * In the standalone (shaded) artifact, the bundled pgjdbc superclass
     * self-registers with the DriverManager during class initialization and
     * would answer {@code jdbc:postgresql://} URLs. Those URLs belong to a
     * real PostgreSQL driver, so the bundled copy is taken out of the
     * DriverManager again. In the unshaded artifact the superclass is
     * vanilla pgjdbc itself, whose registration must be left untouched.
     */
    private static void deregisterBundledPgjdbc() throws SQLException {
        Class<?> superClass = CrateDriver.class.getSuperclass();
        if (!superClass.getName().startsWith("io.crate.shade.")) {
            return;
        }
        for (java.sql.Driver driver : java.util.Collections.list(DriverManager.getDrivers())) {
            if (driver.getClass() == superClass) {
                DriverManager.deregisterDriver(driver);
            }
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String psqlUrl = processURL(url);
        if (psqlUrl == null) {
            return null;
        }
        Properties properties = new Properties();
        if (info != null) {
            properties.putAll(info);
        }
        for (String[] defaultProperty : DEFAULT_PROPERTIES) {
            String name = defaultProperty[0];
            if (!properties.containsKey(name) && !urlContainsParameter(psqlUrl, name)) {
                properties.setProperty(name, defaultProperty[1]);
            }
        }
        Connection connection = super.connect(psqlUrl, properties);
        return connection == null ? null : new CrateConnection(connection);
    }

    /**
     * Rewrites the leading {@code crate://} or {@code jdbc:crate://} scheme
     * to {@code jdbc:postgresql://}; returns null for any other URL. Only
     * the scheme prefix is rewritten — the remainder of the URL, including
     * parameter values that happen to contain the scheme string, passes
     * through untouched.
     */
    static String processURL(String url) {
        String lowerCased = url.toLowerCase(Locale.ENGLISH);
        if (lowerCased.startsWith(CRATE_PREFIX)) {
            return PSQL_PREFIX_LONG + url.substring(CRATE_PREFIX.length());
        }
        if (lowerCased.startsWith(CRATE_PREFIX_LONG)) {
            return PSQL_PREFIX_LONG + url.substring(CRATE_PREFIX_LONG.length());
        }
        return null;
    }

    private static boolean urlContainsParameter(String url, String name) {
        int queryStart = url.indexOf('?');
        if (queryStart < 0) {
            return false;
        }
        for (String parameter : url.substring(queryStart + 1).split("&")) {
            if (parameter.startsWith(name + "=")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean acceptsURL(String url) {
        String lowerCased = url.toLowerCase(Locale.ENGLISH);
        return lowerCased.startsWith(CRATE_PREFIX) || lowerCased.startsWith(CRATE_PREFIX_LONG);
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
     * Registers this driver with the {@link DriverManager}. The pgjdbc
     * superclass registers itself separately through its own service
     * entry; this registration only answers the crate URL schemes.
     */
    public static void register() throws SQLException {
        if (isRegistered()) {
            throw new IllegalStateException(
                "Driver is already registered. It can only be registered once.");
        }
        registeredDriver = new CrateDriver();
        DriverManager.registerDriver(registeredDriver);
    }

    public static void deregister() throws SQLException {
        if (!isRegistered()) {
            throw new IllegalStateException(
                "Driver is not registered (or it has not been registered using Driver.register() method)");
        }
        DriverManager.deregisterDriver(registeredDriver);
        registeredDriver = null;
    }

    public static boolean isRegistered() {
        return registeredDriver != null;
    }
}
