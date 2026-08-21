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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC driver for CrateDB: accepts {@code crate://} and
 * {@code jdbc:crate://} URLs, connects through stock pgjdbc over the
 * PostgreSQL wire protocol, and hands out {@link CrateConnection}s that
 * adapt the few behaviors where CrateDB differs from PostgreSQL.
 *
 * <p>{@code jdbc:postgresql://} URLs are deliberately left to a PostgreSQL
 * driver, which an application may well have installed alongside this one.</p>
 */
public class CrateDriver extends org.postgresql.Driver {

    static final String CRATE_PREFIX = "crate://";
    static final String CRATE_PREFIX_LONG = "jdbc:" + CRATE_PREFIX;
    static final String PSQL_PREFIX_LONG = "jdbc:postgresql://";

    /**
     * CrateDB defaults for connection properties pgJDBC gives a PostgreSQL
     * meaning. pgJDBC resolves URL parameters ahead of the properties a
     * connection is opened with, so a value the caller sets in either place
     * wins over these.
     *
     * <ul>
     * <li>{@code PGDBNAME} is what a URL's path segment sets, and CrateDB reads
     *     it as the schema to resolve unqualified names in. Left out, pgJDBC
     *     fills in the user name, following the PostgreSQL convention of naming
     *     a database after its owner. CrateDB's default schema is
     *     {@code doc}.</li>
     * <li>{@code loadBalanceHosts} spreads connections over the hosts of a URL
     *     naming several, which for a CrateDB cluster is every node.</li>
     * <li>{@code assumeMinServerVersion} lets pgJDBC send the application name
     *     in the startup packet instead of in a round trip of its own.</li>
     * </ul>
     */
    private static final Map<String, String> DEFAULT_PROPERTIES = Map.of(
        "PGDBNAME", "doc",
        "loadBalanceHosts", "true",
        "assumeMinServerVersion", "9.5");

    private static volatile CrateDriver registeredDriver;

    static {
        try {
            register();
            deregisterBundledPgjdbc();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * In the standalone artifact, the bundled pgJDBC superclass self-registers
     * during class initialization and would answer {@code jdbc:postgresql://}
     * URLs that belong to a PostgreSQL driver the application installed on
     * purpose, so the bundled copy is taken back out. Everywhere else the
     * superclass is pgJDBC as published, whose registration is left alone.
     *
     * <p>{@code deregister()} reaches the instance through a static of pgJDBC's
     * own. Searching the DriverManager instead would call
     * {@code DriverManager.getDrivers()} from inside a class initializer the
     * DriverManager's own service scan may have started.</p>
     *
     * <p>The prefix is the one {@code build.gradle} relocates the bundled
     * classes under, and {@code devtools/VerifyArtifacts.java} holds the built
     * jar to this behavior.</p>
     */
    private static void deregisterBundledPgjdbc() throws SQLException {
        if (!CrateDriver.class.getSuperclass().getName().startsWith("io.crate.shade.")
                || !org.postgresql.Driver.isRegistered()) {
            return;
        }
        org.postgresql.Driver.deregister();
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String psqlUrl = processURL(url);
        if (psqlUrl == null) {
            return null;
        }
        // pgJDBC reports an unreadable URL as the jdbc:postgresql:// form
        // this driver rewrote it to, which the caller never wrote. Its own
        // URL is what it can act on.
        if (!super.acceptsURL(psqlUrl)) {
            throw new PSQLException(
                "Cannot read the connection URL " + url + ". A CrateDB URL names one or more "
                + "hosts, closes the host list with a '/', and may name a schema after it: "
                + CRATE_PREFIX_LONG + "localhost:5432/doc",
                PSQLState.CONNECTION_UNABLE_TO_CONNECT);
        }
        return new CrateConnection(super.connect(psqlUrl, withDefaults(info)));
    }

    /** The caller's connection properties, with the CrateDB defaults filled in. */
    static Properties withDefaults(Properties info) {
        Properties properties = new Properties();
        if (info != null) {
            properties.putAll(info);
        }
        DEFAULT_PROPERTIES.forEach(properties::putIfAbsent);
        return properties;
    }

    /**
     * Rewrites the leading {@code crate://} or {@code jdbc:crate://} scheme to
     * {@code jdbc:postgresql://}, and returns null for any other URL. Only the
     * leading scheme is rewritten, so a parameter value holding the scheme
     * string passes through untouched.
     */
    static String processURL(String url) {
        if (url == null) {
            return null;
        }
        String lowerCased = url.toLowerCase(Locale.ENGLISH);
        if (lowerCased.startsWith(CRATE_PREFIX)) {
            return PSQL_PREFIX_LONG + url.substring(CRATE_PREFIX.length());
        }
        if (lowerCased.startsWith(CRATE_PREFIX_LONG)) {
            return PSQL_PREFIX_LONG + url.substring(CRATE_PREFIX_LONG.length());
        }
        return null;
    }

    /** The properties this URL can be opened with, or nothing for a URL not ours. */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        String psqlUrl = processURL(url);
        return psqlUrl == null
            ? new DriverPropertyInfo[0]
            : super.getPropertyInfo(psqlUrl, withDefaults(info));
    }

    @Override
    public boolean acceptsURL(String url) {
        return processURL(url) != null;
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
     * Registers this driver with the {@link DriverManager}, for the crate URL
     * schemes alone. The pgjdbc superclass registers itself through its own
     * service entry.
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
