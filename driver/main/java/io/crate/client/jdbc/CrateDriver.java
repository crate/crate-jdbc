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
 * <p>{@code jdbc:postgresql://} URLs are deliberately not accepted; they
 * remain the province of a (possibly co-installed) PostgreSQL driver.</p>
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
     * <li>{@code PGDBNAME} is what a URL's path segment sets, and CrateDB
     *     reads it as the schema to resolve unqualified names in. Left out,
     *     pgJDBC fills in the user name — a PostgreSQL convention, where a
     *     database is commonly named after its owner. CrateDB has no such
     *     convention and its default schema is {@code doc}.</li>
     * <li>{@code loadBalanceHosts} spreads connections over the hosts of a
     *     URL naming several, which for a CrateDB cluster is every node.</li>
     * <li>{@code assumeMinServerVersion} lets pgJDBC send the application
     *     name in the startup packet rather than in a round trip of its
     *     own.</li>
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
     * with the DriverManager during class initialization and would answer
     * {@code jdbc:postgresql://} URLs. Those belong to a PostgreSQL driver the
     * application installed on purpose, so the bundled copy is taken out of
     * the DriverManager again. Everywhere else the superclass is pgJDBC as
     * published, whose registration must be left alone.
     *
     * <p>pgJDBC holds the instance it registered in a static of its own, which
     * {@code deregister()} takes back out. Reaching it that way rather than by
     * searching the DriverManager matters: the search would call
     * {@code DriverManager.getDrivers()}, and this runs inside a class
     * initializer that the DriverManager's own service scan may have started.</p>
     *
     * <p>The prefix is the one {@code build.gradle} relocates the bundled
     * classes under; {@code devtools/VerifyArtifacts.java} holds the built
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

    /**
     * The caller's connection properties, with the CrateDB defaults filled in
     * for the ones the caller left out.
     */
    static Properties withDefaults(Properties info) {
        Properties properties = new Properties();
        if (info != null) {
            properties.putAll(info);
        }
        DEFAULT_PROPERTIES.forEach(properties::putIfAbsent);
        return properties;
    }

    /**
     * Rewrites the leading {@code crate://} or {@code jdbc:crate://} scheme
     * to {@code jdbc:postgresql://}; returns null for any other URL. Only
     * the scheme prefix is rewritten — the remainder of the URL, including
     * parameter values that happen to contain the scheme string, passes
     * through untouched.
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

    /**
     * The properties a connection to the given URL can be opened with, or
     * nothing for a URL this driver does not answer.
     */
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
