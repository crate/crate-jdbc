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

import org.postgresql.ds.PGSimpleDataSource;

import javax.naming.Reference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A {@code DataSource} handing out CrateDB-aware connections, for frameworks
 * configured with a data source instead of a JDBC URL. Host, port, database and
 * every other pgJDBC property are set as on {@link PGSimpleDataSource},
 * {@link #setUrl} accepts {@code crate://} URLs next to the
 * {@code jdbc:postgresql://} form, and the connections behave as those from
 * {@link CrateDriver} do.
 *
 * <p>Connections are opened through pgJDBC's driver directly, never through the
 * {@code DriverManager}. The standalone artifact keeps its bundled pgJDBC out
 * of the {@code DriverManager}, where a {@code jdbc:postgresql://} URL would
 * then find either no driver or a co-installed one this driver cannot adapt.</p>
 */
public class CrateDataSource extends PGSimpleDataSource {

    private static final long serialVersionUID = 1L;

    /**
     * Building the pgJDBC driver below registers it with the
     * {@code DriverManager}, and in the standalone artifact only
     * {@link CrateDriver}'s class initializer takes the bundled copy back out.
     * Many frameworks are configured with nothing but a data source, so this
     * class is often the first one reached, and class initializers run in the
     * order written. The {@code META-INF/services} entry does not cover it: the
     * {@code DriverManager} scans with the thread context class loader, which
     * never reaches the plugin loaders the standalone artifact is built for.
     */
    static {
        CrateDriver.isRegistered();
    }

    private static final org.postgresql.Driver PG_DRIVER = new org.postgresql.Driver();

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(getUser(), getPassword());
    }

    @Override
    public Connection getConnection(String user, String password) throws SQLException {
        Properties properties = new Properties();
        if (user != null) {
            properties.setProperty("user", user);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
        return new CrateConnection(
            PG_DRIVER.connect(getUrl(), CrateDriver.withDefaults(properties)));
    }

    /**
     * Takes a URL in either driver's scheme. A URL in neither goes to pgJDBC as
     * written, for pgJDBC to reject at configuration time instead of at the
     * first request for a connection.
     */
    @Override
    public void setUrl(String url) {
        String psqlUrl = CrateDriver.processURL(url);
        super.setUrl(psqlUrl == null ? url : psqlUrl);
    }

    @Override
    public void setURL(String url) {
        setUrl(url);
    }

    @Override
    public String getDescription() {
        return "CrateDB JDBC DataSource";
    }

    /** {@link CrateDataSourceFactory} rebuilds this from the directory. */
    @Override
    protected Reference createReference() {
        return new Reference(getClass().getName(), CrateDataSourceFactory.class.getName(), null);
    }
}
