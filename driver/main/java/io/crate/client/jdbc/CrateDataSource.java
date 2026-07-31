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

import org.postgresql.ds.PGSimpleDataSource;

import javax.naming.Reference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A {@code DataSource} handing out CrateDB-aware connections, for frameworks
 * configured with a data source instead of a JDBC URL. Host, port, database
 * and every other pgJDBC property are set as on
 * {@link PGSimpleDataSource}, and {@link #setUrl} accepts {@code crate://}
 * URLs next to the {@code jdbc:postgresql://} form; the connections carry the
 * same behavior as those obtained through {@link CrateDriver}.
 *
 * <p>Connections are opened through pgJDBC's driver directly rather than
 * through the {@code DriverManager}: the standalone artifact keeps its
 * bundled pgJDBC out of the {@code DriverManager} altogether, and a
 * {@code jdbc:postgresql://} URL there either finds no driver at all or
 * finds a co-installed one that this driver cannot adapt.</p>
 *
 * <p>A data source bound into JNDI comes back through
 * {@link CrateDataSourceFactory}, so it carries the CrateDB behavior on the
 * way out of the directory as well as into it.</p>
 */
public class CrateDataSource extends PGSimpleDataSource {

    private static final long serialVersionUID = 1L;

    /**
     * Building the pgJDBC driver below registers it with the
     * {@code DriverManager}, and in the standalone artifact only
     * {@link CrateDriver}'s class initializer takes the bundled copy back out.
     * An application that reaches this class first — a data source is all many
     * frameworks are configured with — would otherwise leave it there to answer
     * {@code jdbc:postgresql://} URLs. Class initializers run in the order they
     * are written, so this has to precede the field.
     *
     * <p>Loading {@link CrateDriver} through the {@code META-INF/services} entry
     * is not enough on its own: the {@code DriverManager} scans for it with the
     * thread context class loader, which never reaches a driver jar loaded in a
     * class loader of its own — the plugin directories the standalone artifact
     * is built for.</p>
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
     * Takes a URL in either driver's scheme. A URL in neither goes to pgJDBC
     * as written, which rejects what it cannot read — the data source is
     * configured long before it is asked for a connection, and that is where
     * an unreadable URL should be reported.
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

    /**
     * Names {@link CrateDataSourceFactory} as what rebuilds this data source
     * from the directory, in place of the pgJDBC factory that would answer
     * for a pgJDBC data source alone.
     */
    @Override
    protected Reference createReference() {
        return new Reference(getClass().getName(), CrateDataSourceFactory.class.getName(), null);
    }
}
