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

package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.jdbc.CrateVersion;
import org.postgresql.jdbc.PgDatabaseMetaData;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConnectionITest extends BaseIntegrationTest {

    @BeforeEach
    void setUpTables() throws Exception {
        dropAllUserTables();
        setUpTestTable();
    }

    @AfterEach
    void tearDownTables() {
        dropAllUserTables();
    }

    @Test
    public void testStrictPropertyFromUrl() throws Exception {
        URI address = serverAddress();
        String url = String.format("crate://%s:%d/doc?user=crate&strict=true",
            address.getHost(), address.getPort());

        try (Connection conn = DriverManager.getConnection(url)) {
            SQLException refused = assertThrows(SQLFeatureNotSupportedException.class, conn::commit);
            assertThat(refused.getSQLState(), is("0A000"));
        }
    }

    @Test
    public void testSetClientInfo() throws Exception {
        try (Connection conn = connect()) {
            conn.setClientInfo(new Properties());
            assertThat(conn.isValid(2), is(true));
        }
    }

    @Test
    public void testConnectWithCrateUrl() throws Exception {
        URI address = serverAddress();
        String hostAndPort = address.getHost() + ":" + address.getPort();
        Properties properties = new Properties();
        properties.setProperty("user", "crate");

        for (String path : new String[]{"/", "/doc", "/doc?application_name=probe"}) {
            for (String prefix : new String[]{"crate://", "jdbc:crate://"}) {
                try (Connection conn = DriverManager.getConnection(prefix + hostAndPort + path, properties)) {
                    assertThat(conn, is(instanceOf(PGConnection.class)));
                    assertThat(conn.isValid(2), is(true));
                }
            }
        }
    }

    @Test
    public void testConnectionWithCustomSchema() throws Exception {
        try (Connection conn = connect()) {
            conn.setSchema("foo");
            assertThat(conn.getSchema(), is("foo"));

            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE t (name STRING) WITH (number_of_replicas=0)");

            ResultSet rs = stmt.executeQuery(
                "SELECT table_schema FROM information_schema.tables WHERE table_name = 't'");
            assertThat(rs.next(), is(true));
            assertThat(rs.getString(1), is("foo"));
        }
    }

    @Test
    public void testPostgresqlUrlNotAccepted() {
        URI address = serverAddress();
        String url = "jdbc:postgresql://" + address.getHost() + ":" + address.getPort() + "/doc";

        SQLException refused = assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        assertThat(refused.getMessage(), containsString("No suitable driver"));
    }

    @Test
    public void testServerErrorSQLState() throws Exception {
        try (Connection conn = connect()) {
            SQLException missingTable = assertThrows(SQLException.class,
                () -> conn.createStatement().execute("select * from does_not_exist"));
            assertThat(missingTable.getSQLState(), is("42P01"));

            SQLException missingColumn = assertThrows(SQLException.class,
                () -> conn.createStatement().execute("select does_not_exist from test"));
            assertThat(missingColumn.getSQLState(), is("42703"));
        }
    }

    @Test
    public void testGetCrateVersion() throws Exception {
        try (Connection conn = connect()) {
            CrateVersion version = conn.getMetaData().unwrap(PgDatabaseMetaData.class).getCrateVersion();

            ResultSet reported = conn.createStatement().executeQuery(
                "select version['number'] from sys.nodes limit 1");
            assertThat(reported.next(), is(true));
            assertThat(version.compareTo(reported.getString(1)), is(0));
        }
    }

    @Test
    public void testMultipleHostsConnectionString() throws Exception {
        URI address = serverAddress();
        String hosts = "127.0.0.1:1," + address.getHost() + ":" + address.getPort();
        String url = String.format("crate://%s/doc%s", hosts,
            address.getQuery() == null ? "" : "?" + address.getQuery());

        try (Connection conn = DriverManager.getConnection(url)) {
            assertThat(conn.createStatement().execute("select 1 from sys.cluster"), is(true));
        }
    }
}
