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

import io.crate.client.jdbc.CrateDriver;
import io.crate.testing.CrateTestServer;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;

public class CrateJDBCDriverTest extends CrateJDBCIntegrationTest {

    private static CrateDriver driver;
    private static final Properties PROP;

    static {
        PROP = new Properties();
        PROP.setProperty("user", "crate");
    }

    private static String hostAndPort;

    @BeforeClass
    public static void beforeClass() {
        driver = new CrateDriver();
        CrateTestServer server = testCluster.randomServer();
        hostAndPort = String.format("%s:%s", server.crateHost(), server.psqlPort());
    }

    @Test
    public void testDriverRegistration() throws Exception {
        Connection c1 = DriverManager.getConnection("crate://" + hostAndPort + "/doc?user=crate");
        assertThat(c1, instanceOf(PGConnection.class));
        c1.close();

        Connection c2 = DriverManager.getConnection("jdbc:crate://" + hostAndPort + "/doc?user=crate");
        assertThat(c1, instanceOf(PGConnection.class));
        c2.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage(
            containsString(String.format("No suitable driver found for %s", "jdbc:mysql://" + hostAndPort + "/")));
        DriverManager.getConnection("jdbc:mysql://" + hostAndPort + "/");
    }

    @Test
    public void testDriverRegistrationDoesNotOverridePostgres() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage(
                containsString(String.format("No suitable driver found for %s", "jdbc:postgresql://" + hostAndPort + "/")));
        DriverManager.getConnection("jdbc:postgresql://" + hostAndPort + "/");
    }

    private void assertInstanceOfCrateConnection(String connString, Properties prop) throws SQLException {
        try (Connection conn = driver.connect(connString, prop)) {
            assertThat(conn, instanceOf(PGConnection.class));
            assertFalse(conn.isClosed());
            assertTrue(conn.isValid(0));
        }
    }

    private void assertConnectionIsNull(String connString, Properties prop) throws SQLException {
        try (Connection conn = driver.connect(connString, prop)) {
            assertThat(conn, is(nullValue()));
        }
    }

    @Test
    public void testConnectDriver() throws Exception {
        assertInstanceOfCrateConnection("jdbc:crate://" + hostAndPort + "/", PROP);
        assertInstanceOfCrateConnection("crate://" + hostAndPort + "/", PROP);

        assertInstanceOfCrateConnection("jdbc:crate://" + hostAndPort + "/db", PROP);
        assertInstanceOfCrateConnection("crate://" + hostAndPort + "/db", PROP);

        assertInstanceOfCrateConnection("jdbc:crate://" + hostAndPort + "/db?asdf=abcd", PROP);
        assertInstanceOfCrateConnection("crate://" + hostAndPort + "/db?asdf=abcd", PROP);

        assertConnectionIsNull("crt://" + hostAndPort + "/", PROP);
        assertConnectionIsNull("jdbc:mysql://" + hostAndPort + "/", PROP);
        assertConnectionIsNull("crate://" + hostAndPort, PROP);
    }

    @Test
    @Ignore("set/get schema is not implemented")
    public void testClientPropertyUrlParser() throws Exception {
        Connection conn = DriverManager.getConnection("crate://" + hostAndPort + "/?prop1=value1&prop2=value2");
        Properties properties = conn.getClientInfo();
        assertThat(properties.size(), is(2));
        assertThat(properties.getProperty("prop1"), is("value1"));
        assertThat(properties.getProperty("prop2"), is("value2"));
        conn.close();
    }

    @Test
    @Ignore
    public void testNullProperty() throws Exception {
        Connection conn = DriverManager.getConnection("crate://" + hostAndPort);
        conn.setClientInfo(null);
        conn.close();
    }
}
