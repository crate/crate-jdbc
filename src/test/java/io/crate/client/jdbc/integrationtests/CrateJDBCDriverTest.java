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

package io.crate.client.jdbc.integrationtests;

import io.crate.client.jdbc.CrateConnection;
import io.crate.client.jdbc.CrateDriver;
import io.crate.testing.CrateTestServer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class CrateJDBCDriverTest extends CrateJDBCIntegrationTest {

    @ClassRule
    public static CrateTestServer testServer = CrateTestServer.fromVersion(CRATE_SERVER_VERSION).build();


    public String hostAndPort = String.format(Locale.ENGLISH, "%s:%d", testServer.crateHost(), testServer.transportPort());
    private CrateDriver driver;
    private static final Properties PROP = new Properties();

    @Before
    public void setUp() throws Exception {
        driver = new CrateDriver();
    }

    @Test
    public void testDriverRegistration() throws Exception {
        Class.forName("io.crate.client.jdbc.CrateDriver");

        Connection c1 = DriverManager.getConnection("crate://" + hostAndPort);
        assertThat(c1, instanceOf(CrateConnection.class));

        Connection c2 = DriverManager.getConnection("jdbc:crate://" + hostAndPort);
        assertThat(c2, instanceOf(CrateConnection.class));

        expectedException.expect(SQLException.class);
        expectedException.expectMessage(String.format("No suitable driver found for %s", "jdbc:mysql://" + hostAndPort));
        DriverManager.getConnection("jdbc:mysql://" + hostAndPort);
    }

    @Test
    public void testDriverRegistrationWithSchemaName() throws Exception {
        Class.forName("io.crate.client.jdbc.CrateDriver");
        Connection connection = DriverManager.getConnection(String.format("crate://%s/foo", hostAndPort));
        assertThat(connection.getSchema(), is("foo"));
    }

    @Test
    public void testInvalidURI() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("URL format is invalid.");
        Class.forName("io.crate.client.jdbc.CrateDriver");
        DriverManager.getConnection(String.format("crate://%s/foo/bla", hostAndPort));
    }

    @Test
    public void testAccepts() throws Exception {
        assertThat(driver.acceptsURL("crate://"), is(true));
        assertThat(driver.acceptsURL("crate://localhost/foo"), is(true));
        assertThat(driver.acceptsURL("crate:///foo"), is(true));
        assertThat(driver.acceptsURL("jdbc:crate://"), is(true));
        assertThat(driver.acceptsURL("crt://"), is(false));
        assertThat(driver.acceptsURL("jdbc:mysql://"), is(false));
    }

    @Test
    public void testConnectDriver() throws Exception {
        assertThat(driver.connect("jdbc:crate://" + hostAndPort, PROP), instanceOf(CrateConnection.class));
        CrateConnection c = (CrateConnection) driver.connect("crate://" + hostAndPort, PROP);

        assertFalse(c.isClosed());
        assertTrue(c.isValid(0));

        assertThat(driver.connect("jdbc:crate://" + hostAndPort + "/db", PROP), instanceOf(CrateConnection.class));
        assertThat(driver.connect("crate://" + hostAndPort + "/db", new Properties()), instanceOf(CrateConnection.class));

        assertThat(driver.connect("jdbc:crate://" + hostAndPort + "/db?asdf=abcd", PROP), instanceOf(CrateConnection.class));
        assertThat(driver.connect("crate://" + hostAndPort + "/db?asdf=abcd", new Properties()), instanceOf(CrateConnection.class));

        assertThat(driver.connect("crt://" + hostAndPort, PROP), is(nullValue()));
        assertThat(driver.connect("jdbc:mysql://" + hostAndPort, PROP), is(nullValue()));

        expectedException.expect(SQLException.class);
        expectedException.expectMessage(String.format("Connect to '/foo%s' failed", hostAndPort.toString()));
        assertThat(driver.connect("crate:///foo" + hostAndPort, PROP), instanceOf(CrateConnection.class));

        expectedException.expectMessage(String.format("Connect to 'localhost/foo%s' failed", hostAndPort.toString()));
        assertThat(driver.connect("crate://localhost/foo" + hostAndPort, PROP), instanceOf(CrateConnection.class));

    }

    @Test
    public void testClientIsShared() throws Exception {
        CrateConnection c1 = (CrateConnection) driver.connect("crate://" + hostAndPort, PROP);
        CrateConnection c2 = (CrateConnection) driver.connect("jdbc:crate://" + hostAndPort, PROP);
        assertThat(c1.client(), sameInstance(c2.client()));
        assertThat(driver.clientURLs().size(), is(1));
        c1.close();
        assertThat(driver.clientURLs().size(), is(1));
        c2.close();
        assertThat(driver.clientURLs().size(), is(0));

    }

    @Test
    public void testClientClosedOnFailure() throws Exception {
        try {
            driver.connect("crate://localhost:44444", PROP);
            fail("This statement should not be reached");
        } catch (SQLException e) {

        }
        assertTrue(driver.clientURLs().isEmpty());
    }

    @Test
    public void testConcurrentConnections() throws Exception {

        int threads = 30;
        final CountDownLatch latch = new CountDownLatch(threads);
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                try {
                    CrateConnection c = (CrateConnection) driver.connect("crate://" + hostAndPort, PROP);
                    assertThat(driver.clientURLs().size(), is(1));
                    c.close();
                } catch (Exception e) {
                    fail(e.toString());
                } finally {
                    latch.countDown();
                }
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            executor.execute(runnable);
        }
        latch.await();
        assertThat(driver.clientURLs().size(), is(0));

    }
}
