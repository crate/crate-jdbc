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

import io.crate.client.CrateTestServer;
import io.crate.client.jdbc.CrateConnection;
import io.crate.client.jdbc.CrateDriver;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CrateDriverTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static CrateTestServer testServer = new CrateTestServer("driver");

    public String hostAndPort = String.format(Locale.ENGLISH, "%s:%d", testServer.crateHost, testServer.transportPort);

    @Test
    public void testDriverRegistration() throws Exception {
        Class.forName("io.crate.client.jdbc.CrateDriver");

        Connection c1 = DriverManager.getConnection("crate://" + hostAndPort);
        assertThat(c1, instanceOf(CrateConnection.class));

        Connection c2 = DriverManager.getConnection("jdbc:crate://" + hostAndPort);
        assertThat(c2, instanceOf(CrateConnection.class));

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Protocol url jdbc-crate:// not supported. Must be one of crate:// or jdbc:crate://");

        DriverManager.getConnection("jdbc-crate://" + hostAndPort);
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
        CrateDriver crateDriver = new CrateDriver();
        assertThat(crateDriver.acceptsURL("crate://"), is(true));
        assertThat(crateDriver.acceptsURL("crate://localhost/foo"), is(true));
        assertThat(crateDriver.acceptsURL("crate:///foo"), is(true));
        assertThat(crateDriver.acceptsURL("jdbc:crate://"), is(true));
        assertThat(crateDriver.acceptsURL("crt://"), is(false));
        assertThat(crateDriver.acceptsURL("jdbc:mysql://"), is(false));
    }
}
