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
import io.crate.client.jdbc.IntegrationTestSuite;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class CrateDriverTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Test
    public void testDriverRegistration() throws Exception {
        Class.forName("io.crate.client.jdbc.CrateDriver");

        String hostAndPort = String.format(Locale.ENGLISH, "%s:%d",
                IntegrationTestSuite.crateTestServer.crateHost,
                IntegrationTestSuite.crateTestServer.transportPort
                );

        Connection c1 = DriverManager.getConnection("crate://" + hostAndPort);
        assertThat(c1, instanceOf(CrateConnection.class));

        Connection c2 = DriverManager.getConnection("jdbc:crate://" + hostAndPort);
        assertThat(c2, instanceOf(CrateConnection.class));

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Protocol url jdbc-crate:// not supported. Must be one of crate:// or jdbc:crate://");

        DriverManager.getConnection("jdbc-crate://" + hostAndPort);
    }
}
