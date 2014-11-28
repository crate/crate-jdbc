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

import io.crate.client.CrateClient;
import io.crate.client.CrateTestServer;
import io.crate.client.InternalCrateClient;
import io.crate.client.jdbc.CrateConnection;
import io.crate.client.jdbc.LoggingHelper;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CrateJDBCMultiServerIntegrationTest {

    static {
        LoggingHelper.configureDefaultSafe();
    }

    @Rule
    public CrateTestServer server1 = new CrateTestServer("MultiServerJDBCTest");

    @Rule
    public CrateTestServer server2 = new CrateTestServer("MultiServerJDBCTest");

    @Test
    public void testConnectToMultipleNodes() throws Exception {
        Class.forName("io.crate.client.jdbc.CrateDriver");

        String hosts = String.format(Locale.ENGLISH, "%s:%d,%s:%d",
                server1.crateHost, server1.transportPort,
                server2.crateHost, server2.transportPort);

        Connection connection = null;
        try {
            connection = DriverManager.getConnection("crate://" + hosts);
            CrateClient client = ((CrateConnection) connection).client();
            verifyAddresses(client);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select count(*) from sys.nodes");
            resultSet.next();
            long aLong = resultSet.getLong(1);

            assertThat(aLong, is(2L));
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void verifyAddresses(CrateClient client) throws NoSuchFieldException, IllegalAccessException {
        Field internalClient = CrateClient.class.getDeclaredField("internalClient");
        internalClient.setAccessible(true);
        InternalCrateClient internalCrateClient = (InternalCrateClient) internalClient.get(client);

        Field nodesService = InternalCrateClient.class.getDeclaredField("nodesService");
        nodesService.setAccessible(true);
        TransportClientNodesService transportClientNodesService = (TransportClientNodesService) nodesService.get(internalCrateClient);
        assertThat(transportClientNodesService.transportAddresses().size(), is(2));
    }
}
