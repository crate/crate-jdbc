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
import io.crate.shade.org.elasticsearch.client.transport.TransportClientNodesService;
import org.junit.ClassRule;
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

    @ClassRule
    public static CrateTestServer server1;

    @ClassRule
    public static CrateTestServer server2;

    static {
        CrateTestServer[] cluster = CrateTestServer.cluster("MultiServerJDBCTest", 2);
        server1 = cluster[0];
        server2 = cluster[1];

    }

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
            int tries = 3;
            long numNodes = 0;
            while (tries > 0) {
                ResultSet resultSet = statement.executeQuery("select count(*) from sys.nodes");
                resultSet.next();
                numNodes = resultSet.getLong(1);

                if (numNodes == 2L) {
                    Thread.sleep(10);
                    break;
                }
                tries--;
            }
            assertThat("nodes did not join a cluster yet", numNodes, is(2L));
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
