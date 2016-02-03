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

package io.crate.client.jdbc;

import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.client.CrateClient;
import io.crate.client.jdbc.testing.Stubs;
import io.crate.shade.org.elasticsearch.action.support.PlainActionFuture;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CrateConnectionTest {

    public CrateDriver.ClientHandle clientHandle() throws Exception {
        CrateClient crateClient = mock(CrateClient.class);
        PlainActionFuture<SQLResponse> response = new PlainActionFuture<>();
        response.onResponse(Stubs.DUMMY_RESPONSE);
        when(crateClient.sql(any(SQLRequest.class))).thenReturn(response);
        CrateDriver.ClientHandle clientHandle = mock(CrateDriver.ClientHandle.class);
        when(clientHandle.client()).thenReturn(crateClient);

        return clientHandle;
    }

    @Test
    public void testReadOnlyConnection() throws Exception {
        CrateConnection conn = new CrateConnection(clientHandle());
        conn.connect();
        assertFalse(conn.isReadOnly());
        conn.setReadOnly(true);
        assertTrue(conn.isReadOnly());
    }

    @Test
    public void testCloseConnection() throws Exception {
        CrateDriver.ClientHandle handle = clientHandle();
        doNothing().when(handle).connectionClosed();

        CrateConnection conn = new CrateConnection(handle);
        conn.connect();
        conn.close();
        verify(handle, times(1)).connectionClosed();
        assertTrue(conn.isClosed());
    }
}
