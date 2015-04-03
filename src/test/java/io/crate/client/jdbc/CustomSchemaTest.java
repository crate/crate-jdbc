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
import io.crate.shade.org.elasticsearch.action.support.PlainActionFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.PreparedStatement;
import java.sql.Statement;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class CustomSchemaTest {

    private CrateConnection conn;

    @Mock
    public CrateClient crateClient;

    @Captor
    ArgumentCaptor<SQLRequest> requestCaptor;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        conn = new CrateConnection(crateClient, "localhost:4300");
        conn.setSchema("foo");

        PlainActionFuture<SQLResponse> response = new PlainActionFuture<>();
        response.onResponse(new SQLResponse());
        when(crateClient.sql(requestCaptor.capture())).thenReturn(response);
    }

    @Test
    public void testStatementExecute() throws Exception {
        Statement statement = conn.createStatement();
        statement.execute("select * from t");

        SQLRequest request = requestCaptor.getValue();
        assertThat(request.getDefaultSchema(), is("foo"));
    }

    @Test
    public void testPreparedStatement() throws Exception {
        PreparedStatement preparedStatement = conn.prepareStatement("select * from t where x = ?");
        preparedStatement.setInt(1, 10);
        preparedStatement.execute();

        SQLRequest request = requestCaptor.getValue();
        assertThat(request.getDefaultSchema(), is("foo"));
    }
}
