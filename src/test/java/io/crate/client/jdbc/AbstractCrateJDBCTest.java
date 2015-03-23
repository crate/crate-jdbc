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

import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.client.CrateClient;
import io.crate.shade.org.elasticsearch.action.ActionFuture;
import io.crate.shade.org.elasticsearch.action.support.PlainActionFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public abstract class AbstractCrateJDBCTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected Connection connection;

    private class CrateTestDatabaseMetadata extends CrateDatabaseMetaData {

        public CrateTestDatabaseMetadata(CrateConnection connection) {
            super(connection);
        }

        @Override
        public String getDatabaseProductVersion() throws SQLException {
            return getServerVersion();
        }
    }

    @Before
    public void prepare() throws Exception {
        CrateClient crateClient = mock(CrateClient.class);
        Answer<ActionFuture<SQLResponse>> sqlAnswer = new Answer<ActionFuture<SQLResponse>>() {
            @Override
            public ActionFuture<SQLResponse> answer(InvocationOnMock invocation) throws Throwable {
                assert invocation.getArguments().length == 1;
                return fakeExecuteSQL(invocation.getArguments()[0]);
            }
        };

        Answer<ActionFuture<SQLBulkResponse>> sqlBulkAnswer = new Answer<ActionFuture<SQLBulkResponse>>() {
            @Override
            public ActionFuture<SQLBulkResponse> answer(InvocationOnMock invocation) throws Throwable {
                assert invocation.getArguments().length == 1;
                return fakeExecuteBulkSQL(invocation.getArguments()[0]);
            }
        };
        when(crateClient.sql((SQLRequest)any())).thenAnswer(sqlAnswer);
        when(crateClient.sql(anyString())).thenAnswer(sqlAnswer);
        when(crateClient.bulkSql((SQLBulkRequest) any())).thenAnswer(sqlBulkAnswer);
        CrateConnection conn = new CrateConnection(crateClient, "localhost:4300");
        connection = spy(conn);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return new CrateTestDatabaseMetadata((CrateConnection)connection);
            }
        }).when(connection).getMetaData();
        ((CrateConnection)connection).connect();
    }

    protected ActionFuture<SQLResponse> fakeExecuteSQL(Object o) {

        final SQLResponse response;

        if (o instanceof String) {
            response = getResponse(new SQLRequest((String)o));
        } else if (o instanceof SQLRequest) {
            response = getResponse((SQLRequest) o);
        } else {
            throw new IllegalArgumentException("invalid SQL requuest");
        }

        return new PlainActionFuture<SQLResponse>() {
            @Override
            public SQLResponse get() throws InterruptedException, ExecutionException {
                return response;
            }

            @Override
            public SQLResponse get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
                return response;
            }
        };
    }

    protected ActionFuture<SQLBulkResponse> fakeExecuteBulkSQL(Object o) {

        final SQLBulkResponse response;

        if (o instanceof String) {
            response = getBulkResponse(new SQLBulkRequest((String)o));
        } else if (o instanceof SQLBulkRequest) {
            response = getBulkResponse((SQLBulkRequest) o);
        } else {
            throw new IllegalArgumentException("invalid SQL Bulk request");
        }

        return new PlainActionFuture<SQLBulkResponse>() {
            @Override
            public SQLBulkResponse get() throws InterruptedException, ExecutionException {
                return response;
            }

            @Override
            public SQLBulkResponse get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
                return response;
            }
        };
    }

    protected abstract SQLResponse getResponse(SQLRequest request);

    protected abstract SQLBulkResponse getBulkResponse(SQLBulkRequest request);

    protected abstract String getServerVersion();
}
