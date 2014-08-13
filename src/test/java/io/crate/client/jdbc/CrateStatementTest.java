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
import io.crate.types.*;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrateStatementTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Connection connection;

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
        when(crateClient.sql((SQLRequest)any())).thenAnswer(sqlAnswer);
        when(crateClient.sql(anyString())).thenAnswer(sqlAnswer);

        connection = new CrateConnection(crateClient, "crate://localhost:4300");
    }

    private ActionFuture<SQLResponse> fakeExecuteSQL(Object o) {

        final SQLResponse response;

        if ((o instanceof String && ((String) o).startsWith("SELECT")) ||
                (o instanceof SQLRequest && ((SQLRequest) o).stmt().toUpperCase().startsWith("SELECT"))) {
            response = new SQLResponse(
                    new String[]{"boo", "i", "l", "f", "d", "s", "t", "o", "al", "ss", "n"},
                    new Object[][]{
                            new Object[]{true, 1, 2L, 4.5F, 34734875.3345734d,
                                    "södkjfhsudkhfjvhvb", 0L,
                                    new MapBuilder<String, Object>().put("a", 123L).map(),
                                    new Long[]{ Long.MIN_VALUE, 0L, Long.MAX_VALUE },
                                    new HashSet<String>(){{ add("a"); add("b"); add("c"); }},
                                    null
                            }
                    },
                    1,
                    System.currentTimeMillis()
            );
            response.colTypes(new DataType[]{
                    BooleanType.INSTANCE,
                    IntegerType.INSTANCE,
                    LongType.INSTANCE,
                    FloatType.INSTANCE,
                    DoubleType.INSTANCE,
                    StringType.INSTANCE,
                    TimestampType.INSTANCE,
                    ObjectType.INSTANCE,
                    new ArrayType(LongType.INSTANCE),
                    new SetType(StringType.INSTANCE),
                    NullType.INSTANCE
            });
        } else {
            response = new SQLResponse(new String[0], new Object[0][], 4L, System.currentTimeMillis());
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

    @Test
    public void testExecute() throws Exception {
        Statement statement = connection.createStatement();
        assertFalse(statement.execute("insert into test (id) values (4547)"));
        assertTrue(statement.execute("select count(*) from test"));
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        Statement statement = connection.createStatement();
        assertThat(statement.executeUpdate("insert into test (id) values (4547)"), is(4));
    }

    @Test
    public void testExecuteUpdateReturningResultSet() throws Exception {
        Statement statement = connection.createStatement();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Execution of statement returned a ResultSet");

        statement.executeUpdate("select * from test");
    }

    @Test
    public void testCloseStatementExecute() throws Exception {
        Statement statement = connection.createStatement();
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.execute("select * from test");
    }

    @Test
    public void testCloseStatementExecuteQuery() throws Exception {
        Statement statement = connection.createStatement();
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.executeQuery("select * from test");
    }

    @Test
    public void testCloseStatementGetResultSet() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("select * from test");
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.getResultSet();
    }

    @Test
    public void testCloseStatementGetConnection() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("select * from test");
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.getConnection();
    }

    @Test
    public void testCloseStatementExecuteUpdate() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("select * from test");
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.executeUpdate("insert into t (id) values(3)");
    }

    @Test
    public void testCloseStatementResultSetsClosed() throws Exception {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test");


        statement.close();
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("ResultSet is closed");

        resultSet.first();

    }

}
