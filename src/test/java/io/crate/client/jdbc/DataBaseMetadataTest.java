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

import io.crate.action.sql.SQLResponse;
import io.crate.client.CrateClient;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.shade.org.elasticsearch.action.ActionFuture;
import io.crate.shade.org.elasticsearch.action.support.PlainActionFuture;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class DataBaseMetadataTest {

    @Test
    public void testGetSchemas() throws Exception {
        final List<String> stmts = new LinkedList<>();
        final CrateConnection conn = mock(CrateConnection.class);
        final CrateClient mockClient = mock(CrateClient.class);
        when(conn.client()).thenReturn(mockClient);
        when(mockClient.sql(anyString())).thenAnswer(new Answer<ActionFuture<SQLResponse>>() {
            @Override
            public ActionFuture<SQLResponse> answer(InvocationOnMock invocation) throws Throwable {
                stmts.add((String) invocation.getArguments()[0]);
                final SQLResponse res = new SQLResponse(new String[]{"schema_name"},
                        new Object[][]{new Object[]{"doc"}},
                        new DataType[] {DataTypes.STRING},
                        1L, 0L, true);
                return new PlainActionFuture<SQLResponse>() {
                    @Override
                    public SQLResponse get() throws InterruptedException, ExecutionException {
                        return res;
                    }

                    @Override
                    public SQLResponse get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
                        return res;
                    }
                };
            }
        });
        CrateDatabaseMetaData metaData = new CrateDatabaseMetaData(conn);
        CrateDatabaseMetaData metaDataSpy = spy(metaData);
        doReturn("0.45.0").when(metaDataSpy).getDatabaseProductVersion();
        doReturn(45).when(metaDataSpy).getDatabaseMinorVersion();
        metaDataSpy.getSchemas();
        assertThat(stmts.size(), is(1));
        assertThat(stmts.get(0), is("select schema_name from information_schema.tables group by schema_name order by schema_name"));

        doReturn("0.46.0").when(metaDataSpy).getDatabaseProductVersion();
        doReturn(46).when(metaDataSpy).getDatabaseMinorVersion();

        metaDataSpy.getSchemas();
        assertThat(stmts.size(), is(2));
        assertThat(stmts.get(1), is("select schema_name from information_schema.schemata order by schema_name"));
    }
}
