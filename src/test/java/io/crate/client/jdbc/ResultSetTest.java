/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.types.DataType;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultSetTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static ResultSet rs;

    @BeforeClass
    public static void setup() {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.cols()).thenReturn(new String[0]);
        when(sqlResponse.rows()).thenReturn(new Object[0][0]);
        when(sqlResponse.columnTypes()).thenReturn(new DataType[0]);
        rs = new CrateResultSet(mock(Statement.class), sqlResponse);
    }

    @Test
    public void testSetInvalidFetchSize() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Fetch size must be greater than or equal to 0.");
        rs.setFetchSize(-1);
    }

    @Test
    public void testIgnoreSetFetchSize() throws Exception {
        rs.setFetchSize(10);
        assertThat(0, is(rs.getFetchSize()));
    }

    @Test
    public void testSetInvalidFetchDirection() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal value for the fetch direction.");
        rs.setFetchDirection(ResultSet.FETCH_REVERSE);
    }

    @Test
    public void testDefaultFetchDirectionDirection() throws Exception {
        assertThat(ResultSet.FETCH_FORWARD, is(rs.getFetchDirection()));
    }

}
