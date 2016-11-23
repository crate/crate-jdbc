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

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.hamcrest.CoreMatchers.is;

public class CrateJDBCReturningTest extends CrateJDBCIntegrationTest{

    private static Connection connection;

    @BeforeClass
    public static void setUpTest() throws Throwable {
        connection = DriverManager.getConnection(getConnectionString());
        connection.createStatement().execute("create table doc.test (id int primary key, name string)");
    }

    @Test
    public void testExecuteUpdate() throws Throwable {
        connection.createStatement().executeUpdate(
                "insert into test (id, name) values(1, 'Trillian')",
                new String[]{"id", "name"});
        connection.createStatement().execute("refresh table test");
        ResultSet rs = connection.createStatement()
                .executeQuery("SELECT count(*) FROM sys.shards WHERE state != 'STARTED'");
        rs.next();
        assertThat(rs.getLong(1), is(1L));
    }
}
