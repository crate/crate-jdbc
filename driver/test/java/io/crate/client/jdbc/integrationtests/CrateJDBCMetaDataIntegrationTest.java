/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.client.jdbc.integrationtests;

import io.crate.testing.CrateTestServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.core.Is.is;

public class CrateJDBCMetaDataIntegrationTest extends CrateJDBCIntegrationTest {

    private static Connection connection;

    @BeforeClass
    public static void beforeClass() throws Exception {
        CrateTestServer server = testCluster.randomServer();
        connection =
                DriverManager.getConnection(String.format("crate://%s:%s/", server.crateHost(), server.psqlPort()));
        connection.createStatement().execute("create table if not exists test.cluster (arr array(int), name string)");
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        connection.createStatement().execute("drop table test.cluster");
    }

    @Test
    public void testGetTables() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables("", "sys", "cluster", null);

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("sys"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), is("SYSTEM TABLE"));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetTablesWithNullSchema() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables("", null, "clus%", null);

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), is("SYSTEM TABLE"));

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), is("TABLE"));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetTablesWithEmptySchema() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables("", "", "clust%", null);
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetColumns() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns("", "test", "clus%", "ar%");

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("COLUMN_NAME"), is("arr"));
        assertThat(rs.getString("TYPE_NAME"), is("integer_array"));
        assertThat(rs.getInt("DATA_TYPE"), is(Types.ARRAY));
        assertThat(rs.getInt("ORDINAL_POSITION"), is(1));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetColumnsWithEmptySchema() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables("", "", "clust%", null);
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetColumnsWithNullSchema() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns("", null, "clus%", "name");

        assertThat(rs.next(), is(true));
        assertThat("sys", rs.getString("TABLE_SCHEM"), is("sys"));
        assertThat("cluster", rs.getString("TABLE_NAME"), is("cluster"));
        assertThat("name", rs.getString("COLUMN_NAME"), is("name"));

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("COLUMN_NAME"), is("name"));
        assertThat(rs.next(), is(false));
    }
}
