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

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class CrateJDBCMetaDataIntegrationTest extends CrateJDBCIntegrationTest {

    private static Connection connection;

    @BeforeClass
    public static void setUpTest() throws Throwable {
        connection = DriverManager.getConnection(getConnectionString());
        connection.createStatement().execute("create table test.cluster (arr array(int), name string)");
        connection.createStatement().execute("create table doc.names (id int primary key, name string)");
        connection.createStatement().execute("create table my_schema.names (id int primary key, name string)");
    }

    @Test
    public void testGetTables() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables("", "sys", "cluster", null);

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("sys"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), anyOf(is("SYSTEM TABLE"), is("BASE TABLE")));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetTablesWithNullSchema() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables("", null, "clus%", null);

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), anyOf(is("SYSTEM TABLE"), is("BASE TABLE")));

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), anyOf(is("TABLE"), is("BASE TABLE")));
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
        assertThat(rs.getString("TABLE_SCHEM"), is("sys"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("COLUMN_NAME"), is("name"));

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("COLUMN_NAME"), is("name"));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetSchemasWithSchemaPattern() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getSchemas("", "tes%");

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_CAT"), is(nullValue()));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetSchemasWithNullSchemaPattern() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getSchemas("", null);

        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
            schemas.add(rs.getString("TABLE_SCHEM"));
        }
        assertThat(schemas, hasItems("sys", "test", "information_schema"));
    }

    @Test

    public void testExcludeNestedColumns() throws Exception {
        ResultSet resultSet = connection.getMetaData().getColumns(null, "sys", "nodes", null);
        int counter = 0;
        while (resultSet.next()) {
            assertFalse(resultSet.getString(4).contains("."));
            assertFalse(resultSet.getString(4).contains("["));
            counter++;
        }
        assertThat(counter, is(15));
    }

    @Test
    public void testTypesResponseNoResult() throws Exception {
        ResultSet result = connection.createStatement().executeQuery("select * from test.cluster where 1=0");
        ResultSetMetaData metaData = result.getMetaData();
        assertThat(metaData.getColumnCount(), is(2));
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            // test that we can get the types, whatever they are
            assertThat(metaData.getColumnType(i), instanceOf(Integer.class));
        }
    }

    @Test
    public void testGetSchemas() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getSchemas();
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
            schemas.add(rs.getString(1));
        }
        assertThat(schemas, hasItems("doc", "sys", "information_schema", "pg_catalog"));
    }

    @Test
    public void testGetPrimaryKeysPk() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("", "doc", "names");
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("doc"));
        assertThat(rs.getString("TABLE_NAME"), is("names"));
        assertThat(rs.getString("COLUMN_NAME"), is("id"));
    }

    @Test
    public void testGetPrimaryKeysNoPk() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("", "test", "cluster");
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetPrimaryWithoutSchemaDoesNotFilter() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("", null, "names");
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("doc"));
        assertThat(rs.getString("TABLE_NAME"), is("names"));
        assertThat(rs.getString("COLUMN_NAME"), is("id"));
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("my_schema"));
        assertThat(rs.getString("TABLE_NAME"), is("names"));
        assertThat(rs.getString("COLUMN_NAME"), is("id"));
    }

    @Test
    public void testGetPrimaryMultiplePks() throws SQLException {
        connection.createStatement().execute("create table if not exists t_multi_pks (id int primary key, id2 int primary key, name string)");
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("", "doc", "t_multi_pks");
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("COLUMN_NAME"), is("id"));
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("COLUMN_NAME"), is("id2"));
        connection.createStatement().execute("drop table t_multi_pks");
    }

    @Test
    public void testUnsupportedMethodWithEmptyImpl() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getPseudoColumns("", "doc", "information_schema", "pg_catalog");
        assertThat(rs.getMetaData().getColumnCount(), is(12));
        assertThat(rs.getMetaData().getColumnName(1), is("TABLE_CAT"));
        // The empty result doesn't contain any rows
        assertThat(rs.next(), is(false));
    }
}
