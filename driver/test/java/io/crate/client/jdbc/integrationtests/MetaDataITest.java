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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.jdbc.PgDatabaseMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MetaDataITest extends BaseIntegrationTest {

    private static Connection conn;

    @BeforeAll
    public static void setUpTest() throws Throwable {
        dropAllUserTables();
        conn = connect();
        conn.createStatement().execute("create table test.cluster (arr array(int), name string)");
        conn.createStatement().execute("create table doc.names (id int primary key, name string)");
        conn.createStatement().execute("create table my_schema.names (id int primary key, name string)");
    }

    @AfterAll
    public static void tearDownTest() throws SQLException {
        if (conn != null) {
            conn.close();
        }
        dropAllUserTables();
    }

    @Test
    public void testGetTables() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getTables(null, "sys", "cluster", null);

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("sys"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), is("TABLE"));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetTablesWithNullSchema() throws SQLException {
        PgDatabaseMetaData metaData = (PgDatabaseMetaData) conn.getMetaData();
        ResultSet rs = metaData.getTables(null, null, "clus%", null);

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("sys"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), is("TABLE"));

        // sys.cluster_health is added to CrateDB 6.0
        if (metaData.getCrateVersion().compareTo("6.0.0") >= 0) {
            assertThat(rs.next(), is(true));
            assertThat(rs.getString("TABLE_SCHEM"), is("sys"));
            assertThat(rs.getString("TABLE_NAME"), is("cluster_health"));
            assertThat(rs.getString("TABLE_TYPE"), is("TABLE"));
        }

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), is("TABLE"));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetTablesWithEmptySchema() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getTables(null, "", "clust%", null);
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetColumns() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns(null, "test", "clus%", "ar%");

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("COLUMN_NAME"), is("arr"));
        assertThat(rs.getString("TYPE_NAME"), is("_int4"));
        assertThat(rs.getInt("DATA_TYPE"), is(Types.ARRAY));
        assertThat(rs.getInt("ORDINAL_POSITION"), is(1));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetColumnsWithEmptySchema() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns(null, "", "clust%", null);
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetColumnsWithNullSchema() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, "clus%", "name");

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
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getSchemas(null, "tes%");

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_CATALOG"), is("crate"));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetSchemasWithNullSchemaPattern() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getSchemas(null, null);

        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
            schemas.add(rs.getString("TABLE_SCHEM"));
        }
        assertThat(schemas, hasItems("sys", "test", "information_schema"));
    }

    @Test
    public void testExcludeNestedColumns() throws Exception {
        ResultSet resultSet = conn.getMetaData().getColumns(null, "sys", "nodes", null);
        while (resultSet.next()) {
            assertFalse(resultSet.getString(4).contains("."));
            assertFalse(resultSet.getString(4).contains("["));
        }
    }

    @Test
    public void testTypesResponseNoResult() throws Exception {
        ResultSet result = conn.createStatement().executeQuery("select * from test.cluster where 1=0");
        ResultSetMetaData metaData = result.getMetaData();
        assertThat(metaData.getColumnCount(), is(2));
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            // test that we can get the types, whatever they are
            assertThat(metaData.getColumnType(i), instanceOf(Integer.class));
        }
    }

    @Test
    public void testGetSchemas() throws Exception {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getSchemas();
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
            schemas.add(rs.getString(1));
        }
        assertThat(schemas, hasItems("doc", "sys", "information_schema", "pg_catalog"));
    }

    @Test
    public void testGetPrimaryKeysPk() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys(null, "doc", "names");
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("doc"));
        assertThat(rs.getString("TABLE_NAME"), is("names"));
        assertThat(rs.getString("COLUMN_NAME"), is("id"));
    }

    @Test
    public void testGetPrimaryKeysNoPk() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys(null, "test", "cluster");
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetPrimaryWithoutSchemaDoesNotFilter() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys(null, null, "names");
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
            assertThat(rs.getString("TABLE_NAME"), is("names"));
            assertThat(rs.getString("COLUMN_NAME"), is("id"));
            schemas.add(rs.getString("TABLE_SCHEM"));
        }
        assertThat(schemas, hasItems("doc", "my_schema"));
    }

    @Test
    public void testGetPrimaryMultiplePks() throws SQLException {
        conn.createStatement().execute("create table if not exists t_multi_pks (id int primary key, id2 int primary key, name string)");
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys(null, "doc", "t_multi_pks");
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("COLUMN_NAME"), is("id"));
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("COLUMN_NAME"), is("id2"));
        conn.createStatement().execute("drop table t_multi_pks");
    }

    @Test
    public void testUnsupportedMethodWithEmptyImpl() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPseudoColumns("", "doc", "information_schema", "pg_catalog");
        assertThat(rs.getMetaData().getColumnCount(), is(12));
        assertThat(rs.getMetaData().getColumnName(1), is("TABLE_CAT"));
        // The empty result doesn't contain any rows
        assertThat(rs.next(), is(false));
    }

    @Test
    public void testGetDatabaseProductName() throws Exception {
        assertThat(conn.getMetaData().getDatabaseProductName(), is("Crate"));
    }

    @Test
    public void testGetCatalogsAndTableTypes() throws Exception {
        DatabaseMetaData metaData = conn.getMetaData();
        List<String> catalogs = new ArrayList<>();
        ResultSet rs = metaData.getCatalogs();
        while (rs.next()) {
            catalogs.add(rs.getString("TABLE_CAT"));
        }
        assertThat(catalogs, is(List.of("crate")));

        List<String> tableTypes = new ArrayList<>();
        ResultSet types = metaData.getTableTypes();
        while (types.next()) {
            tableTypes.add(types.getString("TABLE_TYPE"));
        }
        assertThat(tableTypes, hasItems("TABLE", "VIEW"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("metaDataCallsWithRows")
    public void testMetaDataCallsWithRows(String description, MetaDataQuery query) throws Exception {
        assertThat(description, query.run(conn.getMetaData()).next(), is(true));
    }

    static Stream<Arguments> metaDataCallsWithRows() {
        return Stream.of(
            Arguments.of("best row identifier", query(m ->
                m.getBestRowIdentifier(null, "sys", "summits", DatabaseMetaData.bestRowSession, true))),
            Arguments.of("version columns", query(m -> m.getVersionColumns(null, "sys", "summits"))),
            Arguments.of("functions", query(m -> m.getFunctions(null, null, "current_schema"))),
            Arguments.of("client info properties", query(DatabaseMetaData::getClientInfoProperties))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("metaDataCallsWithoutRows")
    public void testMetaDataCallsWithoutRows(String description, MetaDataQuery query) throws Exception {
        assertThat(query.run(conn.getMetaData()).next(), is(false));
    }

    static Stream<Arguments> metaDataCallsWithoutRows() {
        return Stream.of(
            Arguments.of("imported keys", query(m -> m.getImportedKeys("", "sys", "summits"))),
            Arguments.of("exported keys", query(m -> m.getExportedKeys("", "sys", "summits"))),
            Arguments.of("cross references",
                query(m -> m.getCrossReference("", "sys", "jobs", "", "sys", "jobs_log"))),
            Arguments.of("column privileges",
                query(m -> m.getColumnPrivileges(null, "sys", "summits", null))),
            Arguments.of("function columns", query(m -> m.getFunctionColumns("", "", "substr", "")))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("metaDataCalls")
    public void testMetaDataCallsAnswer(String description, MetaDataQuery query) throws Exception {
        assertThat(query.run(conn.getMetaData()), notNullValue());
    }

    static Stream<Arguments> metaDataCalls() {
        return Stream.of(
            Arguments.of("type info", query(DatabaseMetaData::getTypeInfo)),
            Arguments.of("user-defined types", query(m -> m.getUDTs("", "sys", "t", new int[0]))),
            Arguments.of("procedures", query(m -> m.getProcedures("", "", ""))),
            Arguments.of("procedure columns", query(m -> m.getProcedureColumns("", "", "", ""))),
            Arguments.of("table privileges", query(m -> m.getTablePrivileges("", "sys", "summits")))
        );
    }

    @FunctionalInterface
    interface MetaDataQuery {
        ResultSet run(DatabaseMetaData metaData) throws SQLException;
    }

    private static MetaDataQuery query(MetaDataQuery query) {
        return query;
    }

    @Test
    public void testGetIndexInfo() throws Exception {
        try (ResultSet indexes = conn.getMetaData().getIndexInfo(null, "sys", "summits", true, true)) {
            assertThat(indexes.next(), is(false));

            ResultSetMetaData columns = indexes.getMetaData();
            List<String> names = new ArrayList<>();
            for (int i = 1; i <= columns.getColumnCount(); i++) {
                names.add(columns.getColumnName(i));
            }
            assertThat(names, hasItems("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_NAME", "ORDINAL_POSITION", "COLUMN_NAME", "CARDINALITY"));
        }
    }

    @Test
    public void testGetStatementFromMetaDataResultSet() throws Exception {
        ResultSet schemas = conn.getMetaData().getSchemas();
        assertThat(schemas.getStatement().getResultSet(), sameInstance(schemas));
    }
}
