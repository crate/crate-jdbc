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
 */

package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The {@link DatabaseMetaData} contract this driver owns: what it says CrateDB
 * is, what it lets a caller ask about, and which calls answer at all. The
 * answers it leaves to pgJDBC are pgJDBC's to pin.
 */
public class MetaDataIT extends BaseIntegrationTest {

    /**
     * Table types that exclude the index rows CrateDB exposes through
     * pg_catalog, so assertions can enumerate tables deterministically.
     */
    private static final String[] TABLE_TYPES = {"TABLE", "SYSTEM TABLE", "VIEW", "BASE TABLE"};

    private static Connection conn;

    @BeforeAll
    static void setUpTables() throws Exception {
        dropAllUserTables();
        conn = connect();
        conn.createStatement().execute("create table if not exists test.cluster (arr array(int), name string)");
        conn.createStatement().execute("create table if not exists doc.names (id int primary key, name string)");
        conn.createStatement().execute("create table if not exists my_schema.names (id int primary key, name string)");
    }

    @AfterAll
    static void tearDownTables() throws Exception {
        if (conn != null) {
            conn.close();
        }
        dropAllUserTables();
    }

    /** One column of a metadata result set, in the order the rows came in. */
    private static List<String> columnValues(ResultSet resultSet, String column) throws SQLException {
        List<String> values = new ArrayList<>();
        while (resultSet.next()) {
            values.add(resultSet.getString(column));
        }
        return values;
    }

    /** Every row of a metadata result set, as the strings its columns read as. */
    private static List<List<String>> rows(ResultSet resultSet) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        int columns = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= columns; i++) {
                row.add(resultSet.getString(i));
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * JDBC spells "any catalog" as null, and a caller that spells it as the
     * empty string means the same thing. pgJDBC reads the empty string as
     * "belonging to no catalog", which every CrateDB object fails, so the two
     * spellings have to answer alike wherever a catalog can be named.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("catalogTakingCalls")
    public void anEmptyCatalogNamesTheSameAsNone(String description, CatalogQuery query) throws Exception {
        DatabaseMetaData metaData = conn.getMetaData();
        List<List<String>> named = rows(query.run(metaData, null));

        assertThat(description, named, is(not(empty())));
        assertThat(description, rows(query.run(metaData, "")), is(named));
    }

    static Stream<Arguments> catalogTakingCalls() {
        return Stream.of(
            Arguments.of("getTables", catalogQuery((m, c) -> m.getTables(c, "doc", "names", TABLE_TYPES))),
            Arguments.of("getColumns", catalogQuery((m, c) -> m.getColumns(c, "doc", "names", null))),
            Arguments.of("getSchemas", catalogQuery((m, c) -> m.getSchemas(c, "doc"))),
            Arguments.of("getPrimaryKeys", catalogQuery((m, c) -> m.getPrimaryKeys(c, "doc", "names"))),
            Arguments.of("getBestRowIdentifier", catalogQuery((m, c) ->
                m.getBestRowIdentifier(c, "doc", "names", DatabaseMetaData.bestRowSession, true))),
            Arguments.of("getVersionColumns", catalogQuery((m, c) -> m.getVersionColumns(c, "doc", "names"))),
            Arguments.of("getFunctions", catalogQuery((m, c) -> m.getFunctions(c, "pg_catalog", "current_schema")))
        );
    }

    @FunctionalInterface
    interface CatalogQuery {
        ResultSet run(DatabaseMetaData metaData, String catalog) throws SQLException;
    }

    private static CatalogQuery catalogQuery(CatalogQuery query) {
        return query;
    }

    @Test
    public void getTablesWithNullSchemaSpansAllSchemas() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getTables("", null, "clus%", TABLE_TYPES);

        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"));
        }
        assertThat(tables, hasItems("sys.cluster", "sys.cluster_health", "test.cluster"));
    }

    @Test
    public void getTablesWithEmptySchemaMatchesNothing() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getTables("", "", "clust%", null);
        assertThat(rs.next(), is(false));
    }

    /**
     * Asking for every table type also answers with the index entries CrateDB
     * exposes through pg_class, which are not tables. A caller that wants
     * tables has to say so.
     */
    @Test
    public void getTablesWithNullTypesAlsoListsIndexEntries() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();

        assertThat(columnValues(metaData.getTables("", "doc", "names%", null), "TABLE_TYPE"),
            hasItems("TABLE", "INDEX"));
        assertThat(columnValues(metaData.getTables("", "doc", "names%", TABLE_TYPES), "TABLE_TYPE"),
            is(List.of("TABLE")));
    }

    @Test
    public void getColumnsReportsArrayColumns() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns("", "test", "clus%", "ar%");

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
    public void getColumnsWithEmptySchemaMatchesNothing() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns("", "", "clus%", "name");
        assertThat(rs.next(), is(false));
    }

    @Test
    public void getColumnsWithNullSchemaSpansAllSchemas() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
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
    public void getSchemasReportsTheSingleCatalogEverySchemaBelongsTo() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getSchemas("", "tes%");

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("test"));
        assertThat(rs.getString("TABLE_CATALOG"), is("crate"));
        assertThat(rs.next(), is(false));
    }

    @Test
    public void getSchemasWithNullPatternListsAllSchemas() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getSchemas("", null);

        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
            schemas.add(rs.getString("TABLE_SCHEM"));
        }
        assertThat(schemas, hasItems("sys", "test", "information_schema"));
    }

    @Test
    public void getColumnsListsNestedObjectColumns() throws Exception {
        // Nested object columns are addressable in SQL (e.g. name['child'])
        // and CrateDB's own catalogs list them, so getColumns exposes them
        // alongside the top-level columns.
        ResultSet resultSet = conn.getMetaData().getColumns(null, "sys", "nodes", null);
        List<String> columns = new ArrayList<>();
        while (resultSet.next()) {
            columns.add(resultSet.getString(4));
        }
        assertThat(columns, hasItems("name", "hostname"));
        assertThat(columns.stream().anyMatch(c -> c.contains("[")), is(true));
    }

    /**
     * What a result set says about its own columns, and so what a mapper reads
     * before a single row arrives. CrateDB describes a column by its
     * type alone. It sends none of three things: the table the column came
     * from, so a column is named by the label the query gave it; whether the
     * column takes null; and the precision and scale a {@code numeric(p, s)} was
     * declared with.
     */
    @Test
    public void resultSetMetaDataDescribesTheColumnsOfAQuery() throws Exception {
        conn.createStatement().execute(
            "create table if not exists rsmd (id int primary key, amount numeric(10, 2), label text)");
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(
                 "select id, amount, label as caption from rsmd where 1 = 0")) {
            ResultSetMetaData metaData = rs.getMetaData();

            assertThat(metaData.getColumnCount(), is(3));
            assertThat(metaData.getColumnType(1), is(Types.INTEGER));
            assertThat(metaData.getColumnType(2), is(Types.NUMERIC));
            assertThat(metaData.getColumnClassName(2), is("java.math.BigDecimal"));

            assertThat(metaData.getColumnLabel(3), is("caption"));
            assertThat(metaData.getColumnName(3), is("caption"));

            assertThat(metaData.isNullable(1), is(ResultSetMetaData.columnNullableUnknown));
            assertThat(metaData.getPrecision(2), is(0));
            assertThat(metaData.getScale(2), is(0));

            assertThat(rs.getType(), is(ResultSet.TYPE_FORWARD_ONLY));
            assertThat(rs.getConcurrency(), is(ResultSet.CONCUR_READ_ONLY));
        } finally {
            conn.createStatement().execute("drop table rsmd");
        }
    }

    @Test
    public void getSchemasListsBuiltinSchemas() throws Exception {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getSchemas();
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
            schemas.add(rs.getString(1));
        }
        assertThat(schemas, hasItems("doc", "sys", "information_schema", "pg_catalog"));
    }

    @Test
    public void getPrimaryKeysNamesTheKeyColumnOfATable() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("", "doc", "names");
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("doc"));
        assertThat(rs.getString("TABLE_NAME"), is("names"));
        assertThat(rs.getString("COLUMN_NAME"), is("id"));
    }

    @Test
    public void getPrimaryKeysReturnsNothingForTableWithoutPk() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("", "test", "cluster");
        assertThat(rs.next(), is(false));
    }

    @Test
    public void getPrimaryKeysWithNullSchemaDoesNotFilter() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("", null, "names");
        List<String> keys = new ArrayList<>();
        while (rs.next()) {
            keys.add(rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME")
                     + "." + rs.getString("COLUMN_NAME"));
        }
        assertThat(keys, hasItems("doc.names.id", "my_schema.names.id"));
    }

    @Test
    public void getPrimaryKeysListsCompositeKeyColumns() throws SQLException {
        conn.createStatement().execute(
            "create table if not exists t_multi_pks (id int primary key, id2 int primary key, name string)");
        try {
            // JDBC orders the key columns by name, not by their place in the key.
            assertThat(columnValues(conn.getMetaData().getPrimaryKeys("", "doc", "t_multi_pks"), "COLUMN_NAME"),
                is(List.of("id", "id2")));
        } finally {
            conn.createStatement().execute("drop table t_multi_pks");
        }
    }

    /**
     * What the metadata says CrateDB is, where pgJDBC would say what
     * PostgreSQL is. Frameworks read these before deciding what SQL to
     * generate, so an answer describing the wrong database sends the wrong SQL
     * to the server. The transaction answers belong to {@link TransactionsIT},
     * which owns that story whole.
     */
    @ParameterizedTest(name = "{0} is {1}")
    @MethodSource("crateDbAnswers")
    public void metaDataDescribesCrateDbRatherThanPostgresql(String method, Object value) throws Exception {
        assertThat(answerOf(method), is(value));
    }

    static Stream<Arguments> crateDbAnswers() {
        return Stream.of(
            Arguments.of("getDatabaseProductName", "Crate"),
            Arguments.of("getDriverName", "CrateDB JDBC Driver"),
            // Grammar CrateDB does not have.
            Arguments.of("supportsIntegrityEnhancementFacility", false),
            Arguments.of("supportsSelectForUpdate", false),
            Arguments.of("supportsRefCursors", false),
            // Identifiers are unbounded; PostgreSQL cuts them off at 63.
            Arguments.of("getMaxCatalogNameLength", 0),
            Arguments.of("getMaxColumnNameLength", 0),
            Arguments.of("getMaxCursorNameLength", 0),
            Arguments.of("getMaxProcedureNameLength", 0),
            Arguments.of("getMaxSchemaNameLength", 0),
            Arguments.of("getMaxTableNameLength", 0),
            Arguments.of("getMaxUserNameLength", 0)
        );
    }

    /**
     * The identifier limits the metadata reports are the ones the server
     * keeps: a name far past PostgreSQL's 63 characters survives being
     * created, queried and read back out of the catalog.
     */
    @Test
    public void identifiersAreNotCutOffAtPostgresqlsLimit() throws Exception {
        String longName = "t_" + "x".repeat(80);
        conn.createStatement().execute(
            "create table \"" + longName + "\" (\"" + longName + "\" int)");
        try {
            assertThat(conn.getMetaData().getMaxTableNameLength(), is(0));
            assertThat(columnValues(conn.getMetaData().getColumns(null, "doc", longName, null),
                "COLUMN_NAME"), is(List.of(longName)));
        } finally {
            conn.createStatement().execute("drop table \"" + longName + "\"");
        }
    }

    private static Object answerOf(String method) throws Exception {
        return DatabaseMetaData.class.getMethod(method).invoke(conn.getMetaData());
    }

    /**
     * The driver identifies itself rather than the pgJDBC release it builds
     * on, and reports the PostgreSQL release CrateDB emulates as the product
     * version, that being what PostgreSQL tooling reasons about.
     */
    @Test
    public void metaDataReportsThisDriverAndTheEmulatedServer() throws Exception {
        DatabaseMetaData metaData = conn.getMetaData();
        assertThat(metaData.getDriverVersion(),
            startsWith(metaData.getDriverMajorVersion() + "." + metaData.getDriverMinorVersion()));

        assertThat(metaData.getDatabaseProductVersion(), matchesRegex("\\d+\\.\\d+"));
        assertThat(metaData.getDatabaseMajorVersion(), greaterThanOrEqualTo(10));
        assertThat(metaData.getDatabaseMinorVersion(), greaterThanOrEqualTo(0));
        assertThat(metaData.getMaxConnections(), greaterThan(0));
        assertThat(metaData.getSQLKeywords().split(",").length, greaterThan(0));
    }

    /**
     * CrateDB's catalog is one database named after the product, and its
     * schemas are the ones every cluster has.
     */
    @Test
    public void metaDataListsTheSingleCatalogAndTheBuiltinSchemas() throws Exception {
        DatabaseMetaData metaData = conn.getMetaData();
        assertThat(columnValues(metaData.getCatalogs(), "TABLE_CAT"), is(List.of("crate")));
        assertThat(columnValues(metaData.getSchemas(), "TABLE_SCHEM"), hasItems("blob", "doc", "sys"));
        assertThat(columnValues(metaData.getTables(null, "sys", null, null), "TABLE_NAME"),
            hasItems("summits", "shards", "nodes"));
        assertThat(columnValues(metaData.getTableTypes(), "TABLE_TYPE"), hasItems("TABLE", "VIEW"));
    }

    /** Calls that describe a table every cluster has, and so must find rows. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("metaDataCallsWithRowsToReport")
    public void metaDataCallsWithRowsToReportFindThem(String description, MetaDataQuery query) throws Exception {
        assertThat(description, query.run(conn.getMetaData()).next(), is(true));
    }

    static Stream<Arguments> metaDataCallsWithRowsToReport() {
        return Stream.of(
            Arguments.of("columns", query(m -> m.getColumns(null, "sys", "summits", null))),
            Arguments.of("primary keys", query(m -> m.getPrimaryKeys(null, null, null))),
            Arguments.of("best row identifier", query(m ->
                m.getBestRowIdentifier(null, "sys", "summits", DatabaseMetaData.bestRowSession, true))),
            Arguments.of("version columns", query(m -> m.getVersionColumns(null, "sys", "summits"))),
            Arguments.of("functions", query(m -> m.getFunctions(null, null, "current_schema"))),
            Arguments.of("client info properties", query(DatabaseMetaData::getClientInfoProperties))
        );
    }

    /**
     * CrateDB has no foreign keys, and grants privileges per table rather
     * than per column, so the calls that ask about either find nothing to
     * report instead of failing.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("metaDataCallsWithNothingToReport")
    public void metaDataCallsWithNothingToReportAnswerEmpty(String description, MetaDataQuery query)
            throws Exception {
        assertThat(query.run(conn.getMetaData()).next(), is(false));
    }

    static Stream<Arguments> metaDataCallsWithNothingToReport() {
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

    /**
     * Calls whose rows depend on what a cluster happens to hold. What they
     * owe is an answer at all: each goes through the driver's version
     * handling, which raises rather than returns when the server is too old
     * for the catalog columns pgJDBC reads.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("metaDataCallsThatAnswer")
    public void everyMetaDataCallAnswers(String description, MetaDataQuery query) throws Exception {
        assertThat(query.run(conn.getMetaData()), notNullValue());
    }

    static Stream<Arguments> metaDataCallsThatAnswer() {
        return Stream.of(
            Arguments.of("type info", query(DatabaseMetaData::getTypeInfo)),
            Arguments.of("user-defined types", query(m -> m.getUDTs("", "sys", "t", new int[0]))),
            Arguments.of("procedures", query(m -> m.getProcedures("", "", ""))),
            Arguments.of("procedure columns", query(m -> m.getProcedureColumns("", "", "", ""))),
            Arguments.of("table privileges", query(m -> m.getTablePrivileges("", "sys", "summits")))
        );
    }

    /**
     * A metadata failure the driver has no better account of reaches the
     * caller as it came. The driver reads the server version when a metadata
     * query fails, and reports the version only when it explains the failure —
     * these calls fail on every server, so what the caller sees is the
     * original.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("metaDataCallsThatFail")
    public void aFailureTheDriverCannotExplainReachesTheCallerUnchanged(
            String description, MetaDataQuery query) {
        SQLException failure = assertThrows(SQLFeatureNotSupportedException.class,
            () -> query.run(conn.getMetaData()), description);
        assertThat(description, failure.getMessage(), not(containsString("CrateDB 6.0")));
    }

    static Stream<Arguments> metaDataCallsThatFail() {
        return Stream.of(
            Arguments.of("attributes", query(m -> m.getAttributes(null, "sys", "summits", null))),
            Arguments.of("pseudo columns", query(m -> m.getPseudoColumns(null, "sys", "summits", null))),
            Arguments.of("super tables", query(m -> m.getSuperTables(null, "sys", "summits"))),
            Arguments.of("super types", query(m -> m.getSuperTypes(null, "sys", "t")))
        );
    }

    @FunctionalInterface
    interface MetaDataQuery {
        ResultSet run(DatabaseMetaData metaData) throws SQLException;
    }

    private static MetaDataQuery query(MetaDataQuery query) {
        return query;
    }

    /**
     * Every tool that introspects a schema asks this of every table, so it
     * answers with no rows rather than stopping the introspection. The columns
     * are still the fourteen JDBC names a caller reads a row by.
     */
    @Test
    public void indexInfoAnswersWithNoRows() throws Exception {
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

    /**
     * CrateDB has no {@code PROCEDURE} in its grammar at all, so a tool is
     * told none can exist rather than reading the empty list as "none yet".
     */
    @Test
    public void storedProceduresAreReportedAsUnsupported() throws Exception {
        assertThat(conn.getMetaData().supportsStoredProcedures(), is(false));
        assertThat(conn.getMetaData().allProceduresAreCallable(), is(false));
    }

    /** A table's column count is bounded by the mapping's field limit. */
    @Test
    public void maxColumnsInTableIsCrateDbsFieldLimit() throws Exception {
        assertThat(conn.getMetaData().getMaxColumnsInTable(), is(1000));
    }

    /**
     * Rows navigate back to themselves: a caller that reaches the statement
     * from a metadata result set and asks it for its rows arrives at the ones
     * it started from.
     */
    @Test
    public void metadataRowsNavigateBackToThemselves() throws Exception {
        ResultSet schemas = conn.getMetaData().getSchemas();
        assertThat(schemas.getStatement().getResultSet(), sameInstance(schemas));
    }
}
