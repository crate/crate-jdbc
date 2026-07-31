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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Pins the {@link DatabaseMetaData} contract exposed through the crate://
 * driver: the product name is reported as {@code Crate}, an empty-string
 * catalog argument matches everything (CrateDB has a single catalog), and
 * the remaining surface behaves as stock pgjdbc talking to CrateDB.
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

    @Test
    public void getTablesAcceptsEmptyStringCatalog() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getTables("", "sys", "cluster", TABLE_TYPES);

        assertThat(rs.next(), is(true));
        assertThat(rs.getString("TABLE_SCHEM"), is("sys"));
        assertThat(rs.getString("TABLE_NAME"), is("cluster"));
        assertThat(rs.getString("TABLE_TYPE"), is("TABLE"));
        assertThat(rs.next(), is(false));
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
    public void getSchemasAcceptsEmptyStringCatalogWithPattern() throws SQLException {
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

    @Test
    public void resultSetMetaDataAvailableWithoutRows() throws Exception {
        ResultSet result = conn.createStatement().executeQuery("select * from test.cluster where 1=0");
        ResultSetMetaData metaData = result.getMetaData();
        assertThat(metaData.getColumnCount(), is(2));
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            assertThat(metaData.getColumnType(i), instanceOf(Integer.class));
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
    public void getPrimaryKeysAcceptsEmptyStringCatalog() throws SQLException {
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
        conn.createStatement().execute("create table if not exists t_multi_pks (id int primary key, id2 int primary key, name string)");
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("", "doc", "t_multi_pks");
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("COLUMN_NAME"), is("id"));
        assertThat(rs.next(), is(true));
        assertThat(rs.getString("COLUMN_NAME"), is("id2"));
        conn.createStatement().execute("drop table t_multi_pks");
    }

    @Test
    public void test_allProceduresAreCallable() throws Exception {
        assertThat(conn.getMetaData().allProceduresAreCallable(), is(true));
    }

    @Test
    public void test_allTablesAreSelectable() throws Exception {
        assertThat(conn.getMetaData().allTablesAreSelectable(), is(true));
    }

    @Test
    public void test_autoCommitFailureClosesAllResultSets() throws Exception {
        assertThat(conn.getMetaData().autoCommitFailureClosesAllResultSets(), is(false));
    }

    @Test
    @Disabled
    public void test_dataDefinitionCausesTransactionCommit() throws Exception {
        assertThat(conn.getMetaData().dataDefinitionCausesTransactionCommit(), is(true));
    }

    @Test
    public void test_dataDefinitionIgnoredInTransactions() throws Exception {
        assertThat(conn.getMetaData().dataDefinitionIgnoredInTransactions(), is(false));
    }

    @Test
    public void test_deletesAreDetected() throws Exception {
        assertThat(conn.getMetaData().deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY), is(false));
    }

    @Test
    public void test_doesMaxRowSizeIncludeBlobs() throws Exception {
        assertThat(conn.getMetaData().doesMaxRowSizeIncludeBlobs(), is(false));
    }

    @Test
    public void test_generatedKeyAlwaysReturned() throws Exception {
        assertThat(conn.getMetaData().generatedKeyAlwaysReturned(), is(true));
    }

    @Test
    @Disabled("Not implemented in PostgreSQL JDBC")
    public void test_getAttributes() throws Exception {
        conn.getMetaData().getAttributes("", "", "", "");
    }

    @Test
    public void test_getBestRowIdentifier() throws Exception {
        var result = conn.getMetaData().getBestRowIdentifier(null, "sys", "summits", DatabaseMetaData.bestRowSession, true);
        assertThat(result.next(), is(true));
    }

    @Test
    public void test_getCatalogSeparator() throws Exception {
        assertThat(conn.getMetaData().getCatalogSeparator(), is("."));
    }

    @Test
    public void test_getCatalogTerm() throws Exception {
        assertThat(conn.getMetaData().getCatalogTerm(), is("database"));
    }

    @Test
    public void test_getCatalogs() throws Exception {
        var result = conn.getMetaData().getCatalogs();
        assertThat(result.next(), is(true));
        assertThat(result.getString(1), is("crate"));
    }

    @Test
    public void test_getClientInfoProperties() throws Exception {
        var result = conn.getMetaData().getClientInfoProperties();
        assertThat(result.next(), is(true));
        assertThat(result.getString(1), is("ApplicationName"));
    }

    @Test
    @Disabled("https://github.com/crate/crate/issues/9568")
    public void test_getColumnPrivileges() throws Exception {
        var results = conn.getMetaData().getColumnPrivileges("", "sys", "summits", "");
        assertThat(results.next(), is(true));
    }

    @Test
    public void test_getColumns() throws Exception {
        var results = conn.getMetaData().getColumns(null, "sys", "summits", null);
        assertThat(results.next(), is(true));
        assertThat(results.getString(3), is("summits"));
        assertThat(results.getString(4), is("classification"));
    }

    @Test
    public void test_getCrossReference() throws Exception {
        var results = conn.getMetaData().getCrossReference("", "sys", "jobs", "", "sys", "jobs_log");
        assertThat(results.next(), is(false));
    }

    @Test
    public void test_getDatabaseMajorVersion() throws Exception {
        assertThat(conn.getMetaData().getDatabaseMajorVersion(), is(14));
    }

    @Test
    public void test_getDatabaseMinorVersion() throws Exception {
        assertThat(conn.getMetaData().getDatabaseMinorVersion(), is(0));
    }

    @Test
    public void test_getDatabaseProductName() throws Exception {
        assertThat(conn.getMetaData().getDatabaseProductName(), is("Crate"));
    }

    @Test
    public void test_getDatabaseProductVersion() throws Exception {
        assertThat(conn.getMetaData().getDatabaseProductVersion(), is("14.0"));
    }

    @Test
    @Disabled("Not supported by CrateDB: https://github.com/crate/crate/issues/15113")
    public void test_getDefaultTransactionIsolation() throws Exception {
        assertThat(conn.getMetaData().getDefaultTransactionIsolation(), is(Connection.TRANSACTION_READ_COMMITTED));
    }

    @Test
    public void test_getExportedKeys() throws Exception {
        var results = conn.getMetaData().getExportedKeys("", "sys", "summits");
        assertThat(results.next(), is(false));
    }

    @Test
    public void test_getExtraNameCharacters() throws Exception {
        assertThat(conn.getMetaData().getExtraNameCharacters(), is(""));
    }

    @Test
    public void test_getFunctionColumns() throws Exception {
        var results = conn.getMetaData().getFunctionColumns("", "", "substr", "");
        assertThat(results.next(), is(false));
    }

    @Test
    public void test_getFunctions() throws Exception {
        var results = conn.getMetaData().getFunctions(null, null, "current_schema");
        assertThat(results.next(), is(true));
    }

    @Test
    public void test_getIdentifierQuoteString() throws Exception {
        assertThat(conn.getMetaData().getIdentifierQuoteString(), is("\""));
    }

    @Test
    public void test_getImportedKeys() throws Exception {
        var results = conn.getMetaData().getImportedKeys("", "sys", "summits");
        assertThat(results.next(), is(false));
    }

    @Test
    @Disabled("Blocked by https://github.com/crate/crate/issues/5463")
    public void test_getIndexInfo() throws Exception {
        var results = conn.getMetaData().getIndexInfo("", "sys", "summits", true, true);
        assertThat(results.next(), is(false));
    }

    @Test
    public void test_getMaxBinaryLiteralLength() throws Exception {
        assertThat(conn.getMetaData().getMaxBinaryLiteralLength(), is(0));
    }

    @Test
    public void test_getMaxCatalogNameLength() throws Exception {
        assertThat(conn.getMetaData().getMaxCatalogNameLength(), is(63));
    }

    @Test
    public void test_getMaxCharLiteralLength() throws Exception {
        assertThat(conn.getMetaData().getMaxCharLiteralLength(), is(0));
    }

    @Test
    public void test_getMaxColumnNameLength() throws Exception {
        assertThat(conn.getMetaData().getMaxColumnNameLength(), is(63));
    }

    @Test
    public void test_getMaxColumnsInGroupBy() throws Exception {
        assertThat(conn.getMetaData().getMaxColumnsInGroupBy(), is(0));
    }

    @Test
    public void test_getMaxColumnsInIndex() throws Exception {
        assertThat(conn.getMetaData().getMaxColumnsInIndex(), is(32));
    }

    @Test
    public void test_getMaxColumnsInOrderBy() throws Exception {
        assertThat(conn.getMetaData().getMaxColumnsInOrderBy(), is(0));
    }

    @Test
    public void test_getMaxColumnsInSelect() throws Exception {
        assertThat(conn.getMetaData().getMaxColumnsInSelect(), is(0));
    }

    @Test
    public void test_getMaxColumnsInTable() throws Exception {
        assertThat(conn.getMetaData().getMaxColumnsInTable(), is(1600));
    }

    @Test
    public void test_getMaxConnections() throws Exception {
        assertThat(conn.getMetaData().getMaxConnections(), is(8192));
    }

    @Test
    public void test_getMaxCursorNameLength() throws Exception {
        assertThat(conn.getMetaData().getMaxCursorNameLength(), is(63));
    }

    @Test
    public void test_getMaxIndexLength() throws Exception {
        assertThat(conn.getMetaData().getMaxIndexLength(), is(0));
    }

    @Test
    public void test_getMaxLogicalLobSize() throws Exception {
        assertThat(conn.getMetaData().getMaxLogicalLobSize(), is(0L));
    }

    @Test
    public void test_getMaxProcedureNameLength() throws Exception {
        assertThat(conn.getMetaData().getMaxProcedureNameLength(), is(63));
    }

    @Test
    public void test_getMaxRowSize() throws Exception {
        assertThat(conn.getMetaData().getMaxRowSize(), is(1073741824));
    }

    @Test
    public void test_getMaxSchemaNameLength() throws Exception {
        assertThat(conn.getMetaData().getMaxSchemaNameLength(), is(63));
    }

    @Test
    public void test_getMaxStatementLength() throws Exception {
        assertThat(conn.getMetaData().getMaxStatementLength(), is(0));
    }

    @Test
    public void test_getMaxStatements() throws Exception {
        assertThat(conn.getMetaData().getMaxStatements(), is(0));
    }

    @Test
    public void test_getMaxTableNameLength() throws Exception {
        assertThat(conn.getMetaData().getMaxTableNameLength(), is(63));
    }

    @Test
    public void test_getMaxTablesInSelect() throws Exception {
        assertThat(conn.getMetaData().getMaxTablesInSelect(), is(0));
    }

    @Test
    public void test_getMaxUserNameLength() throws Exception {
        assertThat(conn.getMetaData().getMaxUserNameLength(), is(63));
    }

    @Test
    public void test_getNumericFunctions() throws Exception {
        assertThat(
            conn.getMetaData().getNumericFunctions(),
            is("abs,acos,asin,atan,atan2,ceiling,cos,cot,degrees,exp,floor,log,log10,mod,pi,power,radians,round,sign,sin,sqrt,tan,truncate")
        );
    }

    @Test
    public void test_getPrimaryKeys() throws Exception {
        ResultSet results = conn.getMetaData().getPrimaryKeys(null, null, null);
        assertThat(results.next(), is(true));
    }

    @Test
    public void test_getProcedureColumns() throws Exception {
        conn.getMetaData().getProcedureColumns("", "", "", "");
    }

    @Test
    public void test_getProcedureTerm() throws Exception {
        assertThat(conn.getMetaData().getProcedureTerm(), is("function"));
    }

    @Test
    public void test_getProcedures() throws Exception {
        conn.getMetaData().getProcedures("", "", "");
    }

    @Test
    @Disabled("Not implemented by PostgreSQL JDBC")
    public void test_getPseudoColumns() throws Exception {
        conn.getMetaData().getPseudoColumns("", "sys", "summits", "m");
    }

    @Test
    public void test_getResultSetHoldability() throws Exception {
        assertThat(conn.getMetaData().getResultSetHoldability(), is(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }

    @Test
    @Disabled("Not implemented by PostgreSQL JDBC")
    public void test_getRowIdLifetime() throws Exception {
        assertThat(conn.getMetaData().getRowIdLifetime(), is(RowIdLifetime.ROWID_UNSUPPORTED));
    }

    @Test
    public void test_getSQLKeywords() throws Exception {
        assertThat(conn.getMetaData().getSQLKeywords(), Matchers.containsString("summary"));
    }

    @Test
    public void test_getSQLStateType() throws Exception {
        assertThat(conn.getMetaData().getSQLStateType(), is(DatabaseMetaData.sqlStateSQL));
    }

    @Test
    public void test_getSchemaTerm() throws Exception {
        assertThat(conn.getMetaData().getSchemaTerm(), is("schema"));
    }

    @Test
    public void test_getSchemas() throws Exception {
        var results = conn.getMetaData().getSchemas();
        assertThat(results.next(), is(true));
        assertThat(results.getString(1), is("blob"));
    }

    @Test
    public void test_getSearchStringEscape() throws Exception {
        assertThat(conn.getMetaData().getSearchStringEscape(), is("\\"));
    }

    @Test
    public void test_getStringFunctions() throws Exception {
        assertThat(
            conn.getMetaData().getStringFunctions(),
            is("ascii,char,concat,lcase,left,length,ltrim,repeat,rtrim,space,substring,ucase,replace")
        );
    }

    @Test
    @Disabled("Not implemented in PostgreSQL JDBC")
    public void test_getSuperTables() throws Exception {
        conn.getMetaData().getSuperTables("", "sys", "summits");
    }

    @Test
    @Disabled("Not implemented in PostgreSQL JDBC")
    public void test_getSuperTypes() throws Exception {
        conn.getMetaData().getSuperTypes("", "sys", "t");
    }

    @Test
    public void test_getSystemFunctions() throws Exception {
        assertThat(conn.getMetaData().getSystemFunctions(), is("database,ifnull,user"));
    }

    @Test
    public void test_getTablePrivileges() throws Exception {
        conn.getMetaData().getTablePrivileges("", "sys", "summits");
    }

    @Test
    public void test_getTableTypes() throws Exception {
        var results = conn.getMetaData().getTableTypes();
        assertThat(results.next(), is(true));
    }

    @Test
    public void test_getTables() throws Exception {
        var results = conn.getMetaData().getTables(null, "sys", null, null);
        assertThat(results.next(), is(true));
        assertThat(results.getString(3), is("allocations_pkey"));
    }

    @Test
    public void test_getTimeDateFunctions() throws Exception {
        assertThat(
            conn.getMetaData().getTimeDateFunctions(),
            is("curdate,curtime,dayname,dayofmonth,dayofweek,dayofyear,hour,minute,month,monthname,now,quarter,second,week,year,timestampadd")
        );
    }

    @Test
    public void test_getTypeInfo() throws Exception {
        conn.getMetaData().getTypeInfo();
    }

    @Test
    public void test_getUDTs() throws Exception {
        conn.getMetaData().getUDTs("", "sys", "t", new int[0]);
    }

    @Test
    public void test_getVersionColumns() throws Exception {
        var results = conn.getMetaData().getVersionColumns(null, "sys", "summits");
        assertThat(results.next(), is(true));
        assertThat(results.getString(2), is("ctid"));
    }

    @Test
    public void test_insertsAreDetected() throws Exception {
        assertThat(conn.getMetaData().insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY), is(false));
    }

    @Test
    public void test_isCatalogAtStart() throws Exception {
        assertThat(conn.getMetaData().isCatalogAtStart(), is(true));
    }

    @Test
    public void test_locatorsUpdateCopy() throws Exception {
        assertThat(conn.getMetaData().locatorsUpdateCopy(), is(true));
    }

    @Test
    public void test_nullPlusNonNullIsNull() throws Exception {
        assertThat(conn.getMetaData().nullPlusNonNullIsNull(), is(true));
    }

    @Test
    public void test_nullsAreSortedAtEnd() throws Exception {
        assertThat(conn.getMetaData().nullsAreSortedAtEnd(), is(false));
    }

    @Test
    public void test_nullsAreSortedAtStart() throws Exception {
        assertThat(conn.getMetaData().nullsAreSortedAtStart(), is(false));
    }

    @Test
    public void test_nullsAreSortedHigh() throws Exception {
        assertThat(conn.getMetaData().nullsAreSortedHigh(), is(true));
    }

    @Test
    public void test_nullsAreSortedLow() throws Exception {
        assertThat(conn.getMetaData().nullsAreSortedLow(), is(false));
    }

    @Test
    public void test_othersDeletesAreVisible() throws Exception {
        assertThat(conn.getMetaData().othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY), is(false));
    }

    @Test
    public void test_othersInsertsAreVisible() throws Exception {
        assertThat(conn.getMetaData().othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY), is(false));
    }

    @Test
    public void test_othersUpdatesAreVisible() throws Exception {
        assertThat(conn.getMetaData().othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY), is(false));
    }

    @Test
    public void test_ownDeletesAreVisible() throws Exception {
        assertThat(conn.getMetaData().ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY), is(true));
    }

    @Test
    public void test_ownInsertsAreVisible() throws Exception {
        assertThat(conn.getMetaData().ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY), is(true));
    }

    @Test
    public void test_ownUpdatesAreVisible() throws Exception {
        assertThat(conn.getMetaData().ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY), is(true));
    }

    @Test
    public void test_storesLowerCaseIdentifiers() throws Exception {
        assertThat(conn.getMetaData().storesLowerCaseIdentifiers(), is(true));
    }

    @Test
    public void test_storesLowerCaseQuotedIdentifiers() throws Exception {
        assertThat(conn.getMetaData().storesLowerCaseQuotedIdentifiers(), is(false));
    }

    @Test
    public void test_storesMixedCaseIdentifiers() throws Exception {
        assertThat(conn.getMetaData().storesMixedCaseIdentifiers(), is(false));
    }

    @Test
    public void test_storesMixedCaseQuotedIdentifiers() throws Exception {
        assertThat(conn.getMetaData().storesMixedCaseQuotedIdentifiers(), is(false));
    }

    @Test
    public void test_storesUpperCaseIdentifiers() throws Exception {
        assertThat(conn.getMetaData().storesUpperCaseIdentifiers(), is(false));
    }

    @Test
    public void test_storesUpperCaseQuotedIdentifiers() throws Exception {
        assertThat(conn.getMetaData().storesUpperCaseQuotedIdentifiers(), is(false));
    }

    @Test
    public void test_supportsANSI92EntryLevelSQL() throws Exception {
        assertThat(conn.getMetaData().supportsANSI92EntryLevelSQL(), is(true));
    }

    @Test
    public void test_supportsANSI92FullSQL() throws Exception {
        assertThat(conn.getMetaData().supportsANSI92FullSQL(), is(false));
    }

    @Test
    public void test_supportsANSI92IntermediateSQL() throws Exception {
        assertThat(conn.getMetaData().supportsANSI92IntermediateSQL(), is(false));
    }

    @Test
    public void test_supportsAlterTableWithAddColumn() throws Exception {
        assertThat(conn.getMetaData().supportsAlterTableWithAddColumn(), is(true));
    }

    @Test
    public void test_supportsAlterTableWithDropColumn() throws Exception {
        assertThat(conn.getMetaData().supportsAlterTableWithDropColumn(), is(true));
    }

    @Test
    public void test_supportsBatchUpdates() throws Exception {
        assertThat(conn.getMetaData().supportsBatchUpdates(), is(true));
    }

    @Test
    public void test_supportsCatalogsInDataManipulation() throws Exception {
        assertThat(conn.getMetaData().supportsCatalogsInDataManipulation(), is(false));
    }

    @Test
    public void test_supportsCatalogsInIndexDefinitions() throws Exception {
        assertThat(conn.getMetaData().supportsCatalogsInIndexDefinitions(), is(false));
    }

    @Test
    public void test_supportsCatalogsInPrivilegeDefinitions() throws Exception {
        assertThat(conn.getMetaData().supportsCatalogsInPrivilegeDefinitions(), is(false));
    }

    @Test
    public void test_supportsCatalogsInProcedureCalls() throws Exception {
        assertThat(conn.getMetaData().supportsCatalogsInProcedureCalls(), is(false));
    }

    @Test
    public void test_supportsCatalogsInTableDefinitions() throws Exception {
        assertThat(conn.getMetaData().supportsCatalogsInTableDefinitions(), is(false));
    }

    @Test
    public void test_supportsColumnAliasing() throws Exception {
        assertThat(conn.getMetaData().supportsColumnAliasing(), is(true));
    }

    @Test
    public void test_supportsConvert() throws Exception {
        assertThat(conn.getMetaData().supportsConvert(), is(false));
    }

    @Test
    public void test_supportsConvertWithArgs() throws Exception {
        assertThat(conn.getMetaData().supportsConvert(1, 1), is(false));
    }

    @Test
    public void test_supportsCoreSQLGrammar() throws Exception {
        assertThat(conn.getMetaData().supportsCoreSQLGrammar(), is(false));
    }

    @Test
    public void test_supportsCorrelatedSubqueries() throws Exception {
        assertThat(conn.getMetaData().supportsCorrelatedSubqueries(), is(true));
    }

    @Test
    public void test_supportsDataDefinitionAndDataManipulationTransactions() throws Exception {
        assertThat(conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions(), is(true));
    }

    @Test
    public void test_supportsDataManipulationTransactionsOnly() throws Exception {
        assertThat(conn.getMetaData().supportsDataManipulationTransactionsOnly(), is(false));
    }

    @Test
    public void test_supportsDifferentTableCorrelationNames() throws Exception {
        assertThat(conn.getMetaData().supportsDifferentTableCorrelationNames(), is(false));
    }

    @Test
    public void test_supportsExpressionsInOrderBy() throws Exception {
        assertThat(conn.getMetaData().supportsExpressionsInOrderBy(), is(true));
    }

    @Test
    public void test_supportsExtendedSQLGrammar() throws Exception {
        assertThat(conn.getMetaData().supportsExtendedSQLGrammar(), is(false));
    }

    @Test
    public void test_supportsFullOuterJoins() throws Exception {
        assertThat(conn.getMetaData().supportsFullOuterJoins(), is(true));
    }

    @Test
    public void test_supportsGetGeneratedKeys() throws Exception {
        assertThat(conn.getMetaData().supportsGetGeneratedKeys(), is(true));
    }

    @Test
    public void test_supportsGroupBy() throws Exception {
        assertThat(conn.getMetaData().supportsGroupBy(), is(true));
    }

    @Test
    public void test_supportsGroupByBeyondSelect() throws Exception {
        assertThat(conn.getMetaData().supportsGroupByBeyondSelect(), is(true));
    }

    @Test
    public void test_supportsGroupByUnrelated() throws Exception {
        assertThat(conn.getMetaData().supportsGroupByUnrelated(), is(true));
    }

    @Test
    public void test_supportsIntegrityEnhancementFacility() throws Exception {
        assertThat(conn.getMetaData().supportsIntegrityEnhancementFacility(), is(true));
    }

    @Test
    public void test_supportsLikeEscapeClause() throws Exception {
        assertThat(conn.getMetaData().supportsLikeEscapeClause(), is(true));
    }

    @Test
    public void test_supportsLimitedOuterJoins() throws Exception {
        assertThat(conn.getMetaData().supportsLimitedOuterJoins(), is(true));
    }

    @Test
    public void test_supportsMinimumSQLGrammar() throws Exception {
        assertThat(conn.getMetaData().supportsMinimumSQLGrammar(), is(true));
    }

    @Test
    public void test_supportsMixedCaseIdentifiers() throws Exception {
        assertThat(conn.getMetaData().supportsMixedCaseIdentifiers(), is(false));
    }

    @Test
    public void test_supportsMixedCaseQuotedIdentifiers() throws Exception {
        assertThat(conn.getMetaData().supportsMixedCaseQuotedIdentifiers(), is(true));
    }

    @Test
    public void test_supportsMultipleOpenResults() throws Exception {
        assertThat(conn.getMetaData().supportsMultipleOpenResults(), is(false));
    }

    @Test
    public void test_supportsMultipleResultSets() throws Exception {
        assertThat(conn.getMetaData().supportsMultipleResultSets(), is(true));
    }

    @Test
    public void test_supportsMultipleTransactions() throws Exception {
        assertThat(conn.getMetaData().supportsMultipleTransactions(), is(true));
    }

    @Test
    public void test_supportsNamedParameters() throws Exception {
        assertThat(conn.getMetaData().supportsNamedParameters(), is(false));
    }

    @Test
    public void test_supportsNonNullableColumns() throws Exception {
        assertThat(conn.getMetaData().supportsNonNullableColumns(), is(true));
    }

    @Test
    public void test_supportsOpenCursorsAcrossCommit() throws Exception {
        assertThat(conn.getMetaData().supportsOpenCursorsAcrossCommit(), is(false));
    }

    @Test
    public void test_supportsOpenCursorsAcrossRollback() throws Exception {
        assertThat(conn.getMetaData().supportsOpenCursorsAcrossRollback(), is(false));
    }

    @Test
    public void test_supportsOpenStatementsAcrossCommit() throws Exception {
        assertThat(conn.getMetaData().supportsOpenStatementsAcrossCommit(), is(true));
    }

    @Test
    public void test_supportsOpenStatementsAcrossRollback() throws Exception {
        assertThat(conn.getMetaData().supportsOpenStatementsAcrossRollback(), is(true));
    }

    @Test
    public void test_supportsOrderByUnrelated() throws Exception {
        assertThat(conn.getMetaData().supportsOrderByUnrelated(), is(true));
    }

    @Test
    public void test_supportsOuterJoins() throws Exception {
        assertThat(conn.getMetaData().supportsOuterJoins(), is(true));
    }

    @Test
    public void test_supportsPositionedDelete() throws Exception {
        assertThat(conn.getMetaData().supportsPositionedDelete(), is(false));
    }

    @Test
    public void test_supportsPositionedUpdate() throws Exception {
        assertThat(conn.getMetaData().supportsPositionedUpdate(), is(false));
    }

    @Test
    public void test_supportsRefCursors() throws Exception {
        assertThat(conn.getMetaData().supportsRefCursors(), is(true));
    }

    @Test
    public void test_supportsResultSetConcurrency() throws Exception {
        assertThat(
            conn.getMetaData().supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY),
            is(true)
        );
    }

    @Test
    public void test_supportsResultSetHoldability() throws Exception {
        assertThat(conn.getMetaData().supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT), is(true));
    }

    @Test
    public void test_supportsResultSetType() throws Exception {
        assertThat(conn.getMetaData().supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY), is(true));
    }

    @Test
    public void test_supportsSavepoints() throws Exception {
        assertThat(conn.getMetaData().supportsSavepoints(), is(true));
    }

    @Test
    public void test_supportsSchemasInDataManipulation() throws Exception {
        assertThat(conn.getMetaData().supportsSchemasInDataManipulation(), is(true));
    }

    @Test
    public void test_supportsSchemasInIndexDefinitions() throws Exception {
        assertThat(conn.getMetaData().supportsSchemasInIndexDefinitions(), is(true));
    }

    @Test
    public void test_supportsSchemasInPrivilegeDefinitions() throws Exception {
        assertThat(conn.getMetaData().supportsSchemasInPrivilegeDefinitions(), is(true));
    }

    @Test
    public void test_supportsSchemasInProcedureCalls() throws Exception {
        assertThat(conn.getMetaData().supportsSchemasInProcedureCalls(), is(true));
    }

    @Test
    public void test_supportsSchemasInTableDefinitions() throws Exception {
        assertThat(conn.getMetaData().supportsSchemasInTableDefinitions(), is(true));
    }

    @Test
    public void test_supportsSelectForUpdate() throws Exception {
        assertThat(conn.getMetaData().supportsSelectForUpdate(), is(true));
    }

    @Test
    public void test_supportsSharding() throws Exception {
        assertThat(conn.getMetaData().supportsSharding(), is(false));
    }

    @Test
    public void test_supportsStatementPooling() throws Exception {
        assertThat(conn.getMetaData().supportsStatementPooling(), is(false));
    }

    @Test
    public void test_supportsStoredFunctionsUsingCallSyntax() throws Exception {
        assertThat(conn.getMetaData().supportsStoredFunctionsUsingCallSyntax(), is(true));
    }

    @Test
    public void test_supportsStoredProcedures() throws Exception {
        assertThat(conn.getMetaData().supportsStoredProcedures(), is(true));
    }

    @Test
    public void test_supportsSubqueriesInComparisons() throws Exception {
        assertThat(conn.getMetaData().supportsSubqueriesInComparisons(), is(true));
    }

    @Test
    public void test_supportsSubqueriesInExists() throws Exception {
        assertThat(conn.getMetaData().supportsSubqueriesInExists(), is(true));
    }

    @Test
    public void test_supportsSubqueriesInIns() throws Exception {
        assertThat(conn.getMetaData().supportsSubqueriesInIns(), is(true));
    }

    @Test
    public void test_supportsSubqueriesInQuantifieds() throws Exception {
        assertThat(conn.getMetaData().supportsSubqueriesInQuantifieds(), is(true));
    }

    @Test
    public void test_supportsTableCorrelationNames() throws Exception {
        assertThat(conn.getMetaData().supportsTableCorrelationNames(), is(true));
    }

    @Test
    public void test_supportsTransactionIsolationLevel() throws Exception {
        assertThat(conn.getMetaData().supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED), is(true));
    }

    @Test
    public void test_supportsTransactions() throws Exception {
        assertThat(conn.getMetaData().supportsTransactions(), is(true));
    }

    @Test
    public void test_supportsUnion() throws Exception {
        assertThat(conn.getMetaData().supportsUnion(), is(true));
    }

    @Test
    public void test_supportsUnionAll() throws Exception {
        assertThat(conn.getMetaData().supportsUnionAll(), is(true));
    }

    @Test
    public void test_updatesAreDetected() throws Exception {
        assertThat(conn.getMetaData().updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY), is(false));
    }

    @Test
    public void test_usesLocalFilePerTable() throws Exception {
        assertThat(conn.getMetaData().usesLocalFilePerTable(), is(false));
    }

    @Test
    public void test_usesLocalFiles() throws Exception {
        assertThat(conn.getMetaData().usesLocalFiles(), is(false));
    }
}
