package io.crate.client.jdbc;

import io.crate.action.sql.SQLResponse;
import io.crate.client.CrateClient;
import org.elasticsearch.common.base.Joiner;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CrateDatabaseMetaData implements DatabaseMetaData {

    private final CrateConnection connection;

    public CrateDatabaseMetaData(CrateConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return true;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Crate-Data";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "0.36.0";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "Crate";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return CrateClient.VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return new Integer(CrateClient.VERSION.split(".")[1]);
    }

    @Override
    public int getDriverMinorVersion() {
        return new Integer(CrateClient.VERSION.split(".")[2]);
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "partitioned, dynamic, strict, clustered, shards, fulltext, plain, analyzer," +
                "extends, token_filters, char_filters";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "format";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "date_trunc";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema_name";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        String stmt = "select schema_name, table_name from information_schema.tables";
        List<String> whereConditions = new ArrayList<String>();
        if (schemaPattern != null && schemaPattern.equals("")) {
            whereConditions.add("schema_name is null");
        } else if (schemaPattern != null) {
            whereConditions.add("schema_name like '" + schemaPattern + "'");
        }
        if (tableNamePattern != null) {
            whereConditions.add("table_name like '" + tableNamePattern + "'");
        }
        if (whereConditions.size() > 0) {
            stmt = stmt + " where " + Joiner.on(" and ").join(whereConditions);
        }
        stmt = stmt + " order by schema_name, table_name";
        SQLResponse sqlResponse = connection.client().sql(stmt).actionGet();
        String[] cols = new String[10];
        cols[0] = "TABLE_CAT";
        cols[1] = "TABLE_SCHEM";
        cols[2] = "TABLE_NAME";
        cols[3] = "TABLE_TYPE";
        cols[4] = "REMARKS";
        cols[5] = "TYPE_CAT";
        cols[6] = "TYPE_SCHEM";
        cols[7] = "TYPE_NAME";
        cols[8] = "SELF_REFERENCING_COL_NAME";
        cols[9] = "REF_GENERATION";
        Object[][] rows = new Object[sqlResponse.rows().length][10];
        for (int i = 0; i < sqlResponse.rows().length; i++) {
            rows[i][0] = null;
            rows[i][1] = sqlResponse.rows()[i][0];
            rows[i][2] = sqlResponse.rows()[i][1];
            if (rows[i][1].equals("sys") || rows[i][1].equals("information_schema")) {
                rows[i][3] = "SYSTEM TABLE";
            } else {
                rows[i][3] = "TABLE";
            }
            rows[i][4] = "";
            rows[i][5] = null;
            rows[i][6] = null;
            rows[i][7] = null;
            rows[i][8] = "_id";
            rows[i][9] = "SYSTEM";
        }
        SQLResponse tableResponse = new SQLResponse(cols, rows, sqlResponse.rowCount(), 0L);
        return new CrateResultSet(connection.createStatement(), tableResponse);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        String stmt = "select schema_name from information_schema.tables group by schema_name order by schema_name";
        SQLResponse sqlResponse = connection.client().sql(stmt).actionGet();
        String[] cols = new String[2];
        cols[0] = "TABLE_SCHEM";
        cols[1] = "TABLE_CATALOG";
        Object[][] rows = new Object[sqlResponse.rows().length][2];
        for (int i = 0; i < sqlResponse.rows().length; i++) {
            rows[i][0] = sqlResponse.rows()[i][0];
            rows[i][1] = null;
        }
        SQLResponse tableResponse = new SQLResponse(cols, rows, sqlResponse.rowCount(), 0L);
        return new CrateResultSet(connection.createStatement(), tableResponse);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        String[] cols = new String[]{"TABLE_TYPE"};
        Object[][] rows = new Object[2][1];
        rows[0][0] = "SYSTEM TABLE";
        rows[1][0] = "TABLE";
        SQLResponse tableResponse = new SQLResponse(cols, rows, 2L, 0L);
        return new CrateResultSet(connection.createStatement(), tableResponse);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String stmt = "select schema_name, table_name, column_name, data_type, ordinal_position " +
                "from information_schema.columns";
        List<String> whereConditions = new ArrayList<String>();
        if (schemaPattern != null && schemaPattern.equals("")) {
            whereConditions.add("schema_name is null");
        } else if (schemaPattern != null) {
            whereConditions.add("schema_name like '" + schemaPattern + "'");
        }
        if (tableNamePattern != null) {
            whereConditions.add("table_name like '" + tableNamePattern + "'");
        }
        if (columnNamePattern != null) {
            whereConditions.add("column_name like '" + columnNamePattern + "'");
        }
        if (whereConditions.size() > 0) {
            stmt = stmt + " where " + Joiner.on(" and ").join(whereConditions);
        }
        stmt = stmt + " order by schema_name, table_name, ordinal_position";
        SQLResponse sqlResponse = connection.client().sql(stmt).actionGet();
        String[] cols = new String[24];
        cols[0] = "TABLE_CAT";
        cols[1] = "TABLE_SCHEM";
        cols[2] = "TABLE_NAME";
        cols[3] = "COLUMN_NAME";
        cols[4] = "DATA_TYPE";
        cols[5] = "TYPE_NAME";
        cols[6] = "COLUMN_SIZE";
        cols[7] = "BUFFER_LENGTH";
        cols[8] = "DECIMAL_DIGITS";
        cols[9] = "NUM_PREC_RADIX";
        cols[10] = "NULLABLE";
        cols[11] = "REMARKS";
        cols[12] = "COLUMN_DEF";
        cols[13] = "SQL_DATA_TYPE";
        cols[14] = "SQL_DATETIME_SUB";
        cols[15] = "CHAR_OCTET_LENGTH";
        cols[16] = "ORDINAL_POSITION";
        cols[17] = "IS_NULLABLE";
        cols[18] = "SCOPE_CATALOG";
        cols[19] = "SCOPE_SCHEMA";
        cols[20] = "SCOPE_TABLE";
        cols[21] = "SOURCE_DATA_TYPE";
        cols[22] = "IS_AUTOINCREMENT";
        cols[23] = "IS_GENERATEDCOLUMN";
        Object[][] rows = new Object[sqlResponse.rows().length][24];
        for (int i = 0; i < sqlResponse.rows().length; i++) {
            rows[i][0] = null;
            rows[i][1] = sqlResponse.rows()[i][0];
            rows[i][2] = sqlResponse.rows()[i][1];
            rows[i][3] = sqlResponse.rows()[i][2];
            rows[i][4] = sqlTypeOfCrateType((String)sqlResponse.rows()[i][3]);
            rows[i][5] = sqlResponse.rows()[i][3];
            rows[i][6] = null;
            rows[i][7] = null;
            rows[i][8] = null;
            rows[i][9] = 10;
            rows[i][10] = columnNullable;
            rows[i][11] = null;
            rows[i][12] = null;
            rows[i][13] = null;
            rows[i][14] = null;
            rows[i][15] = null;
            rows[i][16] = ((Number)sqlResponse.rows()[i][4]).intValue();
            rows[i][17] = "YES";
            rows[i][18] = null;
            rows[i][19] = null;
            rows[i][20] = null;
            rows[i][21] = null;
            rows[i][22] = "NO";
            rows[i][23] = "NO";
        }
        SQLResponse tableResponse = new SQLResponse(cols, rows, sqlResponse.rowCount(), 0L);
        return new CrateResultSet(connection.createStatement(), tableResponse);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        String stmt = "select constraint_name from information_schema.table_constraints " +
                "where schema_name = '" + schema + "' and table_name = '" + table + "'";
        SQLResponse sqlResponse = connection.client().sql(stmt).actionGet();
        String[] cols = new String[6];
        cols[0] = "TABLE_CAT";
        cols[1] = "TABLE_SCHEM";
        cols[2] = "TABLE_NAME";
        cols[3] = "COLUMN_NAME";
        cols[4] = "KEY_SEQ";
        cols[5] = "PK_NAME";
        List<Object[]> rowList = new ArrayList<>();
        for (int i = 0; i < sqlResponse.rows().length; i++) {
            String[] pks = (String[])sqlResponse.rows()[i][0];
            for (int j = 0; j < pks.length; j++) {
                Object[] row = new Object[6];
                row[0] = null;
                row[1] = schema;
                row[2] = table;
                row[3] = pks[j];
                row[4] = j;
                row[5] = null;
                rowList.add(row);
            }
        }
        Object[][] rows = rowList.toArray(new Object[rowList.size()][6]);
        SQLResponse tableResponse = new SQLResponse(cols, rows, sqlResponse.rowCount(), 0L);
        return new CrateResultSet(connection.createStatement(), tableResponse);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        String[] cols = new String[18];
        cols[0] = "TYPE_NAME";
        cols[1] = "DATA_TYPE";
        cols[2] = "PRECISION";
        cols[3] = "LITERAL_PREFIX";
        cols[4] = "LITERAL_SUFFIX";
        cols[5] = "CREATE_PARAMS";
        cols[6] = "NULLABLE";
        cols[7] = "CASE_SENSITIVE";
        cols[8] = "SEARCHABLE";
        cols[9] = "UNSIGNED_ATTRIBUTE";
        cols[10] = "FIXED_PREC_SCALE";
        cols[11] = "AUTO_INCREMENT";
        cols[12] = "LOCAL_TYPE_NAME";
        cols[13] = "MINIMUM_SCALE";
        cols[14] = "MAXIMUM_SCALE";
        cols[15] = "SQL_DATA_TYPE";
        cols[16] = "SQL_DATETIME_SUB";
        cols[17] = "NUM_PREC_RADIX";

        Object[][] rows = new Object[21][18];

        rows[0][0] = "byte";
        rows[0][1] = Types.TINYINT;
        rows[0][2] = 3;
        rows[0][3] = null;
        rows[0][4] = null;
        rows[0][5] = null;
        rows[0][6] = typeNullable;
        rows[0][7] = false;
        rows[0][8] = typePredBasic;
        rows[0][9] = true;
        rows[0][10] = false;
        rows[0][11] = false;
        rows[0][12] = rows[0][0];
        rows[0][13] = 0;
        rows[0][14] = 0;
        rows[0][15] = null;
        rows[0][16] = null;
        rows[0][17] = 10;

        rows[1][0] = "long";
        rows[1][1] = Types.BIGINT;
        rows[1][2] = 19;
        rows[1][3] = null;
        rows[1][4] = null;
        rows[1][5] = null;
        rows[1][6] = typeNullable;
        rows[1][7] = false;
        rows[1][8] = typePredBasic;
        rows[1][9] = true;
        rows[1][10] = false;
        rows[1][11] = false;
        rows[1][12] = rows[0][0];
        rows[1][13] = 0;
        rows[1][14] = 0;
        rows[1][15] = null;
        rows[1][16] = null;
        rows[1][17] = 10;

        rows[2][0] = "integer";
        rows[2][1] = Types.INTEGER;
        rows[2][2] = 10;
        rows[2][3] = null;
        rows[2][4] = null;
        rows[2][5] = null;
        rows[2][6] = typeNullable;
        rows[2][7] = false;
        rows[2][8] = typePredBasic;
        rows[2][9] = true;
        rows[2][10] = false;
        rows[2][11] = false;
        rows[2][12] = rows[0][0];
        rows[2][13] = 0;
        rows[2][14] = 0;
        rows[2][15] = null;
        rows[2][16] = null;
        rows[2][17] = 10;

        rows[3][0] = "short";
        rows[3][1] = Types.SMALLINT;
        rows[3][2] = 5;
        rows[3][3] = null;
        rows[3][4] = null;
        rows[3][5] = null;
        rows[3][6] = typeNullable;
        rows[3][7] = false;
        rows[3][8] = typePredBasic;
        rows[3][9] = true;
        rows[3][10] = false;
        rows[3][11] = false;
        rows[3][12] = rows[0][0];
        rows[3][13] = 0;
        rows[3][14] = 0;
        rows[3][15] = null;
        rows[3][16] = null;
        rows[3][17] = 10;

        rows[4][0] = "float";
        rows[4][1] = Types.FLOAT;
        rows[4][2] = 7;
        rows[4][3] = null;
        rows[4][4] = null;
        rows[4][5] = null;
        rows[4][6] = typeNullable;
        rows[4][7] = false;
        rows[4][8] = typePredBasic;
        rows[4][9] = true;
        rows[4][10] = false;
        rows[4][11] = false;
        rows[4][12] = rows[0][0];
        rows[4][13] = 0;
        rows[4][14] = 6;
        rows[4][15] = null;
        rows[4][16] = null;
        rows[4][17] = 10;

        rows[5][0] = "double";
        rows[5][1] = Types.DOUBLE;
        rows[5][2] = 15;
        rows[5][3] = null;
        rows[5][4] = null;
        rows[5][5] = null;
        rows[5][6] = typeNullable;
        rows[5][7] = false;
        rows[5][8] = typePredBasic;
        rows[5][9] = true;
        rows[5][10] = false;
        rows[5][11] = false;
        rows[5][12] = rows[0][0];
        rows[5][13] = 0;
        rows[5][14] = 14;
        rows[5][15] = null;
        rows[5][16] = null;
        rows[5][17] = 10;

        rows[6][0] = "string";
        rows[6][1] = Types.VARCHAR;
        rows[6][2] = null;
        rows[6][3] = null;
        rows[6][4] = null;
        rows[6][5] = null;
        rows[6][6] = typeNullable;
        rows[6][7] = true;
        rows[6][8] = typeSearchable;
        rows[6][9] = true;
        rows[6][10] = false;
        rows[6][11] = false;
        rows[6][12] = rows[0][0];
        rows[6][13] = 0;
        rows[6][14] = 0;
        rows[6][15] = null;
        rows[6][16] = null;
        rows[6][17] = 10;

        rows[7][0] = "ip";
        rows[7][1] = Types.VARCHAR;
        rows[7][2] = 15;
        rows[7][3] = null;
        rows[7][4] = null;
        rows[7][5] = null;
        rows[7][6] = typeNullable;
        rows[7][7] = false;
        rows[7][8] = typeSearchable;
        rows[7][9] = true;
        rows[7][10] = false;
        rows[7][11] = false;
        rows[7][12] = rows[0][0];
        rows[7][13] = 0;
        rows[7][14] = 0;
        rows[7][15] = null;
        rows[7][16] = null;
        rows[7][17] = 10;

        rows[8][0] = "boolean";
        rows[8][1] = Types.BOOLEAN;
        rows[8][2] = null;
        rows[8][3] = null;
        rows[8][4] = null;
        rows[8][5] = null;
        rows[8][6] = typeNullable;
        rows[8][7] = false;
        rows[8][8] = typePredBasic;
        rows[8][9] = true;
        rows[8][10] = false;
        rows[8][11] = false;
        rows[8][12] = rows[0][0];
        rows[8][13] = 0;
        rows[8][14] = 0;
        rows[8][15] = null;
        rows[8][16] = null;
        rows[8][17] = 10;

        rows[9][0] = "timestamp";
        rows[9][1] = Types.TIMESTAMP;
        rows[9][2] = null;
        rows[9][3] = null;
        rows[9][4] = null;
        rows[9][5] = null;
        rows[9][6] = typeNullable;
        rows[9][7] = false;
        rows[9][8] = typePredBasic;
        rows[9][9] = true;
        rows[9][10] = false;
        rows[9][11] = false;
        rows[9][12] = rows[0][0];
        rows[9][13] = 0;
        rows[9][14] = 0;
        rows[9][15] = null;
        rows[9][16] = null;
        rows[9][17] = 10;

        rows[10][0] = "object";
        rows[10][1] = Types.STRUCT;
        rows[10][2] = null;
        rows[10][3] = null;
        rows[10][4] = null;
        rows[10][5] = null;
        rows[10][6] = typeNullable;
        rows[10][7] = false;
        rows[10][8] = typePredNone;
        rows[10][9] = true;
        rows[10][10] = false;
        rows[10][11] = false;
        rows[10][12] = rows[0][0];
        rows[10][13] = 0;
        rows[10][14] = 0;
        rows[10][15] = null;
        rows[10][16] = null;
        rows[10][17] = 10;

        String[] arrayTypes = new String[]{"string_array", "ip_array", "long_array",
                "integer_array", "short_array", "boolean_array", "byte_array",
                "float_array", "double_array", "object_array"};
        for (int i = 11; i < 11 + arrayTypes.length; i++) {
            rows[i][0] = arrayTypes[i-11];
            rows[i][1] = Types.ARRAY;
            rows[i][2] = null;
            rows[i][3] = null;
            rows[i][4] = null;
            rows[i][5] = null;
            rows[i][6] = typeNullable;
            rows[i][7] = false;
            rows[i][8] = typePredNone;
            rows[i][9] = true;
            rows[i][10] = false;
            rows[i][11] = false;
            rows[i][12] = rows[0][0];
            rows[i][13] = 0;
            rows[i][14] = 0;
            rows[i][15] = null;
            rows[i][16] = null;
            rows[i][17] = 10;
        }

        SQLResponse tableResponse = new SQLResponse(cols, rows, 21L, 0L);
        return new CrateResultSet(connection.createStatement(), tableResponse);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return (type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 36;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL99;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        String stmt = "select schema_name from information_schema.tables";
        if (schemaPattern != null) {
            stmt = stmt + " where schema_name like '" + schemaPattern + "'";
        }
        stmt = stmt  + " group by schema_name order by schema_name";
        SQLResponse sqlResponse = connection.client().sql(stmt).actionGet();
        String[] cols = new String[2];
        cols[0] = "TABLE_SCHEM";
        cols[1] = "TABLE_CATALOG";
        Object[][] rows = new Object[sqlResponse.rows().length][2];
        for (int i = 0; i < sqlResponse.rows().length; i++) {
            rows[i][0] = sqlResponse.rows()[i][0];
            rows[i][1] = null;
        }
        SQLResponse tableResponse = new SQLResponse(cols, rows, sqlResponse.rowCount(), 0);
        return new CrateResultSet(connection.createStatement(), tableResponse);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass()))
        {
            return (T) this;
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    public int sqlTypeOfCrateType(String dataType) {
        switch (dataType) {
            case "byte":
                return Types.TINYINT;
            case "long":
                return Types.BIGINT;
            case "integer":
                return Types.INTEGER;
            case "short":
                return Types.SMALLINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "string":
            case "ip":
                return Types.VARCHAR;
            case "boolean":
                return Types.BOOLEAN;
            case "timestamp":
                return Types.TIMESTAMP;
            case "object":
                return Types.STRUCT;
            case "string_array":
            case "ip_array":
            case "integer_array":
            case "long_array":
            case "short_array":
            case "byte_array":
            case "float_array":
            case "double_array":
            case "timestamp_array":
            case "object_array":
                return Types.ARRAY;
            default:
                return Types.OTHER;
        }
    }
}
