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
package io.crate.client.jdbc;

import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

/**
 * A description of CrateDB, where pgJDBC's describes PostgreSQL: the product
 * it names, the SQL CrateDB has no grammar for, the limits it does not put on
 * identifiers, and the single catalog its objects live in.
 *
 * <p>Metadata rows arrive as {@link CrateResultSet}s, so navigating from a row
 * to its statement and connection stays inside this driver.</p>
 */
@SuppressWarnings("deprecation")
public class CrateDatabaseMetaData implements DatabaseMetaData {

    protected final DatabaseMetaData delegate;

    /**
     * The oldest CrateDB whose {@code pg_catalog} carries what pgJDBC's
     * metadata queries read, {@code current_catalog} among it, which arrived in
     * the 6.x line.
     */
    private static final int MINIMUM_MAJOR = 6;
    private static final int MINIMUM_MINOR = 0;
    private static final String MINIMUM_SERVER = MINIMUM_MAJOR + "." + MINIMUM_MINOR;

    private final CrateConnection connection;

    CrateDatabaseMetaData(DatabaseMetaData delegate, CrateConnection connection) {
        this.delegate = delegate;
        this.connection = connection;
    }

    @Adapted
    @Override
    public String getDatabaseProductName() {
        return "Crate";
    }

    @Adapted
    @Override
    public String getDriverName() {
        return "CrateDB JDBC Driver";
    }

    @Adapted
    @Override
    public String getDriverVersion() {
        return CrateDriverVersion.CURRENT.toString();
    }

    @Adapted
    @Override
    public int getDriverMajorVersion() {
        return CrateDriverVersion.CURRENT.major;
    }

    @Adapted
    @Override
    public int getDriverMinorVersion() {
        return CrateDriverVersion.CURRENT.minor;
    }

    @Adapted
    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    /**
     * CrateDB has no transactions: {@code BEGIN} and {@code COMMIT} are
     * accepted and do nothing, and there is no {@code ROLLBACK}. A framework
     * that asks before relying on transactional bookkeeping learns it here,
     * instead of from a rollback that quietly kept every write.
     */
    @Adapted
    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Adapted
    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Adapted
    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Adapted
    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_NONE;
    }

    @Adapted
    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Adapted
    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    /** CrateDB has no foreign keys, and so no referential integrity to enforce. */
    @Adapted
    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    /** {@code FOR UPDATE} is not part of CrateDB's SQL grammar. */
    @Adapted
    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    /** CrateDB has no {@code refcursor} type for a function to hand back. */
    @Adapted
    @Override
    public boolean supportsRefCursors() {
        return false;
    }

    /**
     * {@code PROCEDURE} is not part of CrateDB's SQL grammar at all, so
     * {@link #getProcedures} lists none. A tool offering to browse or call them
     * learns it here, instead of from an empty list it would read as "none
     * defined yet".
     *
     * <p>{@code supportsStoredFunctionsUsingCallSyntax} stays as pgJDBC answers
     * it. A {@code {call f(?)}} escape is honored, pgJDBC rewriting it into a
     * {@code SELECT}.</p>
     */
    @Adapted
    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Adapted
    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    /**
     * The mapping's field limit, where PostgreSQL counts the columns it fits in
     * a page. It is a table setting, so this reports the default a tool sizing
     * a generated table needs.
     */
    @Adapted
    @Override
    public int getMaxColumnsInTable() {
        return 1000;
    }

    /**
     * CrateDB puts no length limit on identifiers, where PostgreSQL cuts them
     * off at 63 characters. Zero is how JDBC spells "no limit", and it keeps a
     * tool from shortening a name the server would have taken.
     */
    @Adapted
    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Adapted
    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Adapted
    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Adapted
    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Adapted
    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Adapted
    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Adapted
    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Adapted
    @Override
    public CrateConnection getConnection() throws SQLException {
        if (connection.isClosed()) {
            throw new PSQLException("This connection has been closed.",
                PSQLState.CONNECTION_DOES_NOT_EXIST);
        }
        return connection;
    }

    /**
     * The URL the connection was opened with, in this driver's scheme instead
     * of the {@code jdbc:postgresql://} form it rewrites one to.
     */
    @Adapted
    @Override
    public String getURL() throws SQLException {
        String url = delegate.getURL();
        return url != null && url.startsWith(CrateDriver.PSQL_PREFIX_LONG)
            ? CrateDriver.CRATE_PREFIX_LONG + url.substring(CrateDriver.PSQL_PREFIX_LONG.length())
            : url;
    }

    /**
     * A catalog argument as pgJDBC's queries want it. JDBC spells "ignore the
     * catalog" as {@code null}, while pgJDBC reads the empty string as "objects
     * belonging to no catalog", which every CrateDB object fails. A caller
     * spelling it the other way would get no rows at all.
     */
    private static String catalogOrNull(String catalog) {
        return "".equals(catalog) ? null : catalog;
    }

    @FunctionalInterface
    private interface MetaDataQuery {
        ResultSet run() throws SQLException;
    }

    private ResultSet metadata(MetaDataQuery query) throws SQLException {
        try {
            return wrap(query.run());
        } catch (SQLException e) {
            throw metadataFailure(e);
        }
    }

    /**
     * A failed metadata query, told in CrateDB's terms when the server is
     * older than the one pgJDBC's catalog queries need. Such a server refuses
     * them by naming a catalog column the caller never asked about. The version
     * is the part a caller can act on.
     */
    private SQLException metadataFailure(SQLException cause) {
        CrateVersion version;
        try {
            version = connection.getCrateVersion();
        } catch (SQLException | RuntimeException unreadable) {
            // Whatever went wrong reading the version, the failure being
            // reported is the one the caller asked about.
            cause.addSuppressed(unreadable);
            return cause;
        }
        if (version.atLeast(MINIMUM_MAJOR, MINIMUM_MINOR)) {
            return cause;
        }
        return new SQLFeatureNotSupportedException(
            "This metadata call needs CrateDB " + MINIMUM_SERVER + " or later; the server is "
            + version + ".", PSQLState.NOT_IMPLEMENTED.getState(), cause);
    }

    /**
     * Metadata rows as this driver's result sets, so that navigating from a
     * row back to its statement and connection stays inside the driver instead
     * of reaching the pgJDBC connection underneath.
     *
     * <p>The rows are taken from the wrapping statement instead of built beside
     * it, so a caller navigating from the rows to the statement and back
     * arrives at the rows it started from.</p>
     */
    private ResultSet wrap(ResultSet resultSet) throws SQLException {
        Statement statement = resultSet.getStatement();
        if (statement == null) {
            return new CrateResultSet(resultSet, null);
        }
        return new CrateStatement(statement, connection).resultSet(resultSet);
    }

    @Adapted
    @Override
    public ResultSet getSchemas() throws SQLException {
        return metadata(() -> delegate.getSchemas());
    }

    @Adapted
    @Override
    public ResultSet getCatalogs() throws SQLException {
        return metadata(() -> delegate.getCatalogs());
    }

    @Adapted
    @Override
    public ResultSet getTableTypes() throws SQLException {
        return metadata(() -> delegate.getTableTypes());
    }

    @Adapted
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return metadata(() -> delegate.getTypeInfo());
    }

    @Adapted
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return metadata(() -> delegate.getClientInfoProperties());
    }

    @Adapted
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return metadata(() -> delegate.getTables(catalogOrNull(catalog), schemaPattern, tableNamePattern, types));
    }

    @Adapted
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getColumns(catalogOrNull(catalog), schemaPattern, tableNamePattern, columnNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return metadata(() -> delegate.getSchemas(catalogOrNull(catalog), schemaPattern));
    }

    @Adapted
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return metadata(() -> delegate.getPrimaryKeys(catalogOrNull(catalog), schema, table));
    }

    @Adapted
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return metadata(() -> delegate.getImportedKeys(catalogOrNull(catalog), schema, table));
    }

    @Adapted
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return metadata(() -> delegate.getExportedKeys(catalogOrNull(catalog), schema, table));
    }

    @Adapted
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return metadata(() -> delegate.getCrossReference(catalogOrNull(parentCatalog), parentSchema, parentTable,
                catalogOrNull(foreignCatalog), foreignSchema, foreignTable));
    }

    /**
     * A catalog no CrateDB carries, which is how the empty answers below are
     * obtained. pgJDBC builds the result set before deciding there is nothing
     * to fill it with, so asking about a catalog that cannot match yields the
     * columns without the query.
     */
    private static final String NO_SUCH_CATALOG = "\u0000";

    /**
     * No indexes are described. pgJDBC reads them through
     * {@code pg_get_indexdef} and {@code information_schema._pg_expandarray},
     * and CrateDB's partial {@code pg_catalog} provides neither, so the query
     * behind this cannot run. Supplying the two functions would still not make
     * it answer: {@code pg_am} and {@code pg_indexes} are present but empty,
     * which a query cannot tell from a schema holding no indexes, and
     * {@code pg_index} describes a primary key's own index as
     * {@code indisprimary} true, {@code indisunique} false and
     * {@code indnatts} zero, a row no correct answer reads out of.
     *
     * <p>The answer is no rows instead of a failure because every tool that
     * introspects a schema asks this of every table, and raising here would
     * stop the introspection rather than the part of it wanting indexes.
     * {@link #getImportedKeys} and {@link #getExportedKeys} do the same for a
     * feature CrateDB equally lacks. The columns a row is addressed by are
     * readable through {@link #getPrimaryKeys}.</p>
     */
    @Adapted
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return metadata(() -> delegate.getIndexInfo(NO_SUCH_CATALOG, schema, table, unique, approximate));
    }

    @Adapted
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return metadata(() -> delegate.getProcedures(catalogOrNull(catalog), schemaPattern, procedureNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getProcedureColumns(catalogOrNull(catalog), schemaPattern, procedureNamePattern, columnNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return metadata(() -> delegate.getFunctions(catalogOrNull(catalog), schemaPattern, functionNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getFunctionColumns(catalogOrNull(catalog), schemaPattern, functionNamePattern, columnNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return metadata(() -> delegate.getBestRowIdentifier(catalogOrNull(catalog), schema, table, scope, nullable));
    }

    @Adapted
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return metadata(() -> delegate.getVersionColumns(catalogOrNull(catalog), schema, table));
    }

    @Adapted
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return metadata(() -> delegate.getTablePrivileges(catalogOrNull(catalog), schemaPattern, tableNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getColumnPrivileges(catalogOrNull(catalog), schema, table, columnNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return metadata(() -> delegate.getUDTs(catalogOrNull(catalog), schemaPattern, typeNamePattern, types));
    }

    @Adapted
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return metadata(() -> delegate.getSuperTypes(catalogOrNull(catalog), schemaPattern, typeNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return metadata(() -> delegate.getSuperTables(catalogOrNull(catalog), schemaPattern, tableNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return metadata(() -> delegate.getAttributes(catalogOrNull(catalog), schemaPattern, typeNamePattern, attributeNamePattern));
    }

    @Adapted
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getPseudoColumns(catalogOrNull(catalog), schemaPattern, tableNamePattern, columnNamePattern));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.isInstance(this) ? iface.cast(this) : delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }

    // <editor-fold defaultstate="collapsed" desc="Delegation to pgJDBC (124 methods)">

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return delegate.allTablesAreSelectable();
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return delegate.autoCommitFailureClosesAllResultSets();
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return delegate.dataDefinitionCausesTransactionCommit();
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return delegate.dataDefinitionIgnoredInTransactions();
    }

    @Override
    public boolean deletesAreDetected(int p0) throws SQLException {
        return delegate.deletesAreDetected(p0);
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return delegate.doesMaxRowSizeIncludeBlobs();
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return delegate.generatedKeyAlwaysReturned();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return delegate.getCatalogSeparator();
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return delegate.getCatalogTerm();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return delegate.getDatabaseMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return delegate.getDatabaseMinorVersion();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return delegate.getDatabaseProductVersion();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return delegate.getExtraNameCharacters();
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return delegate.getIdentifierQuoteString();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return delegate.getJDBCMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return delegate.getJDBCMinorVersion();
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return delegate.getMaxBinaryLiteralLength();
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return delegate.getMaxCharLiteralLength();
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return delegate.getMaxColumnsInGroupBy();
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return delegate.getMaxColumnsInIndex();
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return delegate.getMaxColumnsInOrderBy();
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return delegate.getMaxColumnsInSelect();
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return delegate.getMaxConnections();
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return delegate.getMaxIndexLength();
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return delegate.getMaxLogicalLobSize();
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return delegate.getMaxRowSize();
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return delegate.getMaxStatementLength();
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return delegate.getMaxStatements();
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return delegate.getMaxTablesInSelect();
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return delegate.getNumericFunctions();
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return delegate.getProcedureTerm();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return delegate.getResultSetHoldability();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return delegate.getRowIdLifetime();
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return delegate.getSQLKeywords();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return delegate.getSQLStateType();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return delegate.getSchemaTerm();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return delegate.getSearchStringEscape();
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return delegate.getStringFunctions();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return delegate.getSystemFunctions();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return delegate.getTimeDateFunctions();
    }

    @Override
    public String getUserName() throws SQLException {
        return delegate.getUserName();
    }

    @Override
    public boolean insertsAreDetected(int p0) throws SQLException {
        return delegate.insertsAreDetected(p0);
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return delegate.isCatalogAtStart();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return delegate.isReadOnly();
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return delegate.locatorsUpdateCopy();
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return delegate.nullPlusNonNullIsNull();
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return delegate.nullsAreSortedAtEnd();
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return delegate.nullsAreSortedAtStart();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return delegate.nullsAreSortedHigh();
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return delegate.nullsAreSortedLow();
    }

    @Override
    public boolean othersDeletesAreVisible(int p0) throws SQLException {
        return delegate.othersDeletesAreVisible(p0);
    }

    @Override
    public boolean othersInsertsAreVisible(int p0) throws SQLException {
        return delegate.othersInsertsAreVisible(p0);
    }

    @Override
    public boolean othersUpdatesAreVisible(int p0) throws SQLException {
        return delegate.othersUpdatesAreVisible(p0);
    }

    @Override
    public boolean ownDeletesAreVisible(int p0) throws SQLException {
        return delegate.ownDeletesAreVisible(p0);
    }

    @Override
    public boolean ownInsertsAreVisible(int p0) throws SQLException {
        return delegate.ownInsertsAreVisible(p0);
    }

    @Override
    public boolean ownUpdatesAreVisible(int p0) throws SQLException {
        return delegate.ownUpdatesAreVisible(p0);
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return delegate.storesLowerCaseIdentifiers();
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return delegate.storesLowerCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return delegate.storesMixedCaseIdentifiers();
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return delegate.storesMixedCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return delegate.storesUpperCaseIdentifiers();
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return delegate.storesUpperCaseQuotedIdentifiers();
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return delegate.supportsANSI92EntryLevelSQL();
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return delegate.supportsANSI92FullSQL();
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return delegate.supportsANSI92IntermediateSQL();
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return delegate.supportsAlterTableWithAddColumn();
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return delegate.supportsAlterTableWithDropColumn();
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return delegate.supportsBatchUpdates();
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return delegate.supportsCatalogsInDataManipulation();
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return delegate.supportsCatalogsInIndexDefinitions();
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return delegate.supportsCatalogsInPrivilegeDefinitions();
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return delegate.supportsCatalogsInProcedureCalls();
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return delegate.supportsCatalogsInTableDefinitions();
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return delegate.supportsColumnAliasing();
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return delegate.supportsConvert();
    }

    @Override
    public boolean supportsConvert(int p0, int p1) throws SQLException {
        return delegate.supportsConvert(p0, p1);
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return delegate.supportsCoreSQLGrammar();
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return delegate.supportsCorrelatedSubqueries();
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return delegate.supportsDifferentTableCorrelationNames();
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return delegate.supportsExpressionsInOrderBy();
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return delegate.supportsExtendedSQLGrammar();
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return delegate.supportsFullOuterJoins();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return delegate.supportsGetGeneratedKeys();
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return delegate.supportsGroupBy();
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return delegate.supportsGroupByBeyondSelect();
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return delegate.supportsGroupByUnrelated();
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return delegate.supportsLikeEscapeClause();
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return delegate.supportsLimitedOuterJoins();
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return delegate.supportsMinimumSQLGrammar();
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return delegate.supportsMixedCaseIdentifiers();
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return delegate.supportsMixedCaseQuotedIdentifiers();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return delegate.supportsMultipleOpenResults();
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return delegate.supportsMultipleResultSets();
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return delegate.supportsNamedParameters();
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return delegate.supportsNonNullableColumns();
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return delegate.supportsOpenCursorsAcrossCommit();
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return delegate.supportsOpenCursorsAcrossRollback();
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return delegate.supportsOpenStatementsAcrossCommit();
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return delegate.supportsOpenStatementsAcrossRollback();
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return delegate.supportsOrderByUnrelated();
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return delegate.supportsOuterJoins();
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return delegate.supportsPositionedDelete();
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return delegate.supportsPositionedUpdate();
    }

    @Override
    public boolean supportsResultSetConcurrency(int p0, int p1) throws SQLException {
        return delegate.supportsResultSetConcurrency(p0, p1);
    }

    @Override
    public boolean supportsResultSetHoldability(int p0) throws SQLException {
        return delegate.supportsResultSetHoldability(p0);
    }

    @Override
    public boolean supportsResultSetType(int p0) throws SQLException {
        return delegate.supportsResultSetType(p0);
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return delegate.supportsSchemasInDataManipulation();
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return delegate.supportsSchemasInIndexDefinitions();
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return delegate.supportsSchemasInPrivilegeDefinitions();
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return delegate.supportsSchemasInProcedureCalls();
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return delegate.supportsSchemasInTableDefinitions();
    }

    @Override
    public boolean supportsSharding() throws SQLException {
        return delegate.supportsSharding();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return delegate.supportsStatementPooling();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return delegate.supportsStoredFunctionsUsingCallSyntax();
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return delegate.supportsSubqueriesInComparisons();
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return delegate.supportsSubqueriesInExists();
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return delegate.supportsSubqueriesInIns();
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return delegate.supportsSubqueriesInQuantifieds();
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return delegate.supportsTableCorrelationNames();
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return delegate.supportsUnion();
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return delegate.supportsUnionAll();
    }

    @Override
    public boolean updatesAreDetected(int p0) throws SQLException {
        return delegate.updatesAreDetected(p0);
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return delegate.usesLocalFilePerTable();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return delegate.usesLocalFiles();
    }
    // </editor-fold>
}
