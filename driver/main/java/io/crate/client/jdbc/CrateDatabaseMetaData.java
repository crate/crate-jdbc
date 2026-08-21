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
public class CrateDatabaseMetaData extends ForwardingDatabaseMetaData {

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
        super(delegate);
        this.connection = connection;
    }

    @Override
    public String getDatabaseProductName() {
        return "Crate";
    }

    @Override
    public String getDriverName() {
        return "CrateDB JDBC Driver";
    }

    @Override
    public String getDriverVersion() {
        return CrateDriverVersion.CURRENT.toString();
    }

    @Override
    public int getDriverMajorVersion() {
        return CrateDriverVersion.CURRENT.major;
    }

    @Override
    public int getDriverMinorVersion() {
        return CrateDriverVersion.CURRENT.minor;
    }

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
    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    /** CrateDB has no foreign keys, and so no referential integrity to enforce. */
    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    /** {@code FOR UPDATE} is not part of CrateDB's SQL grammar. */
    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    /** CrateDB has no {@code refcursor} type for a function to hand back. */
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
    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    /**
     * The mapping's field limit, where PostgreSQL counts the columns it fits in
     * a page. It is a table setting, so this reports the default a tool sizing
     * a generated table needs.
     */
    @Override
    public int getMaxColumnsInTable() {
        return 1000;
    }

    /**
     * CrateDB puts no length limit on identifiers, where PostgreSQL cuts them
     * off at 63 characters. Zero is how JDBC spells "no limit", and it keeps a
     * tool from shortening a name the server would have taken.
     */
    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

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

    @Override
    public ResultSet getSchemas() throws SQLException {
        return metadata(() -> delegate.getSchemas());
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return metadata(() -> delegate.getCatalogs());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return metadata(() -> delegate.getTableTypes());
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return metadata(() -> delegate.getTypeInfo());
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return metadata(() -> delegate.getClientInfoProperties());
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return metadata(() -> delegate.getTables(catalogOrNull(catalog), schemaPattern, tableNamePattern, types));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getColumns(catalogOrNull(catalog), schemaPattern, tableNamePattern, columnNamePattern));
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return metadata(() -> delegate.getSchemas(catalogOrNull(catalog), schemaPattern));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return metadata(() -> delegate.getPrimaryKeys(catalogOrNull(catalog), schema, table));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return metadata(() -> delegate.getImportedKeys(catalogOrNull(catalog), schema, table));
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return metadata(() -> delegate.getExportedKeys(catalogOrNull(catalog), schema, table));
    }

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
    private static final String NO_SUCH_CATALOG = " ";

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
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return metadata(() -> delegate.getIndexInfo(NO_SUCH_CATALOG, schema, table, unique, approximate));
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return metadata(() -> delegate.getProcedures(catalogOrNull(catalog), schemaPattern, procedureNamePattern));
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getProcedureColumns(catalogOrNull(catalog), schemaPattern, procedureNamePattern, columnNamePattern));
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return metadata(() -> delegate.getFunctions(catalogOrNull(catalog), schemaPattern, functionNamePattern));
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getFunctionColumns(catalogOrNull(catalog), schemaPattern, functionNamePattern, columnNamePattern));
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return metadata(() -> delegate.getBestRowIdentifier(catalogOrNull(catalog), schema, table, scope, nullable));
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return metadata(() -> delegate.getVersionColumns(catalogOrNull(catalog), schema, table));
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return metadata(() -> delegate.getTablePrivileges(catalogOrNull(catalog), schemaPattern, tableNamePattern));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getColumnPrivileges(catalogOrNull(catalog), schema, table, columnNamePattern));
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return metadata(() -> delegate.getUDTs(catalogOrNull(catalog), schemaPattern, typeNamePattern, types));
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return metadata(() -> delegate.getSuperTypes(catalogOrNull(catalog), schemaPattern, typeNamePattern));
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return metadata(() -> delegate.getSuperTables(catalogOrNull(catalog), schemaPattern, tableNamePattern));
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return metadata(() -> delegate.getAttributes(catalogOrNull(catalog), schemaPattern, typeNamePattern, attributeNamePattern));
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return metadata(() -> delegate.getPseudoColumns(catalogOrNull(catalog), schemaPattern, tableNamePattern, columnNamePattern));
    }
}
