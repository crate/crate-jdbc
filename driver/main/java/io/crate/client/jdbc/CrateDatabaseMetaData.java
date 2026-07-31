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

package io.crate.client.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * CrateDB-aware metadata behavior on top of stock pgjdbc:
 *
 * <ul>
 * <li>{@link #getDatabaseProductName()} reports {@code Crate} so IDEs and
 *     tools that sniff the product name pick a CrateDB dialect instead of a
 *     PostgreSQL one.</li>
 * <li>An empty-string catalog argument is treated like {@code null}
 *     ("ignore catalog"). CrateDB has a single catalog named {@code crate};
 *     since pgjdbc 42.7.5 an empty string filters every result out, which
 *     breaks callers that follow the pre-42.7.5 convention.</li>
 * </ul>
 */
public class CrateDatabaseMetaData extends ForwardingDatabaseMetaData {

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
    public Connection getConnection() {
        return connection;
    }

    private static String catalog(String catalog) {
        return "".equals(catalog) ? null : catalog;
    }

    @Override
    public ResultSet getTables(String cat, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return delegate.getTables(catalog(cat), schemaPattern, tableNamePattern, types);
    }

    @Override
    public ResultSet getColumns(String cat, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return delegate.getColumns(catalog(cat), schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getSchemas(String cat, String schemaPattern) throws SQLException {
        return delegate.getSchemas(catalog(cat), schemaPattern);
    }

    @Override
    public ResultSet getPrimaryKeys(String cat, String schema, String table) throws SQLException {
        return delegate.getPrimaryKeys(catalog(cat), schema, table);
    }

    @Override
    public ResultSet getImportedKeys(String cat, String schema, String table) throws SQLException {
        return delegate.getImportedKeys(catalog(cat), schema, table);
    }

    @Override
    public ResultSet getExportedKeys(String cat, String schema, String table) throws SQLException {
        return delegate.getExportedKeys(catalog(cat), schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return delegate.getCrossReference(catalog(parentCatalog), parentSchema, parentTable,
                catalog(foreignCatalog), foreignSchema, foreignTable);
    }

    @Override
    public ResultSet getIndexInfo(String cat, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return delegate.getIndexInfo(catalog(cat), schema, table, unique, approximate);
    }

    @Override
    public ResultSet getProcedures(String cat, String schemaPattern, String procedureNamePattern) throws SQLException {
        return delegate.getProcedures(catalog(cat), schemaPattern, procedureNamePattern);
    }

    @Override
    public ResultSet getProcedureColumns(String cat, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return delegate.getProcedureColumns(catalog(cat), schemaPattern, procedureNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getFunctions(String cat, String schemaPattern, String functionNamePattern) throws SQLException {
        return delegate.getFunctions(catalog(cat), schemaPattern, functionNamePattern);
    }

    @Override
    public ResultSet getFunctionColumns(String cat, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return delegate.getFunctionColumns(catalog(cat), schemaPattern, functionNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getBestRowIdentifier(String cat, String schema, String table, int scope, boolean nullable) throws SQLException {
        return delegate.getBestRowIdentifier(catalog(cat), schema, table, scope, nullable);
    }

    @Override
    public ResultSet getVersionColumns(String cat, String schema, String table) throws SQLException {
        return delegate.getVersionColumns(catalog(cat), schema, table);
    }

    @Override
    public ResultSet getTablePrivileges(String cat, String schemaPattern, String tableNamePattern) throws SQLException {
        return delegate.getTablePrivileges(catalog(cat), schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getColumnPrivileges(String cat, String schema, String table, String columnNamePattern) throws SQLException {
        return delegate.getColumnPrivileges(catalog(cat), schema, table, columnNamePattern);
    }

    @Override
    public ResultSet getUDTs(String cat, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return delegate.getUDTs(catalog(cat), schemaPattern, typeNamePattern, types);
    }

    @Override
    public ResultSet getSuperTypes(String cat, String schemaPattern, String typeNamePattern) throws SQLException {
        return delegate.getSuperTypes(catalog(cat), schemaPattern, typeNamePattern);
    }

    @Override
    public ResultSet getSuperTables(String cat, String schemaPattern, String tableNamePattern) throws SQLException {
        return delegate.getSuperTables(catalog(cat), schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getAttributes(String cat, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return delegate.getAttributes(catalog(cat), schemaPattern, typeNamePattern, attributeNamePattern);
    }

    @Override
    public ResultSet getPseudoColumns(String cat, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return delegate.getPseudoColumns(catalog(cat), schemaPattern, tableNamePattern, columnNamePattern);
    }
}
