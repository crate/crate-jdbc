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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * What a result set's columns hold, answered for the columns this driver reads
 * itself. A CrateDB OBJECT and a column of nested arrays both arrive as json,
 * which {@link CrateResultSet} decodes into a {@code Map} or a {@code List}
 * instead of handing over pgJDBC's {@code PGobject}, so pgJDBC's answer for
 * what {@code getObject} produces does not hold here.
 *
 * <p>Deciding which columns those are costs a query, since pgJDBC reads a type
 * name through a catalog lookup, so the answer is kept once found. The type
 * code needs no lookup and settles every column that cannot be json, so a row
 * without one never triggers the lookup.</p>
 *
 * <p>{@link CrateParameterMetaData} answers the same question for parameters.</p>
 */
public class CrateResultSetMetaData extends ForwardingResultSetMetaData {

    /** Whether each column carries json, filled in on first use. */
    private Boolean[] json;

    CrateResultSetMetaData(ResultSetMetaData delegate) throws SQLException {
        super(delegate);
    }

    /**
     * Whether a column holds json: an OBJECT, a geo_shape, nested arrays.
     * Reading the type code first leaves the delegate to refuse a column index
     * outside the row, in the terms it refuses one everywhere else.
     */
    boolean isJson(int column) throws SQLException {
        if (delegate.getColumnType(column) != Types.OTHER) {
            return false;
        }
        if (json == null) {
            json = new Boolean[delegate.getColumnCount()];
        }
        Boolean known = json[column - 1];
        if (known == null) {
            known = CrateJson.isJsonType(delegate.getColumnTypeName(column));
            json[column - 1] = known;
        }
        return known;
    }

    /**
     * The class a value of this column is read as. CrateDB sends an OBJECT and
     * a column of nested arrays under one type, read as a {@code Map} and a
     * {@code List}, so {@link Object} is as narrow as a json column gets.
     */
    @Override
    public String getColumnClassName(int column) throws SQLException {
        return isJson(column) ? Object.class.getName() : delegate.getColumnClassName(column);
    }
}
