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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * What a result set's columns hold, answered for the columns this driver reads
 * itself. A CrateDB OBJECT and a column of nested arrays both arrive as json,
 * which {@link CrateResultSet} decodes rather than handing over as pgJDBC's
 * {@code PGobject} — so pgJDBC's answer for what {@code getObject} produces is
 * not the one that holds here.
 *
 * <p>Which columns those are is settled once and kept, because deciding it
 * costs a query: pgJDBC reads a column's type name through a catalog lookup it
 * runs for the whole row. The type code, which needs no lookup, narrows the
 * columns worth asking about first — a row with no json column never triggers
 * it at all.</p>
 */
public class CrateResultSetMetaData extends ForwardingResultSetMetaData {

    /**
     * Whether each column carries json, indexed from zero and filled in on
     * first use. A null entry is a column not yet asked about.
     */
    private Boolean[] json;

    CrateResultSetMetaData(ResultSetMetaData delegate) throws SQLException {
        super(delegate);
    }

    /**
     * Whether a column holds json: an OBJECT, a geo_shape, nested arrays.
     *
     * <p>The type code is read first for both of its properties: it settles
     * every column that cannot be json without the lookup, and it is where a
     * column index outside the row is refused — by the delegate, in the terms
     * it refuses one everywhere else, rather than by this method reaching past
     * the end of what it kept.</p>
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
     * The class a value of this column is read as. For json this is
     * {@link Object}: an OBJECT is read as a {@code Map} and a column of nested
     * arrays as a {@code List}, and CrateDB sends both under the same type, so
     * nothing narrower than what they have in common can be promised. Naming
     * pgJDBC's {@code PGobject} — the class it would have produced — would name
     * one an application never receives from this driver.
     */
    @Override
    public String getColumnClassName(int column) throws SQLException {
        return isJson(column) ? Object.class.getName() : delegate.getColumnClassName(column);
    }
}
