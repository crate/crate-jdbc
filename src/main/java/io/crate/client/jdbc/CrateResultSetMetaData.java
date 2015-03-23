/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.client.jdbc;

import io.crate.shade.com.google.common.base.Preconditions;
import io.crate.client.jdbc.types.Mappings;
import io.crate.types.*;

import java.lang.reflect.Array;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class CrateResultSetMetaData implements ResultSetMetaData {

    private final List<String> columns;
    private final List<DataType> types;

    public CrateResultSetMetaData(List<String> columns, List<DataType> types) {
        Preconditions.checkArgument(columns.size() == types.size(),
                "sizes columns and types do not match");
        this.columns = columns;
        this.types = types;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return DataTypes.NUMERIC_PRIMITIVE_TYPES.contains(types.get(column - 1));
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 50;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columns.get(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columns.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        DataType type = types.get(column-1);
        Integer jdbcType = Mappings.CRATE_TO_JDBC.get(type.getClass());
        if (jdbcType == null) {
            throw new SQLDataException(
                    String.format(Locale.ENGLISH,
                            "type '%s' not supported by JDBC driver", type.getName()));
        }
        return jdbcType;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return types.get(column-1).getName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        // everything is readonly for now
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        DataType type = types.get(column - 1 );
        switch (type.id()) {
            case BooleanType.ID:
                return Boolean.class.getName();
            case ByteType.ID:
                return Byte.class.getName();
            case ShortType.ID:
                return Short.class.getName();
            case IntegerType.ID:
                return Integer.class.getName();
            case LongType.ID:
                return Long.class.getName();
            case FloatType.ID:
                return Float.class.getName();
            case DoubleType.ID:
                return Double.class.getName();
            case StringType.ID:
                return String.class.getName();
            case TimestampType.ID:
                return Timestamp.class.getName();
            case IpType.ID:
                return String.class.getName();
            case ArrayType.ID:
                // TODO: maybe return concrete (e.g. "Long[]") class names
                return Array.class.getName();
            case ObjectType.ID:
                return Map.class.getName();
            case SetType.ID:
                return Set.class.getName();
            default:
                return Object.class.getName();
        }

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
}
