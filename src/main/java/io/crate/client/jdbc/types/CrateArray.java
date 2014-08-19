/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.client.jdbc.types;

import io.crate.action.sql.SQLResponse;
import io.crate.client.jdbc.CrateResultSet;
import io.crate.types.DataType;

import java.sql.*;
import java.util.Arrays;
import java.util.Map;

public class CrateArray implements Array {

    private final DataType type;
    private final Object[] value;
    private final String name;

    public CrateArray(DataType type, Object[] value, String name) {
        this.type = type;
        this.value = value;
        this.name = name;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return type.getName();
    }

    @Override
    public int getBaseType() throws SQLException {
        return Mappings.CRATE_TO_JDBC.get(type.getClass());
    }

    @Override
    public Object getArray() throws SQLException {
        return value;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return value;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return Arrays.copyOfRange(value, (int)index, (int)(index+count));
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getArray(index, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return getResultSet(0, value.length);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return getResultSet(0, value.length);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        SQLResponse response = new SQLResponse();
        String[] cols = new String[]{ name };
        Object[][] rows = new Object[count][];
        for (int i = (int)index; i < index+count; i++) {
            rows[i] = new Object[]{value[i]};
        }
        response.cols(cols);
        response.rows(rows);
        response.colTypes(new DataType[]{type});
        return new CrateResultSet(null, response);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getResultSet(index, count);
    }

    @Override
    public void free() throws SQLException {
    }
}
