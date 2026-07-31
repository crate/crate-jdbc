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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Array wrapper that surfaces json elements (CrateDB OBJECT values) as
 * {@code Map<String, Object>}, mirroring what {@link CrateResultSet} does
 * for scalar OBJECT columns.
 */
public class CrateArray implements Array {

    private final Array delegate;

    CrateArray(Array delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object getArray() throws SQLException {
        return convert(delegate.getArray());
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return convert(delegate.getArray(map));
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return convert(delegate.getArray(index, count));
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return convert(delegate.getArray(index, count, map));
    }

    private Object convert(Object array) throws SQLException {
        if (!(array instanceof Object[])) {
            return array;
        }
        Object[] elements = (Object[]) array;
        // pgjdbc decodes json array elements as raw Strings, not PGobjects.
        boolean jsonElements = CrateJson.isJsonType(delegate.getBaseTypeName());
        Object[] converted = new Object[elements.length];
        for (int i = 0; i < elements.length; i++) {
            Object element = elements[i];
            if (jsonElements && element instanceof String) {
                converted[i] = CrateJson.parse((String) element);
            } else {
                converted[i] = CrateResultSet.fromPg(element);
            }
        }
        return converted;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return delegate.getBaseTypeName();
    }

    @Override
    public int getBaseType() throws SQLException {
        return delegate.getBaseType();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return delegate.getResultSet();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return delegate.getResultSet(map);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return delegate.getResultSet(index, count);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return delegate.getResultSet(index, count, map);
    }

    @Override
    public void free() throws SQLException {
        delegate.free();
    }
}
