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

import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

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
    private boolean freed;

    CrateArray(Array delegate) {
        this.delegate = delegate;
    }

    /**
     * Every method but {@link #free()} raises once the array has been given
     * up, which is what JDBC has a freed array do. Freeing drops what pgJDBC
     * read the elements from, and what its own methods do about that varies by
     * the one called: some answer null, and some reach for the connection they
     * no longer hold.
     */
    private void checkUsable() throws SQLException {
        if (freed) {
            throw new PSQLException("This array has been freed.", PSQLState.OBJECT_NOT_IN_STATE);
        }
    }

    /**
     * The pgJDBC array underneath. Binding an array as a statement parameter
     * goes through the array literal or the binary representation pgJDBC
     * builds from its own implementation.
     */
    Array delegate() {
        return delegate;
    }

    @Override
    public Object getArray() throws SQLException {
        checkUsable();
        return convert(delegate.getArray());
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        checkUsable();
        return convert(delegate.getArray(map));
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        checkUsable();
        return convert(delegate.getArray(index, count));
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        checkUsable();
        return convert(delegate.getArray(index, count, map));
    }

    private Object convert(Object array) throws SQLException {
        // pgjdbc decodes json array elements as raw Strings, not PGobjects.
        return convert(array, CrateJson.isJsonType(delegate.getBaseTypeName()));
    }

    /**
     * Elements are converted in place down through nested arrays. An array
     * whose elements all come back unchanged is handed on as pgJDBC built
     * it, keeping its component type ({@code String[]}, {@code Integer[]},
     * ...); converting widens the array to {@code Object[]}, since a
     * converted value does not share a type with the elements around it.
     */
    private static Object convert(Object array, boolean jsonElements) throws SQLException {
        if (!(array instanceof Object[])) {
            return array;
        }
        Object[] elements = (Object[]) array;
        Object[] converted = null;
        for (int i = 0; i < elements.length; i++) {
            Object element = elements[i];
            Object value;
            if (element instanceof Object[]) {
                value = convert(element, jsonElements);
            } else if (jsonElements && element instanceof String) {
                value = CrateJson.parse((String) element);
            } else {
                value = CrateResultSet.fromPg(element);
            }
            if (converted == null && value != element) {
                converted = new Object[elements.length];
                System.arraycopy(elements, 0, converted, 0, i);
            }
            if (converted != null) {
                converted[i] = value;
            }
        }
        return converted == null ? elements : converted;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        checkUsable();
        return delegate.getBaseTypeName();
    }

    @Override
    public int getBaseType() throws SQLException {
        checkUsable();
        return delegate.getBaseType();
    }

    /**
     * The index/value pairs of this array. The rows belong to the array rather
     * than to a statement, so they report none.
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
        checkUsable();
        return new CrateResultSet(delegate.getResultSet(), null);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        checkUsable();
        return new CrateResultSet(delegate.getResultSet(map), null);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        checkUsable();
        return new CrateResultSet(delegate.getResultSet(index, count), null);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        checkUsable();
        return new CrateResultSet(delegate.getResultSet(index, count, map), null);
    }

    @Override
    public void free() throws SQLException {
        freed = true;
        delegate.free();
    }

    /** The array literal, as the wrapped array prints one. */
    @Override
    public String toString() {
        return delegate.toString();
    }
}
