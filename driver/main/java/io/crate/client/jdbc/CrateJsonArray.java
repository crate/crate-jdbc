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

import org.postgresql.util.PSQLState;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * An array whose elements are arrays. CrateDB sends a column of
 * {@code array(array(...))} as json rather than as a PostgreSQL array — the
 * PostgreSQL array format cannot hold sub-arrays of differing length — so
 * pgJDBC has no decoder for it, and this reads the json instead.
 *
 * <p>The value travels as its json text in both directions: read from a
 * column, and bound back as a parameter through {@link CrateParameters}.</p>
 */
public final class CrateJsonArray implements Array {

    private final String json;
    private final List<?> elements;

    private CrateJsonArray(String json, List<?> elements) {
        this.json = json;
        this.elements = elements;
    }

    /**
     * The array a json value holds. A json value that is not an array — an
     * OBJECT column — has no array to give.
     */
    static CrateJsonArray of(String json) throws SQLException {
        Object value = CrateJson.parse(json);
        if (!(value instanceof List)) {
            throw new SQLDataException("Not an array: " + json,
                PSQLState.MOST_SPECIFIC_TYPE_DOES_NOT_MATCH.getState());
        }
        return new CrateJsonArray(json, (List<?>) value);
    }

    /**
     * A Java value as the array CrateDB reads it back as, or null when it is
     * not a series of arrays and so belongs in a column of another type.
     */
    static CrateJsonArray ofNested(Object value) throws SQLException {
        List<?> elements = asList(value);
        if (elements == null || !containsArrays(elements)) {
            return null;
        }
        return new CrateJsonArray(CrateJson.write(elements), elements);
    }

    /**
     * A series of values as a list, however it was written — a collection or
     * an array of objects — or null for a value that is not a series at all.
     */
    static List<?> asList(Object value) {
        if (value instanceof List) {
            return (List<?>) value;
        }
        // Not List.copyOf: an array element may be null.
        if (value instanceof Collection) {
            return new ArrayList<>((Collection<?>) value);
        }
        if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        }
        return null;
    }

    private static boolean containsArrays(List<?> elements) {
        for (Object element : elements) {
            if (element instanceof Collection || (element != null && element.getClass().isArray())) {
                return true;
            }
        }
        return false;
    }

    /** This array as a parameter, for the server to type from where it lands. */
    CrateParameters.Untyped untyped() {
        return new CrateParameters.Untyped(json);
    }

    /**
     * The elements as Java values: sub-arrays as {@code Object[]}, OBJECT
     * values as {@code Map}, the same shapes the rest of the driver hands out.
     */
    @Override
    public Object getArray() {
        return toArray(elements);
    }

    /**
     * The type map is ignored: it names the Java classes to read SQL user
     * types into, and json carries no type for one to name.
     */
    @Override
    public Object getArray(Map<String, Class<?>> map) {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return toArray(slice(index, count));
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getArray(index, count);
    }

    /**
     * JDBC counts array elements from one, and reads a count of zero as the
     * rest of the array rather than none of it — which is what an array of
     * any other element type does here, since that one is pgJDBC's.
     */
    private List<?> slice(long index, int count) throws SQLException {
        long from = index - 1;
        if (index < 1 || count < 0 || from > elements.size()) {
            throw sliceOutOfRange(index, count);
        }
        long length = count == 0 ? elements.size() - from : count;
        if (from + length > elements.size()) {
            throw sliceOutOfRange(index, count);
        }
        return elements.subList((int) from, (int) (from + length));
    }

    private SQLException sliceOutOfRange(long index, int count) {
        return new SQLDataException(
            "Cannot read " + count + " elements from position " + index
            + " of an array that holds " + elements.size(),
            PSQLState.NUMERIC_VALUE_OUT_OF_RANGE.getState());
    }

    private static Object[] toArray(List<?> elements) {
        Object[] values = new Object[elements.size()];
        for (int i = 0; i < values.length; i++) {
            Object element = elements.get(i);
            values[i] = element instanceof List ? toArray((List<?>) element) : element;
        }
        return values;
    }

    /**
     * The elements are arrays, and json is how they reach the driver — there
     * is no PostgreSQL element type behind them to name.
     */
    @Override
    public String getBaseTypeName() {
        return "json";
    }

    @Override
    public int getBaseType() {
        return Types.OTHER;
    }

    /**
     * A result set over these elements would have to describe a column whose
     * type is "array", which the PostgreSQL protocol has no descriptor for.
     * {@link #getArray()} reads the same elements as Java values.
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
        throw elementsAreArrays();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw elementsAreArrays();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw elementsAreArrays();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw elementsAreArrays();
    }

    private static SQLFeatureNotSupportedException elementsAreArrays() {
        return new SQLFeatureNotSupportedException(
            "The elements of this array are arrays, which have no column type to read them as. "
            + "Use getArray().", PSQLState.NOT_IMPLEMENTED.getState());
    }

    /** Nothing is held open; the value came in with the row. */
    @Override
    public void free() {
    }

    @Override
    public String toString() {
        return json;
    }
}
