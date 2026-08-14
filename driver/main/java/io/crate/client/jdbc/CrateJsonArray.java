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
 * An array whose elements are arrays. The PostgreSQL array format cannot hold
 * sub-arrays of differing length, so CrateDB sends a column of
 * {@code array(array(...))} as json, which pgJDBC has no decoder for. This
 * reads the json instead.
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

    /** The array a json value holds, or nothing for an OBJECT. */
    static CrateJsonArray of(String json) throws SQLException {
        Object value = CrateJson.parse(json);
        if (!(value instanceof List)) {
            throw new SQLDataException("Not an array: " + json,
                PSQLState.MOST_SPECIFIC_TYPE_DOES_NOT_MATCH.getState());
        }
        return new CrateJsonArray(json, (List<?>) value);
    }

    /** A Java value as the array CrateDB reads back, or null for anything else. */
    static CrateJsonArray ofNested(Object value) throws SQLException {
        List<?> elements = asList(value);
        if (elements == null || !containsArrays(elements)) {
            return null;
        }
        return new CrateJsonArray(CrateJson.write(elements), elements);
    }

    /**
     * A series of values as a list, written either as a collection or as an
     * array of objects, or null for a value that is not a series at all.
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

    /** Sub-arrays as {@code Object[]} and OBJECT values as {@code Map}, as elsewhere. */
    @Override
    public Object getArray() {
        return toArray(elements);
    }

    /** The type map names Java classes for SQL user types, and json carries none. */
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
     * rest of the array rather than none of it.
     *
     * <p>An array of any other element type is pgJDBC's, which reads a count
     * of zero that way from the first element alone. Past it, the count is
     * added to the index before being recognised as zero, and the read is
     * refused as out of range. The two agree on {@code (1, 0)} and part company
     * after it.</p>
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
     * The elements are arrays and reach the driver as json, with no PostgreSQL
     * element type behind them to name.
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
     * The rows would need a column descriptor for an array of arrays, which the
     * PostgreSQL protocol has none of. {@link #getArray()} reads the same
     * elements.
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
