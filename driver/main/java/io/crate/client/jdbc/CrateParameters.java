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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Turns statement parameters into the values pgJDBC sends over the
 * PostgreSQL wire protocol:
 *
 * <ul>
 * <li>a {@code Map} becomes json, so it lands in a CrateDB {@code OBJECT}
 *     column; stock pgJDBC would bind it as a PostgreSQL {@code hstore},
 *     an extension CrateDB does not provide,</li>
 * <li>a collection or array of {@code Map}s becomes an {@code OBJECT}
 *     array,</li>
 * <li>a collection or array whose elements are themselves collections or
 *     arrays becomes untyped json, which is how CrateDB takes a column of
 *     nested arrays,</li>
 * <li>a collection or array of moments becomes a typed array of the same
 *     instants at UTC, since pgJDBC binds a series of them by the JVM's own
 *     zone or not at all,</li>
 * <li>any other collection becomes the array of the same values, which is
 *     the form pgJDBC binds a series in,</li>
 * <li>an {@link Array} handed out by this driver is bound through the
 *     pgJDBC array it wraps,</li>
 * <li>an {@link Instant} becomes the same instant at UTC, which is the one
 *     {@code java.time} value pgJDBC has no binding for.</li>
 * </ul>
 *
 * <p>A converted parameter carries the type its conversion implies; a
 * target SQL type passed alongside it applies to values that need no
 * conversion.</p>
 */
final class CrateParameters {

    private CrateParameters() {
    }

    /**
     * The value to hand to the pgJDBC statement, or the argument itself when
     * nothing needs converting — the identity of the return value tells the
     * caller which of the two happened.
     */
    static Object toPg(Object value, CrateConnection connection) throws SQLException {
        if (value instanceof Map) {
            return CrateJson.toJsonObject(value);
        }
        if (value instanceof Instant) {
            // pgJDBC binds every other java.time value but has no branch for
            // this one, and reports that it cannot infer a type. An instant is
            // what a CrateDB timestamp holds, so it goes as the same instant
            // at UTC — the zone CrateDB keeps them in.
            return ((Instant) value).atOffset(ZoneOffset.UTC);
        }
        if (value instanceof CrateJsonArray) {
            return ((CrateJsonArray) value).untyped();
        }
        List<?> elements = CrateJsonArray.asList(value);
        if (elements != null && containsMaps(elements)) {
            return unwrap(connection.createArrayOf("object", elements.toArray()));
        }
        if (elements != null && holdsOnlyMoments(elements)) {
            return unwrap(connection.createArrayOf("timestamp with time zone", elements.toArray()));
        }
        CrateJsonArray nested = CrateJsonArray.ofNested(value);
        if (nested != null) {
            return nested.untyped();
        }
        if (value instanceof Collection) {
            Collection<?> collection = (Collection<?>) value;
            return collection.isEmpty() ? EMPTY_ARRAY : toArray(collection);
        }
        return unwrap(value);
    }

    /**
     * An empty series carries no element to take a type from. The empty array
     * literal, bound untyped, lets the server read it as the array type of the
     * column it lands in — including a column of nested arrays.
     */
    private static final Untyped EMPTY_ARRAY = new Untyped("{}");

    /**
     * A collection as the array of the same values. pgJDBC reads a series
     * from an array, taking its element type from the array's component type,
     * and has no branch for a collection — so a series binds the same way
     * whichever of the two the caller happens to hold.
     */
    private static Object toArray(Collection<?> elements) throws SQLException {
        Class<?> componentType = componentType(elements);
        if (componentType == null) {
            // Nothing but nulls names no element type. The array literal,
            // bound untyped, lets the server take one from the column.
            return new Untyped(nullLiteral(elements.size()));
        }
        Object array = java.lang.reflect.Array.newInstance(componentType, elements.size());
        int index = 0;
        for (Object element : elements) {
            java.lang.reflect.Array.set(array, index++, widened(element, componentType));
        }
        return array;
    }

    /** The array literal of a series of nulls, as the server spells one. */
    private static String nullLiteral(int size) {
        StringBuilder literal = new StringBuilder("{");
        for (int i = 0; i < size; i++) {
            literal.append(i > 0 ? ",NULL" : "NULL");
        }
        return literal.append('}').toString();
    }

    /**
     * The element type a series binds as. Elements of one class bind as that
     * class; numbers of several bind as the widest of them, since a series of
     * whole numbers is a CrateDB {@code bigint} array and one with a fraction
     * in it a {@code double precision} array, whichever Java boxes the callers
     * happened to hold.
     */
    private static Class<?> componentType(Collection<?> elements) throws SQLException {
        Class<?> componentType = null;
        for (Object element : elements) {
            if (element == null) {
                continue;
            }
            if (componentType == null || componentType == element.getClass()) {
                componentType = element.getClass();
                continue;
            }
            Class<?> widest = widest(componentType, element.getClass());
            if (widest == null) {
                throw new SQLException(
                    "Cannot bind a collection that mixes " + componentType.getName()
                    + " with " + element.getClass().getName(),
                    PSQLState.INVALID_PARAMETER_TYPE.getState());
            }
            componentType = widest;
        }
        // pgJDBC has no array form for Byte, and a CrateDB byte column is an
        // int2 either way — createArrayOf("byte", ...) resolves to the same
        // one — so a series of them binds as the width the server holds them
        // at rather than as the width the caller happened to write.
        return componentType == Byte.class ? Short.class : componentType;
    }

    /**
     * The type both numbers fit in, or null for a pair that has none. Two
     * unrelated classes in a series describe no CrateDB array type, and
     * guessing at their common superclass would only move the failure to the
     * server.
     *
     * <p>Only the boxed primitives are widened. {@code BigDecimal} and
     * {@code BigInteger} hold values no {@code double} or {@code long} can,
     * so widening to one would answer a series the caller did not write.</p>
     */
    private static Class<?> widest(Class<?> one, Class<?> other) {
        if (!isBoxedNumber(one) || !isBoxedNumber(other)) {
            return null;
        }
        return isWholeNumber(one) && isWholeNumber(other) ? Long.class : Double.class;
    }

    private static boolean isBoxedNumber(Class<?> type) {
        return isWholeNumber(type) || type == Float.class || type == Double.class;
    }

    private static boolean isWholeNumber(Class<?> type) {
        return type == Byte.class || type == Short.class
            || type == Integer.class || type == Long.class;
    }

    /** A value as the element type the series settled on. */
    private static Object widened(Object element, Class<?> componentType) {
        if (element == null || componentType.isInstance(element)) {
            return element;
        }
        Number number = (Number) element;
        if (componentType == Short.class) {
            return number.shortValue();
        }
        return componentType == Long.class ? (Object) number.longValue() : (Object) number.doubleValue();
    }

    /**
     * Text the server has to type from the column it lands in. CrateDB reads
     * the json type code as {@code OBJECT}, which reaches an OBJECT column and
     * no other, so a value bound for a column of nested arrays — or an array
     * whose element type nothing names — travels without a type code at all.
     */
    static final class Untyped {

        private final String text;

        Untyped(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    /**
     * Hands a converted parameter to pgJDBC. {@link Untyped} text is bound as
     * {@code Types.OTHER}, which is what leaves the parameter's type
     * unspecified on the wire.
     */
    static void bind(PreparedStatement delegate, int parameterIndex, Object converted) throws SQLException {
        if (converted instanceof Untyped) {
            delegate.setObject(parameterIndex, converted.toString(), Types.OTHER);
        } else {
            delegate.setObject(parameterIndex, converted);
        }
    }

    /**
     * Binds an array parameter. An array of arrays has no PostgreSQL array
     * form and goes as the json it was read as.
     */
    static void bindArray(PreparedStatement delegate, int parameterIndex, Array array) throws SQLException {
        if (array instanceof CrateJsonArray) {
            bind(delegate, parameterIndex, ((CrateJsonArray) array).untyped());
        } else {
            delegate.setArray(parameterIndex, unwrap(array));
        }
    }

    /**
     * The pgJDBC array behind an array this driver handed out; every other
     * value passes through, so the result stays reference-equal to the
     * argument when there is nothing to unwrap.
     */
    static Object unwrap(Object value) {
        return value instanceof CrateArray ? ((CrateArray) value).delegate() : value;
    }

    static Array unwrap(Array array) {
        return array instanceof CrateArray ? ((CrateArray) array).delegate() : array;
    }

    /**
     * Whether the elements are a series of {@code OBJECT} values. A series
     * that mixes {@code Map}s with other values has no CrateDB type and is
     * rejected rather than half-converted.
     */
    private static boolean containsMaps(Collection<?> elements) throws SQLException {
        boolean maps = false;
        boolean others = false;
        for (Object element : elements) {
            if (element instanceof Map) {
                maps = true;
            } else if (element != null) {
                others = true;
            }
        }
        if (maps && others) {
            throw new SQLException(
                "Cannot bind a collection that mixes objects with values of another type",
                PSQLState.INVALID_PARAMETER_TYPE.getState());
        }
        return maps;
    }

    /**
     * Whether the elements are all moments in time. pgJDBC binds a series by
     * the class of its elements, and it has an array form for
     * {@link Timestamp} alone — one it writes out in the JVM's own zone, which
     * CrateDB then reads as UTC. Every other way of spelling a moment it
     * refuses outright. So a series of moments is taken away from that path
     * and given a type, which is the one form that means the same instant
     * wherever the JVM stands.
     */
    private static boolean holdsOnlyMoments(Collection<?> elements) {
        boolean moments = false;
        for (Object element : elements) {
            if (element == null) {
                continue;
            }
            if (!(element instanceof Timestamp || element instanceof Instant
                    || element instanceof OffsetDateTime || element instanceof LocalDateTime)) {
                return false;
            }
            moments = true;
        }
        return moments;
    }

    /**
     * The series with every moment in it written as the same instant at UTC —
     * the zone CrateDB keeps timestamps in — and everything else left alone.
     *
     * <p>A {@link Timestamp} is the one moment pgJDBC will put in an array,
     * and it writes it out as a wall clock in the JVM's own zone while the
     * server reads it as UTC, so an application anywhere but at offset zero
     * stores an instant it did not mean. Naming the offset takes the JVM's
     * zone out of it. A {@link LocalDateTime} names no zone at all, so it is
     * read at UTC, which is where the server reads one sent as text.</p>
     */
    static Object[] atUtc(Object[] elements) {
        Object[] moments = new Object[elements.length];
        for (int i = 0; i < elements.length; i++) {
            OffsetDateTime moment = toOffsetDateTime(elements[i]);
            moments[i] = moment != null ? moment : elements[i];
        }
        return moments;
    }

    /** A moment as the same instant at UTC, or null for a value that is not one. */
    private static OffsetDateTime toOffsetDateTime(Object value) {
        if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atOffset(ZoneOffset.UTC);
        }
        if (value instanceof Instant) {
            return ((Instant) value).atOffset(ZoneOffset.UTC);
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toInstant().atOffset(ZoneOffset.UTC);
        }
        return null;
    }
}
