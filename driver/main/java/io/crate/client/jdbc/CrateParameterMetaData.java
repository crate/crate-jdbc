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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * What a statement's parameters take, answered for the parameters this driver
 * binds itself. A CrateDB OBJECT and a column of nested arrays both take json,
 * which {@link CrateParameters} builds from a {@code Map} or a {@code List}
 * rather than expecting pgJDBC's {@code PGobject} — so pgJDBC's answer for what
 * {@code setObject} accepts is not the one that holds here.
 *
 * <p>Which parameters those are is settled once and kept, because deciding it
 * costs a query: pgJDBC reads a parameter's type name through a catalog lookup.
 * The type code, which needs no lookup, narrows the parameters worth asking
 * about first — a statement with no json parameter never triggers it at all.</p>
 */
public class CrateParameterMetaData extends ForwardingParameterMetaData {

    /**
     * Whether each parameter carries json, indexed from zero and filled in on
     * first use. A null entry is a parameter not yet asked about.
     */
    private Boolean[] json;

    CrateParameterMetaData(ParameterMetaData delegate) {
        super(delegate);
    }

    /**
     * Whether a parameter takes json: an OBJECT, a geo_shape, nested arrays.
     *
     * <p>The type code is read first for both of its properties: it settles
     * every parameter that cannot be json without the lookup, and it is where a
     * parameter index outside the statement is refused — by the delegate, in the
     * terms it refuses one everywhere else, rather than by this method reaching
     * past the end of what it kept.</p>
     */
    private boolean isJson(int param) throws SQLException {
        if (delegate.getParameterType(param) != Types.OTHER) {
            return false;
        }
        if (json == null) {
            json = new Boolean[delegate.getParameterCount()];
        }
        Boolean known = json[param - 1];
        if (known == null) {
            known = CrateJson.isJsonType(delegate.getParameterTypeName(param));
            json[param - 1] = known;
        }
        return known;
    }

    /**
     * The class a value bound to this parameter is given as. For json this is
     * {@link Object}: an OBJECT is bound from a {@code Map} and a column of
     * nested arrays from a {@code List}, and CrateDB sends both under the same
     * type, so nothing narrower than what they have in common can be promised.
     * Naming pgJDBC's {@code PGobject} — the class it would have required —
     * would name one an application never has to build for this driver.
     */
    @Override
    public String getParameterClassName(int param) throws SQLException {
        return isJson(param) ? Object.class.getName() : delegate.getParameterClassName(param);
    }
}
