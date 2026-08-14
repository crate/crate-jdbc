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
 * binds itself. {@link CrateParameters} builds a json parameter from a
 * {@code Map} or a {@code List}, so an application never has to construct
 * pgJDBC's {@code PGobject} for one.
 *
 * <p>The reading side, and the caching this shares with it, is described in
 * {@link CrateResultSetMetaData}.</p>
 */
public class CrateParameterMetaData extends ForwardingParameterMetaData {

    /** Whether each parameter carries json, filled in on first use. */
    private Boolean[] json;

    CrateParameterMetaData(ParameterMetaData delegate) {
        super(delegate);
    }

    /** Whether a parameter takes json: an OBJECT, a geo_shape, nested arrays. */
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
     * The class a value bound to this parameter is given as. A json parameter
     * is described as {@link Object}, for the reason
     * {@link CrateResultSetMetaData#getColumnClassName} gives.
     */
    @Override
    public String getParameterClassName(int param) throws SQLException {
        return isJson(param) ? Object.class.getName() : delegate.getParameterClassName(param);
    }
}
