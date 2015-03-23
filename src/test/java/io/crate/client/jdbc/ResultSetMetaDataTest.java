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

package io.crate.client.jdbc;

import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.client.jdbc.types.CrateArray;
import io.crate.types.*;
import io.crate.shade.org.elasticsearch.common.collect.MapBuilder;
import org.junit.Test;

import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ResultSetMetaDataTest extends AbstractCrateJDBCTest {

    @Override
    protected SQLResponse getResponse(SQLRequest request) {
        SQLResponse response = new SQLResponse(
                new String[]{"boo", "i", "l", "f", "d", "s", "t", "o", "al", "ss", "n"},
                new Object[][]{
                        new Object[]{true, 1, 2L, 4.5F, 34734875.3345734d,
                                "södkjfhsudkhfjvhvb", 0L,
                                new MapBuilder<String, Object>().put("a", 123L).map(),
                                new Long[]{ Long.MIN_VALUE, 0L, Long.MAX_VALUE },
                                new HashSet<String>(){{ add("a"); add("b"); add("c"); }},
                                null
                        }
                },
                new DataType[]{
                        BooleanType.INSTANCE,
                        IntegerType.INSTANCE,
                        LongType.INSTANCE,
                        FloatType.INSTANCE,
                        DoubleType.INSTANCE,
                        StringType.INSTANCE,
                        TimestampType.INSTANCE,
                        ObjectType.INSTANCE,
                        new ArrayType(LongType.INSTANCE),
                        new SetType(StringType.INSTANCE),
                        UndefinedType.INSTANCE
                },
                1,
                System.currentTimeMillis(),
                true
        );
        return response;
    }

    @Override
    protected SQLBulkResponse getBulkResponse(SQLBulkRequest request) {
        return null; // never used
    }

    @Override
    protected String getServerVersion() {
        return "0.42.0";
    }

    @Test
    public void testResultSetTypesStatement() throws Exception {
        Statement stmt = connection.createStatement();
        assertThat(stmt.execute("select boo, i, l, f, d, s, t, o, al, ss, n from test"), is(true));
        ResultSet resultSet = stmt.getResultSet();
        assertThat(resultSet.first(), is(true));

        assertThat(resultSet.getBoolean(1), is(true));
        assertThat(resultSet.getBoolean("boo"), is(true));

        assertThat(resultSet.getInt(2), is(1));
        assertThat(resultSet.getInt("i"), is(1));

        assertThat(resultSet.getLong(3), is(2L));
        assertThat(resultSet.getLong("l"), is(2L));

        assertThat(resultSet.getFloat(4), is(4.5F));
        assertThat(resultSet.getFloat("f"), is(4.5F));

        assertThat(resultSet.getDouble(5), is(34734875.3345734d));
        assertThat(resultSet.getDouble("d"), is(34734875.3345734d));

        assertThat(resultSet.getString(6), is("södkjfhsudkhfjvhvb"));
        assertThat(resultSet.getString("s"), is("södkjfhsudkhfjvhvb"));

        assertThat(resultSet.getTimestamp(7), is(new Timestamp(0L)));
        assertThat(resultSet.getTimestamp("t"), is(new Timestamp(0L)));

        assertThat(resultSet.getObject(8), instanceOf(Map.class));
        assertThat(resultSet.getObject("o"), instanceOf(Map.class));

        assertThat(resultSet.getArray(9), instanceOf(CrateArray.class));
        assertThat(resultSet.getArray("al"), instanceOf(CrateArray.class));

        assertThat(resultSet.getObject(9), instanceOf(Long[].class));
        assertThat(resultSet.getObject("al"), instanceOf(Long[].class));

        assertThat(resultSet.getObject(10), instanceOf(Set.class));
        assertThat(resultSet.getObject("ss"), instanceOf(Set.class));

        assertThat(resultSet.getObject(11), is(nullValue()));
        assertThat(resultSet.getObject("n"), is(nullValue()));

        ResultSetMetaData metaData = resultSet.getMetaData();
        assertThat(metaData.getColumnCount(), is(11));

        assertThat(metaData.getColumnName(1), is("boo"));
        assertThat(metaData.getColumnType(1), is(Types.BOOLEAN));
        assertThat(metaData.getColumnLabel(1), is("boo"));

        assertThat(metaData.getColumnName(2), is("i"));
        assertThat(metaData.getColumnType(2), is(Types.INTEGER));
        assertThat(metaData.getColumnLabel(2), is("i"));

        assertThat(metaData.getColumnName(3), is("l"));
        assertThat(metaData.getColumnType(3), is(Types.BIGINT));
        assertThat(metaData.getColumnLabel(3), is("l"));

        assertThat(metaData.getColumnName(4), is("f"));
        assertThat(metaData.getColumnType(4), is(Types.REAL));
        assertThat(metaData.getColumnLabel(4), is("f"));

        assertThat(metaData.getColumnName(5), is("d"));
        assertThat(metaData.getColumnType(5), is(Types.DOUBLE));
        assertThat(metaData.getColumnLabel(5), is("d"));

        assertThat(metaData.getColumnName(6), is("s"));
        assertThat(metaData.getColumnType(6), is(Types.VARCHAR));
        assertThat(metaData.getColumnLabel(6), is("s"));

        assertThat(metaData.getColumnName(7), is("t"));
        assertThat(metaData.getColumnType(7), is(Types.TIMESTAMP));
        assertThat(metaData.getColumnLabel(7), is("t"));

        assertThat(metaData.getColumnName(8), is("o"));
        assertThat(metaData.getColumnType(8), is(Types.JAVA_OBJECT));
        assertThat(metaData.getColumnLabel(8), is("o"));

        assertThat(metaData.getColumnName(9), is("al"));
        assertThat(metaData.getColumnType(9), is(Types.ARRAY));
        assertThat(metaData.getColumnLabel(9), is("al"));

        assertThat(metaData.getColumnName(10), is("ss"));
        assertThat(metaData.getColumnType(10), is(Types.ARRAY));
        assertThat(metaData.getColumnLabel(10), is("ss"));

        assertThat(metaData.getColumnName(11), is("n"));
        assertThat(metaData.getColumnType(11), is(Types.NULL));
        assertThat(metaData.getColumnLabel(11), is("n"));
    }

    @Test
    public void testArrayType() throws Exception {
        Statement stmt = connection.createStatement();
        assertThat(stmt.execute("select boo, i, l, f, d, s, t, o, al, ss from test"), is(true));
        ResultSet resultSet = stmt.getResultSet();
        assertThat(resultSet.first(), is(true));

        Array array = resultSet.getArray("al");
        assertThat(array.getArray(), instanceOf(Long[].class));
        assertThat(array.getBaseType(), is(Types.BIGINT));
        assertThat(array.getBaseTypeName(), is("long"));

        ResultSet arrayResultSet = array.getResultSet();
        assertThat(arrayResultSet.first(), is(true));
        assertThat(arrayResultSet.getLong(1), is(Long.MIN_VALUE));
        assertThat(arrayResultSet.next(), is(true));
        assertThat(arrayResultSet.getLong(1), is(0L));
        assertThat(arrayResultSet.next(), is(true));
        assertThat(arrayResultSet.getLong(1), is(Long.MAX_VALUE));
        assertThat(arrayResultSet.next(), is(false));
    }
}
