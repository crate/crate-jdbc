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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.PGResultSetMetaData;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.lang.reflect.Proxy;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrateResultSetMetaDataTest {

    /** A json column and a column of another type, which is all the decision turns on. */
    private static final int JSON_COLUMN = 1;
    private static final int TEXT_COLUMN = 2;

    /**
     * CrateDB sends an OBJECT as json, which this driver reads as a
     * {@code Map} — so the class it names is the one a caller receives, not
     * the {@code PGobject} pgJDBC would have produced.
     */
    @Test
    public void aJsonColumnIsNamedAsWhatTheDriverReadsItAs() throws SQLException {
        Columns columns = new Columns();

        assertThat(columns.metaData().getColumnClassName(JSON_COLUMN), is(Object.class.getName()));
        assertThat(columns.metaData().getColumnClassName(TEXT_COLUMN), is(String.class.getName()));
    }

    /**
     * Deciding whether a column is json costs a catalog lookup, so the answer
     * is kept. The type code, which costs nothing, settles every column that
     * cannot be json without reaching the lookup at all.
     */
    @Test
    public void aColumnIsLookedUpOnceAndOnlyWhereItsTypeCodeAllowsJson() throws SQLException {
        Columns columns = new Columns();
        CrateResultSetMetaData metaData = columns.metaData();

        metaData.getColumnClassName(JSON_COLUMN);
        metaData.getColumnClassName(JSON_COLUMN);
        metaData.getColumnClassName(TEXT_COLUMN);
        metaData.getColumnClassName(TEXT_COLUMN);

        assertThat(columns.typeNameLookups, is(1));
    }

    /**
     * A column outside the row is refused the way the wrapped metadata refuses
     * one. Deciding what a column holds happens before anything is read from
     * it, so an index nothing describes has to be turned away there rather
     * than reaching past what was kept about the columns that exist.
     */
    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 3, 99})
    public void aColumnOutsideTheRowIsRefused(int column) throws SQLException {
        CrateResultSetMetaData metaData = new Columns().metaData();

        SQLException refused = assertThrows(SQLException.class,
            () -> metaData.getColumnClassName(column));

        assertThat(refused.getSQLState(), is(PSQLState.INVALID_PARAMETER_VALUE.getState()));
    }

    /**
     * A stand-in for the metadata pgJDBC builds for a row, answering the three
     * questions the wrapper asks of it and counting the one that costs a
     * lookup. It is reached through a {@link Proxy} because the wrapper resolves
     * pgJDBC's own interface from its delegate, so the double has to carry both.
     */
    private static final class Columns {

        private int typeNameLookups;

        private CrateResultSetMetaData metaData() throws SQLException {
            ResultSetMetaData delegate = (ResultSetMetaData) Proxy.newProxyInstance(
                Columns.class.getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class, PGResultSetMetaData.class},
                (proxy, method, arguments) -> answer(proxy, method.getName(), arguments));
            return new CrateResultSetMetaData(delegate);
        }

        private Object answer(Object proxy, String called, Object[] arguments) throws SQLException {
            switch (called) {
                case "unwrap":
                    return proxy;
                case "getColumnCount":
                    return 2;
                case "getColumnType":
                    return column(arguments) == JSON_COLUMN ? Types.OTHER : Types.VARCHAR;
                case "getColumnTypeName":
                    typeNameLookups++;
                    return column(arguments) == JSON_COLUMN ? "json" : "varchar";
                case "getColumnClassName":
                    return String.class.getName();
                default:
                    throw new UnsupportedOperationException(called);
            }
        }

        /**
         * The column an argument names, refused where the row has no such
         * column — which is what pgJDBC does, and what the wrapper is being
         * held to doing on its way there.
         */
        private static int column(Object[] arguments) throws SQLException {
            int column = (Integer) arguments[0];
            if (column < 1 || column > 2) {
                throw new PSQLException("The column index is out of range: " + column,
                    PSQLState.INVALID_PARAMETER_VALUE);
            }
            return column;
        }
    }
}
