/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CratePreparedStatementTest extends AbstractCrateJDBCTest {

    private static SQLResponse ID_RESPONSE = new SQLResponse(new String[]{"id"}, new Object[][]{new Object[]{0L}}, new DataType[]{DataTypes.INTEGER}, 1L, 0L, true);
    private static SQLResponse ROWCOUNT_RESPONSE = new SQLResponse(new String[0], new Object[0][], new DataType[0], 4L, 0L, true);

    @Override
    protected SQLResponse getResponse(SQLRequest request) {
        if (request.stmt().toUpperCase().startsWith("SELECT")) {
            return ID_RESPONSE;
        } else {
            return ROWCOUNT_RESPONSE;
        }
    }



    @Test
    public void testExecuteQuerySql() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("executeQuery(String) not supported on PreparedStatement. Use executeQuery()");

        PreparedStatement preparedStatement = connection.prepareStatement("select * from test");
        preparedStatement.executeQuery("this is wrong");
    }

    @Test
    public void testExecuteUpdateSql() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("executeUpdate(String) not supported on PreparedStatement. Use executeQuery().");

        PreparedStatement preparedStatement = connection.prepareStatement("select * from test");
        preparedStatement.executeUpdate("this is wrong");
    }

    @Test
    public void testExecuteQuery() throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("select * from test where a = ?");
        preparedStatement.setInt(1, 1);
        ResultSet resultSet = preparedStatement.executeQuery();
        assertThat(resultSet.getMetaData().getColumnCount(), is(1));
        resultSet.last();
        assertThat(resultSet.getRow(), is(1));
        resultSet.first();
        assertThat(resultSet.getLong(1), is(0L));
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("update test set a = ?");
        preparedStatement.setString(1, "foobar");
        int rowCount = preparedStatement.executeUpdate();
        assertThat(rowCount, is(4));
    }

    @Test
    public void testExecute() throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("select * from test where a = ?");
        preparedStatement.setInt(1, 1);
        assertTrue(preparedStatement.execute());
        ResultSet resultSet = preparedStatement.getResultSet();
        assertThat(resultSet.getMetaData().getColumnCount(), is(1));
        resultSet.last();
        assertThat(resultSet.getRow(), is(1));
        resultSet.first();
        assertThat(resultSet.getLong(1), is(0L));

        preparedStatement = connection.prepareStatement("update test set a = ?");
        preparedStatement.setString(1, "foobar");
        assertFalse(preparedStatement.execute());
        int rowCount = preparedStatement.getUpdateCount();
        assertThat(rowCount, is(4));
    }

    @Test
    public void testCountParameters() throws Exception {
        assertParameterCount("select '?' from sys.cluster", newBitSet(""));
        assertParameterCount("select * from sys.cluster where a=?", newBitSet("1"));
        assertParameterCount("select '?''?' from sys.cluster", newBitSet(""));
        assertParameterCount("select '?''?' from sys.cluster where a=$5 and b='$7'", newBitSet("00001"));
        assertParameterCount("select $a from sys.cluster", newBitSet(""));
        assertParameterCount("select ? from sys.cluster where a = $1", newBitSet("1"));
        assertParameterCount("select ? from sys.cluster where a = ?", newBitSet("11"));
        assertParameterCount("select ? from sys.cluster where a = $2", newBitSet("11"));
        assertParameterCount("select ? from sys.cluster where a = $3", newBitSet("101"));

    }

    /**
     * create a new BitSet from the given bits in a string.
     *
     * @param bits every character that is not 0 is interpreted as a 1 and its index is set
     *             int the resulting BitSet
     * @return a new BitSet
     */
    private BitSet newBitSet(String bits) {
        BitSet bs = new BitSet(bits.length());
        for (int i = 0; i< bits.length(); i++) {
            if (bits.charAt(i) != '0') {
                bs.set(i);
            }
        }
        return bs;
    }

    private void assertParameterCount(String stmt, BitSet count) {
        assertThat(CratePreparedStatement.CratePreparedStatementParser.getParameters(stmt).toString(), is(count.toString()));
    }

    @Test
    public void testParametersNotAllValuesGivenExecute() throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("select *, ? from test where a = ? and b = $3");
        preparedStatement.setString(1, "foo");
        preparedStatement.setInt(2, 4);

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Not all parameters have been provided a value");

        preparedStatement.execute();
    }

    @Test
    public void testParameters() throws Exception {
        CratePreparedStatement preparedStatement = (CratePreparedStatement)connection.prepareStatement("select *, ? from test where a = ? and b = $3");
        preparedStatement.setObject(1, "hallo");
        preparedStatement.setByte(2, (byte)4);
        preparedStatement.setInt(3, 1);
        assertTrue(preparedStatement.execute());
    }

    @Test
    public void testExecuteBatchEmpty() throws Exception {
        CratePreparedStatement preparedStatement = (CratePreparedStatement) connection.prepareStatement("update test where c = ? and a = ? and b = $3");
        preparedStatement.setInt(1, 1);
        preparedStatement.setObject(2, newHashMap());
        preparedStatement.setLong(1, Long.MAX_VALUE);

        int[] results = preparedStatement.executeBatch();
        assertThat(results.length, is(0));
    }

    @Test
    public void testExecuteBatchOneRow() throws Exception {
        CratePreparedStatement preparedStatement = (CratePreparedStatement) connection.prepareStatement("update test where c = ? and a = ? and b = $3");
        preparedStatement.setInt(1, 1);
        preparedStatement.setObject(2, newHashMap());
        preparedStatement.setLong(3, Long.MAX_VALUE);
        preparedStatement.addBatch();

        int[] results = preparedStatement.executeBatch();
        assertThat(results.length, is(1));
        assertThat(results[0], is(4));
    }

    @Test
    public void testExecuteBatchMultipleRows() throws Exception {
        CratePreparedStatement preparedStatement = (CratePreparedStatement) connection.prepareStatement("update test where c = ? and a = ? and b = $3");

        preparedStatement.setInt(1, 1);
        preparedStatement.setObject(2, newHashMap());
        preparedStatement.setLong(3, Long.MAX_VALUE);
        preparedStatement.addBatch();

        preparedStatement.setString(3, "foo");
        preparedStatement.setString(2, "bar");
        preparedStatement.setString(1, "baz");
        preparedStatement.addBatch();

        int[] results = preparedStatement.executeBatch();
        assertThat(results.length, is(2));
        assertArrayEquals(results, new int[]{4, Statement.SUCCESS_NO_INFO});

    }

    @Test
    public void testExecuteBatchNotAllParamatersProvided() throws Exception {
        CratePreparedStatement preparedStatement = (CratePreparedStatement) connection.prepareStatement("update test where c = ? and a = ? and b = $3");
        preparedStatement.setInt(1, 1);
        preparedStatement.setObject(2, newHashMap());

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Not all parameters have been provided a value");

        preparedStatement.addBatch();
    }
}
