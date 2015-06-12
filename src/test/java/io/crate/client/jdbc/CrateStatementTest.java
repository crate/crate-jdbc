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


import io.crate.action.sql.*;
import io.crate.types.*;
import io.crate.shade.org.elasticsearch.common.collect.MapBuilder;
import io.crate.shade.org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.sql.*;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class CrateStatementTest extends AbstractCrateJDBCTest {

    @Override
    protected SQLResponse getResponse(SQLRequest request) {
        SQLResponse response;
        if (request.stmt().toUpperCase().startsWith("SELECT")) {
            response = new SQLResponse(
                    new String[]{"boo", "i", "l", "f", "d", "s", "t", "o", "al", "ss", "n"},
                    new Object[][]{
                            new Object[]{true, 1, 2L, 4.5F, 34734875.3345734d,
                                    "södkjfhsudkhfjvhvb", 0L,
                                    new MapBuilder<String, Object>().put("a", 123L).map(),
                                    new Long[]{Long.MIN_VALUE, 0L, Long.MAX_VALUE},
                                    new HashSet<String>() {{
                                        add("a");
                                        add("b");
                                        add("c");
                                    }},
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
        } else if (request.stmt().toUpperCase().startsWith("ERROR")) {
            throw new SQLActionException("bla", 4000, RestStatus.BAD_REQUEST, "");
        } else {
            response = new SQLResponse(new String[0], new Object[0][], new DataType[0], 4L, System.currentTimeMillis(), true);
        }
        return response;
    }

    @Override
    protected SQLBulkResponse getBulkResponse(SQLBulkRequest request) {
        return null; // never used here
    }

    @Override
    protected String getServerVersion() {
        return "0.42.0";
    }

    @Test
    public void testCreateStatement() throws Exception {
        Statement statement = connection.createStatement();
        assertFalse(statement.isClosed());
        statement.close();

        statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        assertFalse(statement.isClosed());
        statement.close();

        statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertFalse(statement.isClosed());
        statement.close();
    }

    @Test
    public void testCreateStatementWithInvalidResultSetType() throws Exception {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("Connection: createStatement(int resultSetType, int resultSetConcurrency) is not supported with arguments: resultSetType=1004, resultSetConcurrency=1007");
        connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test
    public void testCreateStatementWithInvalidResultSetConcurrency() throws Exception {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("Connection: createStatement(int resultSetType, int resultSetConcurrency) is not supported with arguments: resultSetType=1003, resultSetConcurrency=1008");
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    }

    @Test
    public void testCreateStatementWithInvalidResultSetHoldability() throws Exception {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("Connection: createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) is not supported with arguments: resultSetType=1003, resultSetConcurrency=1007, resultSetHoldability=2");
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    public void testPrepareStatement() throws Exception {
        String query = "select count(*) from test where x = ?";
        PreparedStatement stmt = connection.prepareStatement(query);
        assertFalse(stmt.isClosed());
        stmt.close();

        stmt = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertFalse(stmt.isClosed());
        stmt.close();

        stmt = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertFalse(stmt.isClosed());
        stmt.close();
    }

    @Test
    public void testPrepareStatementWithInvalidResultSetType() throws Exception {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("Connection: prepareStatement(String sql, int resultSetType, int resultSetConcurrency) is not supported with arguments: sql=\"select count(*) from test where x = ?\", resultSetType=1004, resultSetConcurrency=1007");
        connection.prepareStatement("select count(*) from test where x = ?",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test
    public void testPrepareStatementWithInvalidResultSetConcurrency() throws Exception {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("Connection: prepareStatement(String sql, int resultSetType, int resultSetConcurrency) is not supported with arguments: sql=\"select count(*) from test where x = ?\", resultSetType=1003, resultSetConcurrency=1008");
        connection.prepareStatement("select count(*) from test where x = ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    }

    @Test
    public void testPrepareStatementWithInvalidResultSetHoldability() throws Exception {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("Connection: prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) is not supported with arguments: sql=\"select count(*) from test where x = ?\", resultSetType=1003, resultSetConcurrency=1007, resultSetHoldability=2");
        connection.prepareStatement("select count(*) from test where x = ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }


    @Test
    public void testExecute() throws Exception {
        Statement statement = connection.createStatement();
        assertFalse(statement.execute("insert into test (id) values (4547)"));
        assertTrue(statement.execute("select count(*) from test"));
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        Statement statement = connection.createStatement();
        assertThat(statement.executeUpdate("insert into test (id) values (4547)"), is(4));
    }

    @Test
    public void testExecuteUpdateReturningResultSet() throws Exception {
        Statement statement = connection.createStatement();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Execution of statement returned a ResultSet");

        statement.executeUpdate("select * from test");
    }

    @Test
    public void testCloseStatementExecute() throws Exception {
        Statement statement = connection.createStatement();
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.execute("select * from test");
    }

    @Test
    public void testCloseStatementExecuteQuery() throws Exception {
        Statement statement = connection.createStatement();
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.executeQuery("select * from test");
    }

    @Test
    public void testCloseStatementGetResultSet() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("select * from test");
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.getResultSet();
    }

    @Test
    public void testCloseStatementGetConnection() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("select * from test");
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.getConnection();
    }

    @Test
    public void testCloseStatementExecuteUpdate() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("select * from test");
        statement.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Statement is closed");
        statement.executeUpdate("insert into t (id) values(3)");
    }

    @Test
    public void testCloseStatementResultSetsClosed() throws Exception {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test");
        statement.close();
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("ResultSet is closed");

        resultSet.first();
    }

    @Test
    public void testBatchSelect() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Error during executeBatch");

        Statement statement = connection.createStatement();
        statement.addBatch("select * from test");
        statement.executeBatch();
    }

    @Test
    public void testExecuteEmptyBatch() throws Exception {
        Statement statement = connection.createStatement();
        int[] results = statement.executeBatch();
        assertThat(results.length, is(0));
    }

    @Test
    public void testExecuteBatch() throws Exception {
        Statement statement = connection.createStatement();
        statement.addBatch("update test set a = 1");
        statement.addBatch("insert into test (a=2)");

        int[] results = statement.executeBatch();
        assertArrayEquals(results, new int[]{4, 4});

    }

    @Test
    public void testExecuteBatchError() throws Exception {
        Statement statement = connection.createStatement();
        statement.addBatch("update test set a = 1");
        statement.addBatch("error yeah!");
        statement.addBatch("insert into test (a=2)");

        try {
            statement.executeBatch();
            fail("no SQLException raised");
        } catch (BatchUpdateException e) {
            assertArrayEquals(e.getUpdateCounts(), new int[]{4, Statement.EXECUTE_FAILED, 4});
        }

    }
}
