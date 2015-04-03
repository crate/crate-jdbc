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

import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class CrateStatement extends CrateStatementBase {


    protected SQLResponse sqlResponse;
    protected List<String> batch = new LinkedList<>();

    public CrateStatement(CrateConnection connection) {
        super(connection);
    }


    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        if (execute(sql) && sqlResponse.rowCount() > 0) {
            resultSet = null;
            throw new SQLException("Execution of statement returned a ResultSet");
        } else {
            // return 0 if no affected Rows are given
            return (int)Math.max(0L, sqlResponse.rowCount());
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        SQLRequest sqlRequest = new SQLRequest(sql);
        sqlRequest.setDefaultSchema(connection.getSchema());
        sqlRequest.includeTypesOnResponse(true);
        try {
            sqlResponse = connection.client().sql(sqlRequest).actionGet();
        } catch (SQLActionException e) {
            throw new SQLException(e.getMessage(), e);
        }
        if (sqlResponse.rowCount() < 0 || sqlResponse.rowCount() != sqlResponse.rows().length) {
            return false;
        }
        resultSet = new CrateResultSet(this, sqlResponse);
        return true;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return (int) sqlResponse.rowCount();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        if (resultSet != null) {
            resultSet.close();
        }
        boolean failed = false;
        int[] results = new int[batch.size()];

        for (int i = 0, batchSize = batch.size(); i < batchSize; i++) {
            String command = batch.get(i);
            try {
                int result = executeUpdate(command);
                results[i] = (result == -1 ? SUCCESS_NO_INFO : result);
            } catch (SQLException e) {
                failed = true;
                results[i] = EXECUTE_FAILED;
            }
        }
        clearBatch();
        if (failed) {
            throw new BatchUpdateException("Error during executeBatch", results);
        }
        return results;
    }

}
