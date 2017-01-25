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

package io.crate.client.jdbc.integrationtests;

import org.junit.Test;
import org.postgresql.core.*;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.List;


/**
 * We are allowed to disable autoCommit (= manual commit) if strict mode is not enabled.
 * Manual commit is required for {@link org.postgresql.jdbc.PgStatement#setFetchSize(int)} to work correctly!
 * If autoCommit is true {@link org.postgresql.jdbc.PgStatement#executeInternal(CachedQuery, ParameterList, int)}
 * will never set the QueryExecutor.QUERY_FORWARD_CURSOR flag and therefore fetch all results at once instead of
 * batching them.
 */
public class CrateJDBCFetchSizeIntegrationTest extends CrateJDBCIntegrationTest {

    /**
     * fetch size and execution flag is correctly appied if autoCommit == false
     */
    @Test
    public void testFetchSizeNotIgnoredIfManualCommit() throws Exception {
        try (Connection connection = DriverManager.getConnection(getConnectionString())) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(10);
                statement.execute("select * from sys.summits");
                ResultSet rs = statement.getResultSet();
                assertEquals(10, rs.getFetchSize());
                Field rowsField = rs.getClass().getDeclaredField("rows");
                rowsField.setAccessible(true);
                List rows = (List) rowsField.get(rs);
                assertEquals(10, rows.size());
            }
        }
    }

    /*
     * fetch size is ignored if autoCommit == true
     */
    @Test
    public void testFetchSizeIgnoredIfAutocommit() throws Exception {
        try (Connection connection = DriverManager.getConnection(getConnectionString())) {
            connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(10);
                statement.execute("select * from sys.summits");
                ResultSet rs = statement.getResultSet();
                assertEquals(10, rs.getFetchSize());
                Field rowsField = rs.getClass().getDeclaredField("rows");
                rowsField.setAccessible(true);
                List rows = (List) rowsField.get(rs);
                assertEquals(1605, rows.size());
            }
        }
    }
}
