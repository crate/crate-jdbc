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

package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgResultSet;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins cursor-based fetching through the crate:// driver. pgjdbc honors
 * {@code setFetchSize} only while autoCommit is disabled: with manual
 * commit it opens a forward cursor and buffers one batch of rows at a
 * time, with autoCommit enabled it materializes the whole result set. The
 * buffered batch is observed on the underlying pgjdbc result set, reached
 * via {@link ResultSet#unwrap}.
 */
public class FetchSizeIT extends BaseIntegrationTest {

    @Test
    public void fetchSizeBatchesRowsWithManualCommit() throws Exception {
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(10);
                statement.execute("select * from sys.summits");
                ResultSet rs = statement.getResultSet();
                assertEquals(10, rs.getFetchSize());
                assertEquals(10, bufferedRowCount(rs));
            }
        }
    }

    @Test
    public void fetchSizeIgnoredWithAutoCommit() throws Exception {
        try (Connection connection = connect()) {
            connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(10);
                statement.execute("select * from sys.summits");
                ResultSet rs = statement.getResultSet();
                assertEquals(10, rs.getFetchSize());
                assertEquals(countSummits(connection), bufferedRowCount(rs));
            }
        }
    }

    @Test
    public void fetchSizeBatchesRowsWithPreparedStatement() throws Exception {
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement("select * from sys.summits")) {
                statement.setFetchSize(10);
                ResultSet rs = statement.executeQuery();
                assertEquals(10, bufferedRowCount(rs));
            }
        }
    }

    private static int bufferedRowCount(ResultSet rs) throws Exception {
        PgResultSet pgResultSet = rs.unwrap(PgResultSet.class);
        Field rowsField = PgResultSet.class.getDeclaredField("rows");
        rowsField.setAccessible(true);
        return ((List<?>) rowsField.get(pgResultSet)).size();
    }

    private static int countSummits(Connection connection) throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select count(*) from sys.summits");
        rs.next();
        return rs.getInt(1);
    }
}
