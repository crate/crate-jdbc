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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.PGConnection;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ConnectionPoolITest extends BaseIntegrationTest {

    private HikariDataSource pool(int size) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connectionUrl());
        config.setMaximumPoolSize(size);
        config.setMinimumIdle(1);
        config.setConnectionTestQuery(null);
        return new HikariDataSource(config);
    }

    @Test
    public void testPooledConnection() throws Exception {
        try (HikariDataSource pool = pool(2);
             Connection conn = pool.getConnection()) {
            assertThat(conn.isWrapperFor(PGConnection.class), is(true));

            ResultSet rs = conn.createStatement().executeQuery("select name from sys.cluster");
            assertThat(rs.next(), is(true));
        }
    }

    @Test
    public void testConnectionReset() throws Exception {
        try (HikariDataSource pool = pool(1)) {
            for (int borrow = 0; borrow < 3; borrow++) {
                try (Connection conn = pool.getConnection()) {
                    conn.setAutoCommit(false);
                    conn.setReadOnly(false);
                    conn.createStatement().execute("select 1");
                    if (borrow % 2 == 0) {
                        conn.commit();
                    } else {
                        conn.rollback();
                    }
                    conn.setReadOnly(true);
                }
            }

            try (Connection conn = pool.getConnection()) {
                assertThat(conn.getAutoCommit(), is(true));
                assertThat(conn.isReadOnly(), is(false));
                assertThat(conn.isValid(2), is(true));
            }
        }
    }

    @Test
    public void testConcurrentConnections() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(4);
        try (HikariDataSource pool = pool(4)) {
            List<Callable<Integer>> queries = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                queries.add(() -> {
                    try (Connection conn = pool.getConnection()) {
                        ResultSet rs = conn.createStatement()
                            .executeQuery("select count(*) from sys.summits");
                        rs.next();
                        return rs.getInt(1);
                    }
                });
            }
            int expected = queries.get(0).call();
            for (Future<Integer> result : workers.invokeAll(queries)) {
                assertThat(result.get(), is(expected));
            }
        } finally {
            workers.shutdownNow();
        }
    }
}
