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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.crate.client.jdbc.CrateConnection;
import io.crate.client.jdbc.CrateDataSource;
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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Most applications reach the driver through a connection pool, which
 * borrows and returns connections on its own terms: it validates them,
 * resets autoCommit and read-only state between users, and rolls back
 * whatever the previous borrower left behind. A pool that finds any of
 * that missing refuses to hand out connections at all, so these are the
 * calls the driver has to answer the way JDBC describes them.
 */
public class ConnectionPoolIT extends BaseIntegrationTest {

    private HikariDataSource pool(int size) {
        CrateDataSource dataSource = new CrateDataSource();
        dataSource.setUrl(connectionUrl());

        HikariConfig config = new HikariConfig();
        config.setDataSource(dataSource);
        config.setMaximumPoolSize(size);
        config.setMinimumIdle(1);
        config.setConnectionTestQuery(null);
        return new HikariDataSource(config);
    }

    @Test
    public void pooledConnectionsCarryTheCrateBehavior() throws Exception {
        try (HikariDataSource pool = pool(2);
             Connection conn = pool.getConnection()) {
            assertThat(conn.unwrap(CrateConnection.class), is(instanceOf(CrateConnection.class)));

            ResultSet rs = conn.createStatement().executeQuery("select name from sys.cluster");
            assertThat(rs.next(), is(true));
        }
    }

    /**
     * A pool hands the same physical connection to one borrower after
     * another, resetting it in between — autoCommit back on, read-only
     * cleared, and a rollback of whatever the last borrower left open. A
     * connection that came back unusable from any of that would strand in
     * the pool.
     */
    @Test
    public void connectionsSurviveBeingReturnedAndBorrowedAgain() throws Exception {
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
    public void poolServesConcurrentBorrowers() throws Exception {
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
