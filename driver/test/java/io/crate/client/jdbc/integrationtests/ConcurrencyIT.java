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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * The driver under several threads at once.
 *
 * <p>An application reaches the driver through a pool, and a pool is
 * concurrency: several threads holding connections from one data source,
 * and several threads on one connection, which JDBC allows and which pools
 * produce between validation and hand-out. What
 * the caches a connection keeps promise has to hold when more than one caller
 * asks at once, and a promise about an object kept across calls is one only a
 * lock can make.</p>
 *
 * <p>This is not a race detector. Interleavings are what they happen to be on
 * the machine running it, and an answer that is only sometimes wrong may pass.
 * What it does catch is the kind that is reliably wrong under load: results
 * reaching the wrong thread, a cache that answers two callers differently, and
 * anything at all escaping as an unchecked exception.</p>
 */
public class ConcurrencyIT extends BaseIntegrationTest {

    private static final String TABLE = "concurrency";

    private static final int THREADS = 8;

    /** Enough turns that a thread's answer being another thread's is not luck. */
    private static final int ROUNDS = 40;

    @BeforeAll
    static void setUpTable() throws Exception {
        dropAllUserTables();
        try (Connection conn = connect(); Statement statement = conn.createStatement()) {
            statement.execute("create table " + TABLE + " (id integer primary key, owner string) "
                + "clustered into 4 shards with (number_of_replicas = 0)");
            try (PreparedStatement insert = conn.prepareStatement(
                     "insert into " + TABLE + " (id, owner) values (?, ?)")) {
                for (int id = 0; id < THREADS; id++) {
                    insert.setInt(1, id);
                    insert.setString(2, "thread-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            statement.execute("refresh table " + TABLE);
        }
        ensureYellow();
    }

    @AfterAll
    static void dropTable() throws Exception {
        dropAllUserTables();
    }

    /**
     * Each thread reads the row it owns and gets that row. A pool hands out a
     * connection to one thread at a time, so a thread seeing another's answer
     * would mean the driver carrying state across a connection it should not.
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    public void everyThreadBorrowingFromAPoolReadsItsOwnRows() throws Exception {
        try (HikariDataSource pool = pool(THREADS / 2)) {
            List<String> wrong = inParallel(thread -> {
                List<String> mistakes = new ArrayList<>();
                for (int round = 0; round < ROUNDS; round++) {
                    try (Connection conn = pool.getConnection();
                         PreparedStatement statement = conn.prepareStatement(
                             "select owner from " + TABLE + " where id = ?")) {
                        statement.setInt(1, thread);
                        try (ResultSet rows = statement.executeQuery()) {
                            rows.next();
                            String owner = rows.getString(1);
                            if (!("thread-" + thread).equals(owner)) {
                                mistakes.add("thread " + thread + " read " + owner);
                            }
                        }
                    }
                }
                return mistakes;
            });
            assertThat(String.join("\n  ", wrong), wrong, is(empty()));
        }
    }

    /**
     * A connection's cached answers are one answer, however many callers ask
     * at once. {@code getMetaData} and {@code getCrateVersion} each read the
     * server once and keep what came back, and {@code MetaDataIT} pins the
     * single object a lone caller is given — a promise that only holds if the
     * check and the assignment cannot be split.
     *
     * <p>Callers are released together rather than started together, because
     * a race that has to be lost in the window between two statements is not
     * one that starting eight threads will find on its own.</p>
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    public void everyThreadSharingAConnectionGetsTheOneCachedAnswer() throws Exception {
        for (int attempt = 0; attempt < 20; attempt++) {
            try (Connection conn = connect()) {
                CrateConnection crate = conn.unwrap(CrateConnection.class);
                CountDownLatch start = new CountDownLatch(1);
                List<String> answers = Collections.synchronizedList(new ArrayList<>());
                List<String> failures = inParallel(thread -> {
                    start.await();
                    answers.add(System.identityHashCode(conn.getMetaData())
                        + " " + System.identityHashCode(crate.getCrateVersion())
                        + " " + conn.getMetaData().getDatabaseProductName());
                    return Collections.<String>emptyList();
                }, start);
                assertThat(String.join("\n  ", failures), failures, is(empty()));
                Set<String> distinct = new LinkedHashSet<>(answers);
                assertThat("callers arriving together were handed " + distinct.size()
                        + " different answers where the connection keeps one: " + distinct,
                    distinct.size(), is(1));
            }
        }
    }

    /**
     * Nothing escapes a shared connection as an unchecked exception. A caller
     * can catch a {@link SQLException} and decide what to do; a state left
     * half-changed by two threads reaches it as a crash it cannot.
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    public void nothingEscapesASharedConnectionUnchecked() throws Exception {
        try (Connection conn = connect()) {
            List<String> failures = inParallel(thread -> {
                List<String> unchecked = new ArrayList<>();
                for (int round = 0; round < ROUNDS; round++) {
                    try (Statement statement = conn.createStatement();
                         ResultSet rows = statement.executeQuery(
                             "select owner from " + TABLE + " where id = " + thread)) {
                        rows.next();
                        rows.getString(1);
                        conn.getMetaData().getDatabaseProductName();
                    } catch (SQLException expected) {
                        // Two threads on one connection get in each other's
                        // way, and being told so is the contract.
                    } catch (RuntimeException crash) {
                        unchecked.add("thread " + thread + " round " + round + ": " + crash);
                    }
                }
                return unchecked;
            });
            assertThat(String.join("\n  ", failures), failures, is(empty()));
        }
    }

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

    @FunctionalInterface
    private interface Work {
        List<String> run(int thread) throws Exception;
    }

    private static List<String> inParallel(Work work) throws Exception {
        return inParallel(work, null);
    }

    /**
     * Runs the work on every thread and collects what each has to complain
     * about. A latch, where one is given, is released once every thread is
     * waiting on it, so that they arrive together rather than in the order
     * they were started.
     */
    private static List<String> inParallel(Work work, CountDownLatch start) throws Exception {
        ExecutorService threads = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<List<String>>> running = new ArrayList<>();
            for (int thread = 0; thread < THREADS; thread++) {
                int index = thread;
                running.add(threads.submit((Callable<List<String>>) () -> work.run(index)));
            }
            if (start != null) {
                start.countDown();
            }
            List<String> complaints = new ArrayList<>();
            for (Future<List<String>> future : running) {
                try {
                    complaints.addAll(future.get(2, TimeUnit.MINUTES));
                } catch (java.util.concurrent.ExecutionException e) {
                    complaints.add(String.valueOf(e.getCause()));
                }
            }
            return complaints;
        } finally {
            threads.shutdownNow();
        }
    }
}
