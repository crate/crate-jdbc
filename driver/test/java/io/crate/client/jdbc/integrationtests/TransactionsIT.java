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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the transaction surface of the crate:// driver. CrateDB has no
 * transactions: BEGIN and COMMIT parse as server-side no-ops, ROLLBACK is
 * not in its grammar at all. The driver therefore treats
 * {@link Connection#rollback()} as a silent client-side no-op so frameworks
 * that call it during routine cleanup keep working, while savepoint
 * rollback is reported as unsupported.
 */
public class TransactionsIT extends BaseIntegrationTest {

    @BeforeEach
    void setUpTables() throws Exception {
        dropAllUserTables();
        setUpTestTable();
    }

    @AfterEach
    void tearDownTables() {
        dropAllUserTables();
    }

    @Test
    public void rollbackIsNoOpAndAllWritesPersist() throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.execute("insert into test (id, string_field) values (50, 'committed')");
            conn.commit();
            stmt.execute("insert into test (id, string_field) values (51, 'rolled-back')");
            conn.rollback();
            conn.setAutoCommit(true);
            conn.createStatement().execute("refresh table test");
            ResultSet rs = conn.createStatement().executeQuery(
                "select count(*) from test where id in (50, 51)");
            assertTrue(rs.next());
            assertThat(rs.getLong(1), is(2L));
        }
    }

    @Test
    public void commitSucceedsWithAutoCommitDisabled() throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            conn.createStatement().execute("insert into test (id, string_field) values (60, 'committed')");
            conn.commit();
            conn.setAutoCommit(true);
            conn.createStatement().execute("refresh table test");
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from test where id = 60");
            assertTrue(rs.next());
            assertThat(rs.getLong(1), is(1L));
        }
    }

    @Test
    public void rollbackToSavepointIsUnsupported() throws Exception {
        try (Connection conn = connect()) {
            Savepoint savepoint = new Savepoint() {
                @Override
                public int getSavepointId() {
                    return 1;
                }

                @Override
                public String getSavepointName() {
                    return "sp";
                }
            };
            assertThrows(SQLFeatureNotSupportedException.class, () -> conn.rollback(savepoint));
        }
    }

    @Test
    public void autoCommitCanBeToggled() throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            assertThat(conn.getAutoCommit(), is(false));
            conn.setAutoCommit(true);
            assertThat(conn.getAutoCommit(), is(true));
        }
    }
}
