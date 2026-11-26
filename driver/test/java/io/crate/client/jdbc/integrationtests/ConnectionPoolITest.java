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
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ConnectionPoolITest extends BaseIntegrationTest {

    @Test
    public void testConnectionReset() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(getConnectionString());
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(1);
        try (HikariDataSource pool = new HikariDataSource(config)) {
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
}
