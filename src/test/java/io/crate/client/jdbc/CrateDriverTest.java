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

import io.crate.client.AbstractIntegrationTest;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class CrateDriverTest extends AbstractIntegrationTest {

    @Test
    public void testDriverRegistration() throws Exception {
        try {
            DriverManager.getConnection("crate://localhost:44300");
            fail("expected SQLException");
        } catch (SQLException e) {
        }

        Class.forName("io.crate.client.jdbc.CrateDriver");
        Connection c = DriverManager.getConnection("crate://localhost:44300");
        assertThat(c, instanceOf(CrateConnection.class));
    }

}
