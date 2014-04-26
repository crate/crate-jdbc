/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.client;

import io.crate.action.sql.SQLResponse;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CrateClientTest extends AbstractIntegrationTest {

    @Test
    public void testCreateClient() throws Exception {
        CrateClient client = new CrateClient("localhost:" +  transportPort);

        SQLResponse r = client.sql("create table test (id int)").actionGet();
        assertEquals(1, r.rowCount());

        r = client.sql("insert into test values (1)").actionGet();
        assertEquals(1, r.rowCount());

        r = client.sql("refresh table test").actionGet();
        assertEquals(-1, r.rowCount());

        r = client.sql("select id from test").actionGet();

        assertEquals(1, r.rows().length);
        assertEquals("id", r.cols()[0]);
        assertEquals(1, r.rows()[0][0]);
    }
}
