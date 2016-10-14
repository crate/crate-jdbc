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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import io.crate.testing.CrateTestCluster;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.HashMap;


@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
public class CrateJDBCIntegrationTest extends RandomizedTest {

    private static final String CRATE_SERVER_VERSION = "0.56.1";
    static final String PSQL_PORT = "5432";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static CrateTestCluster testCluster = CrateTestCluster
        .fromVersion(CRATE_SERVER_VERSION)
        .keepWorkingDir(false)
        .settings(new HashMap<String, Object>() {{
            put("psql.port", PSQL_PORT);
            put("psql.enabled", true);
        }})
        .build();
}
