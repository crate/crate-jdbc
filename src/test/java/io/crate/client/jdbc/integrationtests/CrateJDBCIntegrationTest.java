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

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CrateJDBCIntegrationTest {

    public static final String CRATE_SERVER_VERSION = clientVersion();

    private static String clientVersion() {
        String cp = System.getProperty("java.class.path");
        Matcher m = Pattern.compile("crate-client-([\\d\\.]{5,})\\.jar").matcher(cp);

        if (m.find()) {
            String version = m.group(1);
            return version;
        }
        throw new RuntimeException("unable to get version of crate-client");
    }

    @After
    public void tearDown() throws Exception {
        /**
         * Check if no Crate client threads are left over ...
         */
        List<Thread> threads = new ArrayList<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith("elasticsearch[crate-client-")) {
                threads.add(thread);
            }
        }
        if (threads.size() > 0) {
            System.err.printf("%d thread(s) from Crate clients where left over!\n" +
                    "This may indicate a thread leak if JDBC connections should be closed at the end of a test method.\n",
                    threads.size());
            for (Thread th : threads) {
                System.err.printf("%s\n", th.toString());
            }
        }
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

}
