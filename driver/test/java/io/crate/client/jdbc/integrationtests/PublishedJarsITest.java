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
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.client.jdbc.integrationtests;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.ServiceLoader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublishedJarsITest extends BaseIntegrationTest {

    @Test
    public void testCrateJdbcJarConnectsWithItsPomDependencies() throws Exception {
        assertCanQuery(System.getProperty("jdbc.jar.classpath"));
    }

    @Test
    public void testStandaloneJarConnectsWithoutOtherDependencies() throws Exception {
        assertCanQuery(System.getProperty("jdbc.standaloneJar"));
    }

    private static void assertCanQuery(String classpath) throws Exception {
        assertNotNull(classpath);
        String url = getConnectionString();
        String[] paths = classpath.split(File.pathSeparator);
        URL[] urls = new URL[paths.length];
        for (int i = 0; i < paths.length; i++) {
            urls[i] = new File(paths[i]).toURI().toURL();
        }
        try (URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            Driver driver = null;
            for (Driver candidate : ServiceLoader.load(Driver.class, loader)) {
                if (candidate.acceptsURL(url)) {
                    driver = candidate;
                    break;
                }
            }
            assertNotNull(driver, "no java.sql.Driver accepts " + url);
            try (Connection connection = driver.connect(url, new Properties());
                 ResultSet rs = connection.createStatement().executeQuery("select 1")) {
                assertTrue(rs.next());
                assertThat(rs.getInt(1), is(1));
            }
        }
    }
}
