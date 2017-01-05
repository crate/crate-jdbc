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

import org.junit.Test;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CrateJDBCDriverTest {

    @Test
    public void testProcessUrl() throws Exception {
        assertThat(CrateDriver.processURL("crate://localhost:5432/"), is("jdbc:postgresql://localhost:5432/"));
        assertThat(CrateDriver.processURL("jdbc:crate://localhost:5432/"), is("jdbc:postgresql://localhost:5432/"));
        assertThat(CrateDriver.processURL("postgres://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("jdbc://postgres://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("foo://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("crate://crate1.local:5432/"), is("jdbc:postgresql://crate1.local:5432/"));
        assertThat(CrateDriver.processURL("jdbc:crate://crate1.local:5432/"), is("jdbc:postgresql://crate1.local:5432/"));
    }

    @Test
    public void testAccepts() throws Exception {
        CrateDriver driver = new CrateDriver();

        assertThat(driver.acceptsURL("crate://"), is(true));
        assertThat(driver.acceptsURL("crate://localhost/foo"), is(true));
        assertThat(driver.acceptsURL("crate:///foo"), is(true));
        assertThat(driver.acceptsURL("jdbc:crate://"), is(true));

        assertThat(driver.acceptsURL("cr8://"), is(false));
        assertThat(driver.acceptsURL("mysql://"), is(false));
        assertThat(driver.acceptsURL("jdbc:mysql://"), is(false));
        assertThat(driver.acceptsURL("postgres://"), is(false));
        assertThat(driver.acceptsURL("jdbc:postgres://"), is(false));
    }

}
