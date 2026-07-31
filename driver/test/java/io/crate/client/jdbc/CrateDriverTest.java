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

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class CrateDriverTest {

    @Test
    public void processUrlRewritesCrateSchemes() {
        assertThat(CrateDriver.processURL("crate://localhost:5432/"), is("jdbc:postgresql://localhost:5432/"));
        assertThat(CrateDriver.processURL("jdbc:crate://localhost:5432/"), is("jdbc:postgresql://localhost:5432/"));
        assertThat(CrateDriver.processURL("crate://crate1.local:5432/"), is("jdbc:postgresql://crate1.local:5432/"));
        assertThat(CrateDriver.processURL("jdbc:crate://crate1.local:5432/"), is("jdbc:postgresql://crate1.local:5432/"));
        assertThat(CrateDriver.processURL("crate://h:1"), is("jdbc:postgresql://h:1"));
        assertThat(CrateDriver.processURL("jdbc:crate://h:1"), is("jdbc:postgresql://h:1"));
    }

    @Test
    public void processUrlIsCaseInsensitiveOnTheScheme() {
        assertThat(CrateDriver.processURL("CRATE://h:1"), is("jdbc:postgresql://h:1"));
        assertThat(CrateDriver.processURL("JDBC:CRATE://h:1"), is("jdbc:postgresql://h:1"));
        assertThat(CrateDriver.processURL("Crate://h:1"), is("jdbc:postgresql://h:1"));
    }

    @Test
    public void processUrlRewritesOnlyTheLeadingScheme() {
        assertThat(CrateDriver.processURL("crate://h:1/doc?fallback=jdbc:crate://other:2"),
            is("jdbc:postgresql://h:1/doc?fallback=jdbc:crate://other:2"));
        assertThat(CrateDriver.processURL("jdbc:crate://h:1/doc?fallback=jdbc:crate://other:2"),
            is("jdbc:postgresql://h:1/doc?fallback=jdbc:crate://other:2"));
    }

    @Test
    public void processUrlRejectsForeignSchemes() {
        assertThat(CrateDriver.processURL("postgres://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("jdbc:postgresql://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("jdbc://postgres://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("foo://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("foo://h/?u=jdbc:crate://h:1"), nullValue());
    }

    @Test
    public void processUrlAcceptsBareScheme() {
        assertThat(CrateDriver.processURL("crate://"), is("jdbc:postgresql://"));
        assertThat(CrateDriver.processURL("jdbc:crate://"), is("jdbc:postgresql://"));
    }

    @Test
    public void acceptsUrlForBothPrefixes() {
        CrateDriver driver = new CrateDriver();

        assertThat(driver.acceptsURL("crate://"), is(true));
        assertThat(driver.acceptsURL("crate://localhost/foo"), is(true));
        assertThat(driver.acceptsURL("crate:///foo"), is(true));
        assertThat(driver.acceptsURL("jdbc:crate://"), is(true));
        assertThat(driver.acceptsURL("CRATE://"), is(true));
        assertThat(driver.acceptsURL("JDBC:CRATE://"), is(true));

        assertThat(driver.acceptsURL("cr8://"), is(false));
        assertThat(driver.acceptsURL("mysql://"), is(false));
        assertThat(driver.acceptsURL("jdbc:mysql://"), is(false));
        assertThat(driver.acceptsURL("postgres://"), is(false));
        assertThat(driver.acceptsURL("jdbc:postgres://"), is(false));
        assertThat(driver.acceptsURL("jdbc:postgresql://"), is(false));
    }
}
