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

package io.crate.client.jdbc;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class CrateDriverVersionTest {

    /**
     * The version the driver answers with is the one the artifact was built
     * as, whole and split into the parts {@code java.sql.Driver} asks for, so
     * that a release cannot ship a driver misreporting itself.
     */
    @Test
    public void reportedVersionMatchesTheVersionBeingBuilt() {
        CrateDriverVersion version = CrateDriverVersion.CURRENT;

        assertThat(version.toString(), is(System.getProperty("project.version")));
        assertThat(version.toString(), startsWith(version.major + "." + version.minor + "."));
    }
}
