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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrateQueryTimeoutTest {

    /**
     * Every unit CrateDB prints an interval in, including the fractional forms
     * it will not parse back — reading {@code statement_timeout} has to yield a
     * number the server accepts, whatever spelling it came in.
     */
    @ParameterizedTest
    @CsvSource({
        "0s, 0",
        "0, 0",
        "500ms, 500",
        "2.5s, 2500",
        "3s, 3000",
        "1.5m, 90000",
        "1h, 3600000",
        "1.5h, 5400000",
        "2d, 172800000",
        // Units ending in another are read whole rather than as their tail.
        "500micros, 1",
        "1500micros, 1",
        "200nanos, 1",
    })
    public void aTimeoutSettingReadsAsMilliseconds(String setting, long millis) throws SQLException {
        assertThat(CrateQueryTimeout.millisOf(setting), is(millis));
    }

    /**
     * The server prints this setting in a vocabulary of its own, so a spelling
     * outside it is a thing to report. Reading one is the first step of every
     * execution that carries a timeout, and a failure there reaches the
     * application as whatever it was raised as.
     */
    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "   ", "forever", "ms", "0x10ms", "--3s", "1,5m"})
    public void aTimeoutSettingInNoKnownFormIsRefused(String setting) {
        SQLException refused = assertThrows(SQLException.class,
            () -> CrateQueryTimeout.millisOf(setting));

        assertThat(refused.getMessage(), containsString("statement_timeout"));
    }
}
