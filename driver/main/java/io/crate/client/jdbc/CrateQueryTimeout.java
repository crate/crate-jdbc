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
package io.crate.client.jdbc;

import org.postgresql.util.PSQLState;

import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A statement's query timeout, held as CrateDB's {@code statement_timeout} for
 * the length of one execution and given back afterwards.
 *
 * <p>pgJDBC delivers a timeout as a cancel request on a second connection,
 * which reaches the session only when the URL names the nodes themselves.
 * {@code statement_timeout} travels on the connection already holding the
 * session, so it survives a load balancer in front of the cluster. Both stay
 * in play; {@code docs/internals.rst} covers what each one reaches.</p>
 *
 * <p>The setting belongs to the session while the timeout belongs to one
 * statement, so holding it costs three round trips around an execution: the
 * session's value is read, replaced, and put back. A statement that sets no
 * timeout pays none of it.</p>
 */
@FunctionalInterface
interface CrateQueryTimeout extends AutoCloseable {

    CrateQueryTimeout NONE = () -> {
    };

    @Override
    void close() throws SQLException;

    /** The session's current {@code statement_timeout}, in milliseconds. */
    static long readMillis(Statement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery(
                "select setting from pg_settings where name = 'statement_timeout'")) {
            return rs.next() ? millisOf(rs.getString(1)) : 0;
        }
    }

    static void applyMillis(Statement statement, long millis) throws SQLException {
        statement.execute("set statement_timeout = '" + millis + "ms'");
    }

    /**
     * Milliseconds per unit of the interval {@code pg_settings} reports the
     * setting in. Order matters: {@code micros}, {@code nanos} and {@code ms}
     * all end in {@code s}, so each unit is tried before any unit it ends with
     * and a suffix is read whole instead of as its own tail.
     */
    Map<String, Double> UNIT_MILLIS = unitMillis();

    private static Map<String, Double> unitMillis() {
        Map<String, Double> units = new LinkedHashMap<>();
        units.put("micros", 0.001);
        units.put("nanos", 0.000_001);
        units.put("ms", 1.0);
        units.put("s", 1_000.0);
        units.put("m", 60 * 1_000.0);
        units.put("h", 3_600 * 1_000.0);
        units.put("d", 86_400 * 1_000.0);
        return Collections.unmodifiableMap(units);
    }

    /**
     * Milliseconds of an interval as CrateDB prints one: a number, possibly
     * carrying a fraction, and a unit ({@code 1.5m}).
     *
     * <p>CrateDB does not read back everything it prints. It renders 90 seconds
     * as {@code 1.5m} and then rejects {@code 1.5m} as an invalid interval, so
     * a value read from the server is converted to milliseconds, the form it
     * always accepts, before being given back.</p>
     *
     * <p>Anything shorter than a millisecond rounds up, since zero would mean
     * no timeout at all. A spelling the units do not cover is reported against
     * the setting, which the number parser's own complaint would not name.</p>
     */
    static long millisOf(String setting) throws SQLException {
        if (setting == null) {
            throw unreadable(null);
        }
        String value = setting.trim();
        for (Map.Entry<String, Double> unit : UNIT_MILLIS.entrySet()) {
            if (!value.endsWith(unit.getKey())) {
                continue;
            }
            double millis = amountOf(setting,
                value.substring(0, value.length() - unit.getKey().length())) * unit.getValue();
            return millis > 0 && millis < 1 ? 1 : (long) millis;
        }
        return (long) amountOf(setting, value);
    }

    private static double amountOf(String setting, String amount) throws SQLException {
        try {
            return Double.parseDouble(amount);
        } catch (NumberFormatException notANumber) {
            throw unreadable(setting);
        }
    }

    private static SQLException unreadable(String setting) {
        return new SQLDataException(
            "Cannot read the statement_timeout the server reports: " + setting,
            PSQLState.DATA_ERROR.getState());
    }
}
