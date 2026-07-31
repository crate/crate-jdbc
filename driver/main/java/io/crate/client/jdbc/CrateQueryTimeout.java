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
 * <p>A JDBC query timeout otherwise travels only as a PostgreSQL cancel
 * request, which the driver sends on a second connection to the host the
 * session is on. That reaches the right node when the URL names the nodes
 * themselves, and any node of the cluster when the one name it resolves is a
 * load balancer in front of them — the cancel carries no routing of its own,
 * so a request that lands elsewhere finds no session to cancel.
 * {@code statement_timeout} travels with the statement instead, on the
 * connection already holding the session, and the server schedules the abort
 * itself.</p>
 *
 * <p>Both are left in place, because each covers a case the other does not.
 * Neither covers a query CrateDB answers inline, without handing it to its
 * execution pool — over {@code sys} tables and table functions. The server
 * never reaches the point where it arms {@code statement_timeout}, and the
 * thread that would read a cancel request is the one running the query. Such
 * a query has to be bounded in its own text, with a {@code LIMIT} or a
 * narrower filter.</p>
 *
 * <p>What is bounded is the execution, not the reading of its rows. A
 * statement with a fetch size leaves a cursor open under manual commit mode,
 * and the fetches that bring the remaining batches run after the setting has
 * been given back — so they carry no timeout, and the same {@code LIMIT} is
 * what bounds them.</p>
 *
 * <p>Holding the setting costs three round trips around every execution — the
 * session's value is read, replaced, and put back — because the setting
 * belongs to the session while the timeout belongs to one statement. A
 * statement that sets no timeout pays none of it.</p>
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
     * setting in. Every unit ending in another is tried before the one it ends
     * with — {@code micros}, {@code nanos} and {@code ms} all end in {@code s}
     * — so that a suffix is read whole rather than as its own tail.
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
     * Milliseconds of an interval as CrateDB prints one — a number and a unit,
     * where the number may carry a fraction ({@code 1.5m}).
     *
     * <p>The printed form is not always one CrateDB accepts back: it prints 90
     * seconds as {@code 1.5m} and rejects {@code 1.5m} as an invalid interval.
     * Milliseconds are the form it always takes, so a value read from the
     * server is converted before it is given back to it.</p>
     *
     * <p>Anything shorter than a millisecond rounds up rather than down: zero
     * is not a shorter timeout but no timeout at all.</p>
     *
     * <p>A form the units do not cover is refused rather than left to the
     * number parser, whose complaint names neither the setting nor what was
     * being done with it. The server prints this text in a vocabulary of its
     * own — one it does not read back — so a spelling this does not know is a
     * thing to report, not a thing to crash on.</p>
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
