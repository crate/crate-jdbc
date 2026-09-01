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

import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The version of the CrateDB server behind a connection.
 * {@code DatabaseMetaData.getDatabaseProductVersion()} reports the PostgreSQL
 * release CrateDB emulates on the wire instead, that being the version pgJDBC
 * and PostgreSQL tooling reason about.
 */
public final class CrateVersion {

    /** The server reports itself as, for example, "CrateDB 6.4.1 (built ...)". */
    private static final Pattern VERSION = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

    private final String text;
    private final int major;
    private final int minor;
    private final int patch;

    CrateVersion(String text) throws SQLException {
        Matcher matcher = VERSION.matcher(text);
        if (!matcher.find()) {
            throw new PSQLException("Cannot read the CrateDB version from: " + text,
                PSQLState.DATA_ERROR);
        }
        this.text = matcher.group();
        this.major = Integer.parseInt(matcher.group(1));
        this.minor = Integer.parseInt(matcher.group(2));
        this.patch = Integer.parseInt(matcher.group(3));
    }

    public int major() {
        return major;
    }

    public int minor() {
        return minor;
    }

    public int patch() {
        return patch;
    }

    public boolean atLeast(int major, int minor) {
        return this.major > major || (this.major == major && this.minor >= minor);
    }

    @Override
    public String toString() {
        return text;
    }
}
