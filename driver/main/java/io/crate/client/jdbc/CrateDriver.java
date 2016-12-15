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

import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CrateDriver extends Driver {

    private static final String PROTOCOL = "jdbc";
    private static final String PSQL_PROTOCOL = "postgresql";
    private static final String CRATE_PROTOCOL = "crate";
    private static final String PREFIX = CRATE_PROTOCOL + ":" + "//";
    private static final String LONG_PREFIX = PROTOCOL + ":" + CRATE_PROTOCOL + ":" + "//";

    static {
        try {
            Driver.deregister();
            assert !isRegistered() : "The PostgreSQL driver is registered.";
            DriverManager.registerDriver(new CrateDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public CrateDriver() {
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url.startsWith(PREFIX)) {
            url = String.format("%s:%s", PROTOCOL, url);
        } else if (!url.startsWith(LONG_PREFIX)) {
            return null;
        }
        url = url.replace(CRATE_PROTOCOL, PSQL_PROTOCOL);
        return super.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(PREFIX) || url.startsWith(LONG_PREFIX);
    }

    @Override
    public int getMajorVersion() {
        return CrateDriverVersion.CURRENT.major;
    }

    @Override
    public int getMinorVersion() {
        return CrateDriverVersion.CURRENT.minor;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }
}
