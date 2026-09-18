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

import org.postgresql.jdbc.CrateVersion;
import org.postgresql.jdbc.PgDatabaseMetaData;
import org.testcontainers.cratedb.CrateDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class BaseIntegrationTest {

    private static final String DEFAULT_CRATEDB_VERSION = "6.4.1";
    private static final Duration SHARD_ALLOCATION_TIMEOUT = Duration.ofMinutes(2);

    private static CrateDBContainer container;
    private static String connectionUrl;
    private static CrateVersion serverVersion;

    static DockerImageName serverImage() {
        String imageName = System.getenv("CRATEDB_IMAGE");
        if (imageName == null) {
            imageName = "crate:" + System.getenv().getOrDefault("CRATEDB_VERSION", DEFAULT_CRATEDB_VERSION);
        }
        return DockerImageName.parse(imageName).asCompatibleSubstituteFor("crate");
    }

    static synchronized String connectionUrl() {
        if (connectionUrl == null) {
            String externalUrl = System.getenv("CRATE_URL");
            if (externalUrl != null) {
                connectionUrl = externalUrl;
            } else {
                container = new CrateDBContainer(serverImage());
                container.start();
                connectionUrl = String.format(
                    "crate://%s:%d/doc?user=crate", container.getHost(), container.getMappedPort(5432));
            }
        }
        return connectionUrl;
    }

    protected static Connection connect() throws SQLException {
        return DriverManager.getConnection(connectionUrl());
    }

    protected static Connection connectWith(String... properties) throws SQLException {
        if (properties.length % 2 != 0) {
            throw new IllegalArgumentException("Connection properties come in name and value pairs");
        }
        Properties props = new Properties();
        for (int i = 0; i < properties.length; i += 2) {
            props.setProperty(properties[i], properties[i + 1]);
        }
        return DriverManager.getConnection(connectionUrl(), props);
    }

    // URI cannot parse a URL naming several hosts, so take the first one.
    protected static URI serverAddress() {
        String url = connectionUrl();
        String withoutScheme = url.substring(url.indexOf("://") + "://".length());
        int schemaSeparator = withoutScheme.indexOf('/');
        String hosts = withoutScheme.substring(0, schemaSeparator);
        int nextHost = hosts.indexOf(',');
        return URI.create("crate://" + (nextHost < 0 ? hosts : hosts.substring(0, nextHost))
            + withoutScheme.substring(schemaSeparator));
    }

    protected static synchronized boolean serverAtLeast(int major, int minor) {
        if (serverVersion == null) {
            try (Connection conn = connect()) {
                serverVersion = conn.getMetaData().unwrap(PgDatabaseMetaData.class).getCrateVersion();
            } catch (SQLException e) {
                throw new IllegalStateException("Cannot read the CrateDB version under test", e);
            }
        }
        return !serverVersion.before(major + "." + minor);
    }

    protected static void dropAllUserTables() {
        try (Connection conn = connect()) {
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT table_schema, table_name FROM information_schema.tables " +
                "WHERE table_schema NOT IN ('pg_catalog', 'sys', 'information_schema', 'blob')")) {
                while (rs.next()) {
                    tables.add(String.format("\"%s\".\"%s\"", rs.getString(1), rs.getString(2)));
                }
            }
            try (Statement statement = conn.createStatement()) {
                for (String table : tables) {
                    statement.execute("DROP TABLE IF EXISTS " + table);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot drop the tables left by a previous test", e);
        }
    }

    protected static void setUpTestTable() throws SQLException, InterruptedException {
        try (Connection conn = connect()) {
            conn.createStatement().execute(
                "create table if not exists test (" +
                " id integer primary key," +
                " string_field string," +
                " boolean_field boolean," +
                " byte_field byte," +
                " short_field short," +
                " integer_field integer," +
                " long_field long," +
                " float_field float," +
                " double_field double," +
                " timestamp_field timestamp," +
                " object_field object as (\"inner\" string)," +
                " ip_field ip," +
                " geo_point_field geo_point," +
                " geo_shape_field geo_shape" +
                ") clustered by (id) into 1 shards with (number_of_replicas=0)");
        }
        ensureYellow();
    }

    protected static void insertIntoTestTable() throws SQLException {
        Map<String, Object> objectField = new HashMap<>();
        objectField.put("inner", "Zoon");
        try (Connection conn = connect()) {
            PreparedStatement preparedStatement =
                conn.prepareStatement("insert into test (id, string_field, boolean_field, byte_field, " +
                                      "short_field, integer_field, long_field, float_field, double_field, object_field, " +
                                      "timestamp_field, ip_field, geo_point_field, geo_shape_field) values " +
                                      "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "Youri");
            preparedStatement.setBoolean(3, true);
            preparedStatement.setByte(4, (byte) 120);
            preparedStatement.setShort(5, (short) 1000);
            preparedStatement.setInt(6, 1200000);
            preparedStatement.setLong(7, 120000000000L);
            preparedStatement.setFloat(8, 1.4f);
            preparedStatement.setDouble(9, 3.456789);
            preparedStatement.setObject(10, objectField);
            preparedStatement.setTimestamp(11, new Timestamp(1000L));
            preparedStatement.setString(12, "127.0.0.1");
            preparedStatement.setArray(13, conn.createArrayOf("double", new Double[]{9.7419021d, 47.4048045d}));
            preparedStatement.setString(14, "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
            preparedStatement.execute();
            conn.createStatement().execute("refresh table test");
        }
    }

    protected static void ensureYellow() throws SQLException, InterruptedException {
        long deadline = System.nanoTime() + SHARD_ALLOCATION_TIMEOUT.toNanos();
        try (Connection conn = connect();
             Statement statement = conn.createStatement()) {
            while (countUnassignedShards(statement) > 0) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(
                        "Shards were still unassigned after " + SHARD_ALLOCATION_TIMEOUT);
                }
                Thread.sleep(100);
            }
        }
    }

    private static long countUnassignedShards(Statement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery(
            "SELECT count(*) FROM sys.shards WHERE state != 'STARTED'")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
