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

package io.crate.client.jdbc.integrationtests;

import org.testcontainers.cratedb.CrateDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Boots one CrateDB per test JVM and connects to it through the crate://
 * scheme, i.e. through {@link io.crate.client.jdbc.CrateDriver}.
 *
 * <p>The server version comes from the {@code CRATEDB_VERSION} environment
 * variable (a tag of the {@code crate} Docker image), defaulting to a
 * recent release. An externally managed server can be used instead by
 * setting {@code CRATE_URL} to a full JDBC URL, in which case no container
 * is started.</p>
 */
public abstract class BaseIntegrationTest {

    private static final String DEFAULT_CRATEDB_VERSION = "6.2.2";

    private static CrateDBContainer container;
    private static String connectionUrl;

    static synchronized String connectionUrl() {
        if (connectionUrl == null) {
            String externalUrl = System.getenv("CRATE_URL");
            if (externalUrl != null) {
                connectionUrl = externalUrl;
            } else {
                String imageName = System.getenv("CRATEDB_IMAGE");
                if (imageName == null) {
                    imageName = "crate:" + System.getenv().getOrDefault("CRATEDB_VERSION", DEFAULT_CRATEDB_VERSION);
                }
                DockerImageName image = DockerImageName.parse(imageName)
                    .asCompatibleSubstituteFor("crate");
                container = new CrateDBContainer(image);
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

    protected static void dropAllUserTables() {
        try (Connection conn = connect()) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT table_schema, table_name FROM information_schema.tables " +
                "WHERE table_schema NOT IN ('pg_catalog', 'sys', 'information_schema', 'blob')");
            while (rs.next()) {
                conn.createStatement().execute(String.format(
                    "DROP TABLE IF EXISTS \"%s\".\"%s\"", rs.getString(1), rs.getString(2)));
            }
        } catch (SQLException ignored) {
        }
    }

    /**
     * Creates the shared {@code test} table covering every scalar CrateDB
     * data type plus OBJECT, geo_point and geo_shape.
     */
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
        while (countUnassignedShards() > 0) {
            Thread.sleep(100);
        }
    }

    private static long countUnassignedShards() throws SQLException {
        try (Connection conn = connect()) {
            ResultSet rs = conn.createStatement()
                .executeQuery("SELECT count(*) FROM sys.shards WHERE state != 'STARTED'");
            rs.next();
            return rs.getLong(1);
        }
    }
}
