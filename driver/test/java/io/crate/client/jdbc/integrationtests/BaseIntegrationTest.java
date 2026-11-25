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

import io.crate.testing.CrateTestCluster;
import io.crate.testing.CrateTestServer;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseIntegrationTest {

    private static final String DEFAULT_CRATE_VERSION = "6.2.2";

    private static final CrateTestCluster TEST_CLUSTER;

    static {
        String url = System.getenv("CRATE_URL");
        String path = System.getenv("CRATE_PATH");
        CrateTestCluster.Builder builder;
        if (url != null) {
            builder = CrateTestCluster.fromURL(url);
        } else if (path != null) {
            builder = CrateTestCluster.fromFile(path);
        } else {
            builder = CrateTestCluster.fromVersion(System.getenv().getOrDefault("CRATE_VERSION", DEFAULT_CRATE_VERSION));
        }
        TEST_CLUSTER = builder.keepWorkingDir(false).build();
        try {
            TEST_CLUSTER.before();
        } catch (Throwable e) {
            throw new ExceptionInInitializerError(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(TEST_CLUSTER::after));
    }

    @BeforeEach
    public void setUp() throws Exception {
        setUpTestTable();
    }

    @AfterEach
    public void tearDown() {
        tearDownTables();
    }

    static String crateHost() {
        return server().crateHost();
    }

    static int psqlPort() {
        return server().psqlPort();
    }

    private static CrateTestServer server() {
        return TEST_CLUSTER.randomServer();
    }

    static String getConnectionString() {
        return String.format("crate://%s:%s/doc?user=crate", crateHost(), psqlPort());
    }

    private static void tearDownTables() {
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT table_schema, table_name " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema not in ('pg_catalog', 'sys', 'information_schema', 'blob')"
                );
            while (rs.next()) {
                conn.createStatement().execute(String.format(
                    "DROP TABLE IF EXISTS \"%s\".\"%s\"", rs.getString("table_schema"), rs.getString("table_name")
                ));
            }
        } catch (Exception ignore) {
        }
    }

    private static void setUpTestTable() throws SQLException, InterruptedException {
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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

    static void insertIntoTestTable() throws SQLException {
        Map<String, Object> objectField = new HashMap<String, Object>() {{
            put("inner", "Zoon");
        }};
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
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

    static void ensureYellow() throws SQLException, InterruptedException {
        while (countUnassigned() > 0) {
            Thread.sleep(100);
        }
    }

    private static Long countUnassigned() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getConnectionString())) {
            ResultSet rs = conn.createStatement()
                .executeQuery("SELECT count(*) FROM sys.shards WHERE state != 'STARTED'");
            rs.next();
            return rs.getLong(1);
        }
    }
}
