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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import io.crate.testing.CrateTestCluster;
import io.crate.testing.CrateTestServer;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
public abstract class BaseIntegrationTest extends RandomizedTest {

    private static final String[] CRATE_VERSIONS = new String[] {
            "4.8.4",
            "5.3.0",
    };

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static CrateTestCluster TEST_CLUSTER;

    private static String getRandomServerVersion() {
        String version = System.getenv().get("CRATE_VERSION");
        if (version != null) {
            return version;
        }
        Random random = getRandom();
        return CRATE_VERSIONS[random.nextInt(CRATE_VERSIONS.length)];
    }

    @BeforeClass
    public static void setUpCluster() throws Throwable {
        String downloadUrl = System.getenv().get("CRATE_URL");
        CrateTestCluster.Builder builder;
        if (downloadUrl != null) {
            builder = CrateTestCluster.fromURL(downloadUrl);
        } else {
            String filePath = System.getenv().get("CRATE_PATH");
            if (filePath != null) {
                builder = CrateTestCluster.fromFile(filePath);
            } else {
                String versionNumber = System.getenv().get("CRATE_VERSION");
                if (versionNumber != null) {
                    builder = CrateTestCluster.fromVersion(versionNumber);
                } else {
                    builder = CrateTestCluster.fromVersion(getRandomServerVersion());
                }
            }
        }
        TEST_CLUSTER = builder.keepWorkingDir(false).build();
        TEST_CLUSTER.before();
    }

    @AfterClass
    public static void tearDownCluster() {
        TEST_CLUSTER.after();
    }

    @Before
    public void setUp() throws Exception {
        setUpTestTable();
    }

    @After
    public void tearDown() {
        tearDownTables();
    }

    static String getConnectionString() {
        CrateTestServer server = TEST_CLUSTER.randomServer();
        return String.format("crate://%s:%s/doc?user=crate", server.crateHost(), server.psqlPort());
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
