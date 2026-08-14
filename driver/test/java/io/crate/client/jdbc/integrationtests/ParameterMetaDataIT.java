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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What a statement answers about the values it expects, which a caller reads
 * to decide what to bind — connection pools and query builders do.
 *
 * <p>CrateDB sends OBJECT, geo_shape and nested-array parameters as json, and
 * this driver takes those from a {@code Map} or a {@code List}. pgJDBC, which
 * would want its own {@code PGobject} for them, names that class; the class an
 * application here actually binds is what the driver names instead.</p>
 */
@Tag("pgjdbc-types")
public class ParameterMetaDataIT extends BaseIntegrationTest {

    private static Connection conn;

    private static final String INSERT =
        "insert into param_meta (id, name, obj, shape, nested) values (?, ?, ?, ?, ?)";

    @BeforeAll
    static void setUpTable() throws Exception {
        dropAllUserTables();
        conn = connect();
        try (Statement statement = conn.createStatement()) {
            statement.execute(
                "create table param_meta ("
                + " id integer,"
                + " name text,"
                + " obj object as (n integer),"
                + " shape geo_shape,"
                + " nested array(array(integer))"
                + ") clustered into 1 shards with (number_of_replicas=0)");
        }
        ensureYellow();
    }

    @AfterAll
    static void dropTable() throws Exception {
        if (conn != null) {
            conn.close();
        }
        dropAllUserTables();
    }

    private static ParameterMetaData parametersOf(String sql) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            return statement.getParameterMetaData();
        }
    }

    /**
     * A parameter CrateDB has a PostgreSQL type for is described by pgJDBC,
     * which the driver has no reason to improve on.
     */
    @ParameterizedTest(name = "{1}")
    @CsvSource({
        "1, int4, java.lang.Integer",
        "2, varchar, java.lang.String",
    })
    public void aParameterWithAPostgresTypeIsDescribedByPgJdbc(
            int index, String typeName, String className) throws Exception {
        ParameterMetaData parameters = parametersOf(INSERT);
        assertThat(parameters.getParameterTypeName(index), is(typeName));
        assertThat(parameters.getParameterClassName(index), is(className));
    }

    /**
     * A parameter that travels as json is bound from a {@code Map} or a
     * {@code List}, so the class named is the one those have in common. The
     * three CrateDB types that reach the server this way are all described the
     * same, because on the wire they are the same.
     */
    @ParameterizedTest(name = "{1}")
    @CsvSource({
        "3, object",
        "4, geo_shape",
        "5, nested array",
    })
    public void aJsonParameterIsDescribedByWhatBindsToIt(int index, String column) throws Exception {
        ParameterMetaData parameters = parametersOf(INSERT);
        assertThat(column, parameters.getParameterType(index), is(Types.OTHER));
        assertThat(column, parameters.getParameterTypeName(index), is("json"));
        assertThat(column, parameters.getParameterClassName(index), is("java.lang.Object"));
    }

    /**
     * The class pgJDBC would have named for the same parameter, which an
     * application would then have had to build. Naming it is the behavior this
     * driver replaces, so the two answers have to differ for the replacement to
     * be doing anything.
     */
    @Test
    public void pgJdbcWouldNameItsOwnWrapperForAJsonParameter() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement(INSERT)) {
            ParameterMetaData parameters = statement.getParameterMetaData();
            ParameterMetaData underlying =
                parameters.unwrap(org.postgresql.jdbc.PgParameterMetaData.class);

            assertThat(underlying.getParameterClassName(3), is("org.postgresql.util.PGobject"));
            assertThat(parameters.getParameterClassName(3), is(not(underlying.getParameterClassName(3))));
        }
    }

    /**
     * Asked twice, a parameter answers the same. The driver keeps what it
     * worked out, because reading a parameter's type name costs the server a
     * catalog query.
     */
    @Test
    public void aParameterAnswersTheSameOnEveryRead() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement(INSERT)) {
            ParameterMetaData parameters = statement.getParameterMetaData();
            for (int round = 0; round < 3; round++) {
                assertThat(parameters.getParameterClassName(3), is("java.lang.Object"));
                assertThat(parameters.getParameterClassName(1), is("java.lang.Integer"));
            }
        }
    }

    /**
     * An index naming no parameter is refused, in the terms the parameters are
     * described in everywhere else — the driver does not answer for a parameter
     * the statement does not have, nor fail differently because it was asked
     * for the class rather than the type.
     */
    @Test
    public void anIndexOutsideTheStatementIsRefused() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement(INSERT)) {
            ParameterMetaData parameters = statement.getParameterMetaData();
            assertThat(parameters.getParameterCount(), is(5));
            assertThrows(SQLException.class, () -> parameters.getParameterClassName(0));
            assertThrows(SQLException.class, () -> parameters.getParameterClassName(6));
        }
    }

    /**
     * A statement whose parameters are all ordinary types is described without
     * the driver reading a single type name, and answers as pgJDBC does.
     */
    @Test
    public void aStatementWithoutJsonParametersIsDescribedThroughout() throws Exception {
        ParameterMetaData parameters =
            parametersOf("select id from param_meta where id = ? and name = ?");
        assertThat(parameters.getParameterCount(), is(2));
        assertThat(parameters.getParameterClassName(1), is("java.lang.Integer"));
        assertThat(parameters.getParameterClassName(2), is("java.lang.String"));
    }
}
