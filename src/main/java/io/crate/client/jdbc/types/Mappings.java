/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.client.jdbc.types;

import io.crate.shade.com.google.common.collect.ImmutableMap;
import io.crate.types.*;

import java.sql.Types;
import java.util.Map;

public class Mappings {
    public static final Map<Class<? extends DataType>, Integer> CRATE_TO_JDBC = ImmutableMap.<Class<? extends DataType>, Integer>builder()
            .put(BooleanType.class, Types.BOOLEAN)
            .put(ByteType.class, Types.TINYINT)
            .put(ShortType.class, Types.SMALLINT)
            .put(IntegerType.class, Types.INTEGER)
            .put(LongType.class, Types.BIGINT)
            .put(FloatType.class, Types.REAL)
            .put(DoubleType.class, Types.DOUBLE)
            .put(StringType.class, Types.VARCHAR)
            .put(TimestampType.class, Types.TIMESTAMP)
            .put(IpType.class, Types.VARCHAR)
            .put(ArrayType.class, Types.ARRAY)
            .put(SetType.class, Types.ARRAY)
            .put(ObjectType.class, Types.JAVA_OBJECT)
            .put(UndefinedType.class, Types.NULL)
            .build();
}
