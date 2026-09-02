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

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * One wrapper per result set a statement is holding. Asking a statement for
 * its rows twice gives back the same object, the way it does on a pgJDBC
 * statement and the way callers that compare result sets expect.
 */
final class CurrentResultSet {

    private final Statement statement;

    private ResultSet delegate;
    private CrateResultSet wrapper;

    CurrentResultSet(Statement statement) {
        this.statement = statement;
    }

    CrateResultSet of(ResultSet resultSet) {
        if (resultSet == null) {
            return null;
        }
        if (resultSet != delegate) {
            delegate = resultSet;
            wrapper = new CrateResultSet(resultSet, statement);
        }
        return wrapper;
    }
}
