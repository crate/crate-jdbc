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

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

/**
 * Rebuilds a {@link CrateDataSource} from the JNDI reference it was bound as.
 * A directory holds a data source as the properties it was configured with plus
 * the name of the factory that turns them back into an object. pgJDBC's own
 * factory answers for pgJDBC's classes alone, and returns nothing when handed a
 * CrateDB data source.
 */
public class CrateDataSourceFactory implements ObjectFactory {

    @Override
    public Object getObjectInstance(Object obj, Name name, Context context, Hashtable<?, ?> environment) {
        if (!(obj instanceof Reference)) {
            return null;
        }
        Reference reference = (Reference) obj;
        if (!CrateDataSource.class.getName().equals(reference.getClassName())) {
            return null;
        }
        CrateDataSource dataSource = new CrateDataSource();
        dataSource.setFromReference(reference);
        return dataSource;
    }
}
