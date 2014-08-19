/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.client.jdbc;

import java.util.Comparator;
import java.util.Scanner;

/**
 *
 */
public class VersionStringComparator implements Comparator<String> {

    private static final VersionStringComparator INSTANCE = new VersionStringComparator();

    public static int compareVersions(String o1, String o2) {
        return INSTANCE.compare(o1, o2);
    }

    private VersionStringComparator() {}

    @Override
    public int compare(String o1, String o2) {
        Scanner o1Scanner = new Scanner(o1).useDelimiter("\\.");
        Scanner o2Scanner = new Scanner(o2).useDelimiter("\\.");
        while(o1Scanner.hasNextInt() && o2Scanner.hasNextInt()) {
            int v1 = o1Scanner.nextInt();
            int v2 = o2Scanner.nextInt();
            if(v1 < v2) {
                return -1;
            } else if(v1 > v2) {
                return 1;
            }
        }
        return o1Scanner.hasNextInt() ? 1 : (o2Scanner.hasNextInt() ? -1 : 0);
    }
}
