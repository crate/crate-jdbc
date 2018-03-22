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

public class CrateDriverVersion {

    private static final boolean SNAPSHOT = true;
    static final CrateDriverVersion CURRENT = new CrateDriverVersion(20400, SNAPSHOT);

    private final int id;
    final byte major;
    final byte minor;
    private final byte revision;
    private final Boolean snapshot;

    private CrateDriverVersion(int id, Boolean snapshot) {
        this.id = id;
        this.major = (byte) ((id / 10000) % 100);
        this.minor = (byte) ((id / 100) % 100);
        this.revision = (byte) ((id) % 100);
        this.snapshot = snapshot;
    }

    private boolean snapshot() {
        return snapshot != null && snapshot;
    }

    /**
     * Just the version number (without -SNAPSHOT if snapshot).
     */
    private String number() {
        return String.valueOf(major) + '.' + minor + '.' + revision;
    }

    public static void main(String[] args) {
        System.out.println(
                "Version: " + CrateDriverVersion.CURRENT +
                ", JVM: " + System.getProperty("java.version"));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(number());
        if (snapshot()) {
            sb.append("-SNAPSHOT");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CrateDriverVersion version = (CrateDriverVersion) o;

        return id == version.id;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
