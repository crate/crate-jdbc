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

package io.crate.client.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The version of this driver, as the build stamped it into
 * {@code version.properties}. Reading it from the artifact instead of
 * restating it in source keeps a release from shipping a driver that
 * misreports itself through {@link java.sql.DatabaseMetaData} and
 * {@link java.sql.Driver}.
 */
final class CrateDriverVersion {

    private static final String RESOURCE = "version.properties";

    /** A Maven version: {@code major.minor.patch} with an optional qualifier. */
    private static final Pattern VERSION = Pattern.compile("(\\d+)\\.(\\d+)\\.\\d+.*");

    static final CrateDriverVersion CURRENT = read();

    private final String text;
    final int major;
    final int minor;

    private CrateDriverVersion(String text) {
        Matcher matcher = VERSION.matcher(text);
        if (!matcher.matches()) {
            throw new IllegalStateException("Not a driver version: " + text);
        }
        this.text = text;
        this.major = Integer.parseInt(matcher.group(1));
        this.minor = Integer.parseInt(matcher.group(2));
    }

    private static CrateDriverVersion read() {
        Properties properties = new Properties();
        try (InputStream resource = CrateDriverVersion.class.getResourceAsStream(RESOURCE)) {
            if (resource == null) {
                throw new IllegalStateException(RESOURCE + " is missing from the driver jar");
            }
            properties.load(resource);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + RESOURCE, e);
        }
        String version = properties.getProperty("version");
        if (version == null) {
            throw new IllegalStateException(RESOURCE + " does not name a version");
        }
        return new CrateDriverVersion(version);
    }

    @Override
    public String toString() {
        return text;
    }
}
