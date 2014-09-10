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

import org.apache.log4j.*;
import org.apache.log4j.varia.NullAppender;

import java.util.Enumeration;

public class LoggingHelper {

    public static boolean isConfigured() {
        Enumeration appenders = Logger.getRootLogger().getAllAppenders();
        if (appenders.hasMoreElements()) {
            return true;
        }
        else {
            Enumeration loggers = LogManager.getCurrentLoggers() ;
            while (loggers.hasMoreElements()) {
                Logger c = (Logger) loggers.nextElement();
                if (c.getAllAppenders().hasMoreElements())
                    return true;
            }
        }
        return false;
    }

    public static void muteSafe() {
        if (!isConfigured()) {
            Logger root = Logger.getLogger("io.crate.client");
            root.removeAllAppenders();
            root.setLevel(Level.OFF);
            root.addAppender(new NullAppender());
            Logger.getLogger("org.elasticsearch").setLevel(Level.OFF);
        }
    }

    public static void configureDefaultSafe() {
        if (!isConfigured()) {
            Logger.getRootLogger().setLevel(Level.INFO);

            Logger crateLogger = Logger.getLogger("io.crate.client");
            crateLogger.addAppender(new ConsoleAppender(
                    new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        }
    }
}
