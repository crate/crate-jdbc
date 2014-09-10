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

package io.crate.client;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.*;

import static org.junit.Assert.assertFalse;

public abstract class AbstractIntegrationTest {

    public static final int httpPort = 44200;
    public static final int transportPort = 44300;
    public static final String crateHost = "127.0.0.1";
    private static final String workingDir = System.getProperty("user.dir");
    private static Process crateProcess;

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static void startCrateAsDaemon() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(
                "bin/crate",
                "-Des.index.storage.type=memory",
                "-Des.network.host=" + crateHost,
                "-Des.cluster.name=Testing"+transportPort,
                "-Des.http.port="+httpPort,
                "-Des.transport.tcp.port="+transportPort
        );
        processBuilder.directory(new File(workingDir+"/parts/crate/"));
        processBuilder.redirectErrorStream(true);
        crateProcess = processBuilder.start();
    }

    /**
     * wait until crate is ready to receive transport requests
     * @param timeoutMillis the number of milliseconds to wait
     * @return true if server is ready, false if a timeout or another IOException occurred
     */
    private static boolean waitUntilServerIsReady(final int timeoutMillis) throws IOException {

        final InetSocketAddress address = new InetSocketAddress(crateHost, transportPort);
        final SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);

        FutureTask<Boolean> task = new FutureTask<>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                boolean connected = false;
                while (!connected) {
                    try {
                        socketChannel.socket().connect(address, timeoutMillis);
                        connected = true;
                    } catch (IOException e) {
                        return false;
                    }
                }
                return true;
            }
        });
        task.run();
        try {
            return task.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return false;
        }
    }

    private static void printCrateStdErr() throws IOException {
        InputStream error = crateProcess.getErrorStream();
        InputStreamReader errorReader = new InputStreamReader(error);
        BufferedReader bre = new BufferedReader(errorReader);
        String line;
        while ((line = bre.readLine()) != null) {
            System.err.println(line);
        }
    }

    private static void printCrateStdOut() throws IOException {
        InputStream error = crateProcess.getInputStream();
        InputStreamReader errorReader = new InputStreamReader(error);
        BufferedReader bre = new BufferedReader(errorReader);
        String line;

        while (bre.ready() && (line = bre.readLine()) != null) {
            System.err.println(line);
        }
    }

    private static void deletePath(Path path) throws Exception {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });

    }

    private static void wipeDataDirectory() throws Exception {
        File dataDir = new File(workingDir + "/parts/crate/data");
        if (dataDir.exists()) {
            deletePath(dataDir.toPath());
            assertFalse(dataDir.exists());
        }
    }

    private static void wipeLogs() throws Exception {
        File logDir = new File(workingDir + "/parts/crate/logs");
        if (logDir.exists()) {
            deletePath(logDir.toPath());
            assertFalse(logDir.exists());
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        startCrateAsDaemon();
        // give crate time to settle
        if (!waitUntilServerIsReady(20000)) {
            printCrateStdOut();
            printCrateStdErr();
            throw new IllegalStateException("Crate Test Server not started");
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        crateProcess.destroy();
        crateProcess.waitFor();
        wipeDataDirectory();
        wipeLogs();
    }


}
