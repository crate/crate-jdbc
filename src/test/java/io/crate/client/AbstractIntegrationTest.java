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
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertFalse;

public abstract class AbstractIntegrationTest {

    private static final int r = new Random(System.currentTimeMillis()).nextInt(1000);
    public static final int httpPort = 44200 + r;
    public static final int transportPort = 44300 + r;
    public static final String crateHost = "127.0.0.1";
    private static final String workingDir = System.getProperty("user.dir");
    private static Process crateProcess;
    private static boolean started = false;
    private static final StringBuilder crateStdout = new StringBuilder();

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
        new Thread(new Runnable() {
            @Override
            public void run() {
                InputStream is = crateProcess.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String line = null;
                try {
                    line = reader.readLine();
                    while (true) {
                        synchronized (crateStdout) {
                            crateStdout.append(line);
                        }
                        if (line == null ) {
                            Thread.sleep(100);
                        }
                        line = reader.readLine();
                        try {
                            crateProcess.exitValue();
                            break;
                        } catch (IllegalThreadStateException e) {
                            // fine
                        }

                    }
                } catch (IOException|InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * wait until crate is ready
     * @param timeoutMillis the number of milliseconds to wait
     * @return true if server is ready, false if a timeout or another IOException occurred
     */
    private static boolean waitUntilServerIsReady(final int timeoutMillis) throws IOException {
        URL url = new URL("http", "127.0.0.1", httpPort, "/");
        final HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setConnectTimeout(timeoutMillis);
        FutureTask<Boolean> task = new FutureTask<>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                int responseCode = -1;
                while (responseCode != 200) {
                    try {
                        conn.connect();
                        responseCode = conn.getResponseCode();
                    } catch (ConnectException e) {
                        // carry on
                    } catch (IOException e) {
                        e.printStackTrace();
                        return false;
                    }
                    System.out.print('.');
                    Thread.sleep(100);
                }
                return responseCode == 200;
            }
        });
        new Thread(task).start();
        try {
            return task.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            task.cancel(true);
            return false;
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

    private static void printCrateStdout() {
        synchronized (crateStdout) {
            System.err.println(crateStdout.toString());
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        if (!started) {
            System.out.println("Starting crate server process...");
            startCrateAsDaemon();
        }
        if (!waitUntilServerIsReady(60 * 1000)) { // wait 1 minute max
            printCrateStdout();
            crateProcess.destroy();
            started = false;
            throw new IllegalStateException("Crate Test Server not started");
        }
        started = true;
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        System.out.println("Stopping crate server process...");
        crateProcess.destroy();
        crateProcess.waitFor();
        wipeDataDirectory();
        wipeLogs();
        started = false;
    }


}
