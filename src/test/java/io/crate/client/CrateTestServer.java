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

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.junit.rules.ExternalResource;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertFalse;

public class CrateTestServer extends ExternalResource {

    public final int httpPort;
    public final int transportPort;
    public final String crateHost;
    private final String workingDir;

    private Process crateProcess;
    private ThreadPoolExecutor executor;
    private BlockingQueue<Runnable> workQueue;

    public CrateTestServer() {
        int randomInt = new Random(System.currentTimeMillis()).nextInt(1000);
        httpPort = 42000 + randomInt;
        transportPort = 44300 + randomInt;
        crateHost = "127.0.0.1";
        workingDir = System.getProperty("user.dir");
        workQueue = new ArrayBlockingQueue<>(3);
        executor = new ThreadPoolExecutor(3, 3, 10, TimeUnit.SECONDS, workQueue, new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                System.err.println(r.toString() + " got rejected");
            }
        });
        executor.prestartAllCoreThreads();
    }

    @Override
    protected void before() throws Throwable {
        System.out.println("Starting crate server process...");
        startCrateAsDaemon();
        if (!waitUntilServerIsReady(60 * 1000)) { // wait 1 minute max
            crateProcess.destroy();
            throw new IllegalStateException("Crate Test Server not started");
        }

    }

    @Override
    protected void after() {
        System.out.println("Stopping crate server process...");
        crateProcess.destroy();
        try {
            crateProcess.waitFor();
            wipeDataDirectory();
            wipeLogs();
        } catch (Exception e) {
            e.printStackTrace();
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private void startCrateAsDaemon() throws IOException, InterruptedException {
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
        // print server stdout to stdout
        workQueue.add(new Runnable() {
            @Override
            public void run() {
                InputStream is = crateProcess.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                try {
                    while (true) {

                        if (reader.ready()) {
                            System.out.println(reader.readLine());
                        } else {
                            Thread.sleep(100);
                        }
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
        });
    }

    /**
     * wait until crate is ready
     * @param timeoutMillis the number of milliseconds to wait
     * @return true if server is ready, false if a timeout or another IOException occurred
     */
    private boolean waitUntilServerIsReady(final int timeoutMillis) throws IOException {
        final CrateClient client = new CrateClient(crateHost + ":" + transportPort);
        FutureTask<Boolean> task = new FutureTask<>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {

                while (true) {
                    try {
                        client.sql("select id from sys.cluster")
                                .actionGet(timeoutMillis, TimeUnit.MILLISECONDS);
                        client.close();
                        break;
                    } catch (NoNodeAvailableException e) {
                        // carry on
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                    Thread.sleep(100);
                }
                return true;
            }
        });
        workQueue.add(task);
        try {
            return task.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            task.cancel(true);
            return false;
        }
    }

    private void deletePath(Path path) throws Exception {
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

    private void wipeDataDirectory() throws Exception {
        File dataDir = new File(workingDir + "/parts/crate/data");
        if (dataDir.exists()) {
            deletePath(dataDir.toPath());
            assertFalse(dataDir.exists());
        }
    }

    private void wipeLogs() throws Exception {
        File logDir = new File(workingDir + "/parts/crate/logs");
        if (logDir.exists()) {
            deletePath(logDir.toPath());
            assertFalse(logDir.exists());
        }
    }
}
