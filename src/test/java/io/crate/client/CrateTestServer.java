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

import io.crate.shade.com.google.common.base.Joiner;
import io.crate.shade.com.google.common.base.MoreObjects;
import io.crate.shade.org.elasticsearch.client.transport.NoNodeAvailableException;
import io.crate.shade.org.elasticsearch.common.Nullable;
import org.junit.rules.ExternalResource;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;
import java.util.concurrent.*;

import static org.junit.Assert.assertFalse;

public class CrateTestServer extends ExternalResource {

    public final int httpPort;
    public final int transportPort;
    public final String crateHost;
    private final String workingDir;
    private final String clusterName;
    private final String[] unicastHosts;

    private Process crateProcess;
    private ThreadPoolExecutor executor;

    public static CrateTestServer[] cluster(String clusterName, int numberOfNodes) {
        int transportPorts[] = new int[numberOfNodes];
        int httpPorts[] = new int[numberOfNodes];
        for (int i = 0; i<numberOfNodes; i++) {
            transportPorts[i] = randomAvailablePort();
            httpPorts[i] = randomAvailablePort();
        }
        String hostAddress = InetAddress.getLoopbackAddress().getHostAddress();
        CrateTestServer[] servers = new CrateTestServer[numberOfNodes];
        String[] unicastHosts = getUnicastHosts(hostAddress, transportPorts);
        for (int i = 0; i< numberOfNodes; i++) {
            servers[i] = new CrateTestServer(clusterName, hostAddress,
                    httpPorts[i], transportPorts[i],
                    unicastHosts);
        }
        return servers;
    }

    private static String[] getUnicastHosts(String hostAddress, int[] transportPorts) {
        String[] result = new String[transportPorts.length];
        for (int i=0; i < transportPorts.length;i++) {
            result[i] = String.format(Locale.ENGLISH, "%s:%d", hostAddress, transportPorts[i]);
        }
        return result;
    }

    public CrateTestServer(@Nullable String clusterName) {
        this(clusterName,
                randomAvailablePort(),
                randomAvailablePort(),
                System.getProperty("user.dir"),
                InetAddress.getLoopbackAddress().getHostAddress());
    }

    public CrateTestServer(@Nullable String clusterName,String host, int httpPort, int transportPort, String ... unicastHosts) {
        this(clusterName, httpPort, transportPort, System.getProperty("user.dir"), host, unicastHosts);
    }

    public CrateTestServer(@Nullable String clusterName, int httpPort, int transportPort,
                           String workingDir, String host, String ... unicastHosts) {
        this.clusterName = MoreObjects.firstNonNull(clusterName, "Testing" + transportPort);
        this.crateHost = host;
        this.httpPort = httpPort;
        this.transportPort = transportPort;
        this.unicastHosts = unicastHosts;
        this.workingDir = workingDir;
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(3);
        executor = new ThreadPoolExecutor(3, 3, 10, TimeUnit.SECONDS, workQueue, new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                System.err.println(r.toString() + " got rejected");
            }
        });
        executor.prestartAllCoreThreads();
    }

    /**
     * @return a random available port for binding
     */
    public static int randomAvailablePort() {
        try {
            ServerSocket socket = new  ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        String[] command = new String[]{
                "bin/crate",
                "-Des.index.storage.type=memory",
                "-Des.network.host=" + crateHost,
                "-Des.cluster.name=" + clusterName,
                "-Des.http.port=" + httpPort,
                "-Des.transport.tcp.port=" + transportPort,
                "-Des.discovery.zen.ping.multicast.enabled=false",
                "-Des.discovery.zen.ping.unicast.hosts=" + Joiner.on(",").join(unicastHosts)
        };
        ProcessBuilder processBuilder = new ProcessBuilder(
            command
        );
        assert new File(workingDir).exists();
        processBuilder.directory(new File(workingDir, "/parts/crate"));
        processBuilder.redirectErrorStream(true);
        crateProcess = processBuilder.start();
        // print server stdout to stdout
        executor.submit(new Runnable() {
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
                } catch (IOException | InterruptedException e) {
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
        executor.submit(task);
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
