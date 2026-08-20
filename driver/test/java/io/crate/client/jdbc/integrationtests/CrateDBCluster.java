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

package io.crate.client.jdbc.integrationtests;

import org.testcontainers.containers.Network;
import org.testcontainers.cratedb.CrateDBContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Several CrateDB nodes forming one cluster, on a Docker network of their own.
 *
 * <p>A cluster is where the driver's own defaults start to mean something.
 * {@code loadBalanceHosts} is on unless a caller turns it off, and a query
 * timeout is delivered to the server directly precisely because pgJDBC's
 * cancel request travels on a second connection that a load balancer may point
 * at another node. On one node all of that is inert.</p>
 *
 * <p>Nodes reach each other by network alias over the transport port, and that is
 * not published — the shared network is what makes that unnecessary. Clients
 * reach them from outside on the PostgreSQL port each container mapped, which
 * is a different one per node, so a URL can name them all.</p>
 */
final class CrateDBCluster implements AutoCloseable {

    /** How long the nodes get to find each other before the fixture gives up. */
    private static final Duration FORMATION_TIMEOUT = Duration.ofMinutes(2);

    private final Network network;
    private final List<CrateDBContainer> nodes;

    private CrateDBCluster(Network network, List<CrateDBContainer> nodes) {
        this.network = network;
        this.nodes = nodes;
    }

    /**
     * Starts a cluster of the given size and comes back once every node has
     * joined it. That is not the same as every node answering, and it is the
     * thing a test needs to be true.
     */
    static CrateDBCluster start(DockerImageName image, int size) {
        Network network = Network.newNetwork();
        List<String> aliases = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            aliases.add("crate-" + i);
        }
        List<CrateDBContainer> nodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            nodes.add(new CrateDBContainer(image)
                .withNetwork(network)
                .withNetworkAliases(aliases.get(i))
                // Wholesale, because the constructor's command pins the node to
                // being a cluster of one.
                .withCommand(command(aliases, i)));
        }
        // In parallel: a node started on its own waits for the others, and a
        // cluster brought up one node at a time would wait for each in turn.
        Startables.deepStart(nodes).join();
        CrateDBCluster cluster = new CrateDBCluster(network, nodes);
        try {
            cluster.awaitFormation(size);
        } catch (RuntimeException e) {
            cluster.close();
            throw e;
        }
        return cluster;
    }

    private static String command(List<String> aliases, int node) {
        StringJoiner seeds = new StringJoiner(",");
        aliases.forEach(seeds::add);
        return "crate"
            + " -Ccluster.name=crate-jdbc-test"
            + " -Cnode.name=" + aliases.get(node)
            // The transport has to bind to the address the other nodes resolve
            // the alias to, rather than to the loopback the default would pick.
            + " -Cnetwork.host=_site_"
            + " -Cdiscovery.seed_hosts=" + seeds
            + " -Ccluster.initial_master_nodes=" + seeds
            + " -Cgateway.expected_data_nodes=" + aliases.size()
            + " -Cgateway.recover_after_data_nodes=" + aliases.size()
            // Round trips are counted from this, and a cluster is where the
            // count is worth having.
            + " -Cstats.enabled=true";
    }

    /**
     * Waits until the cluster reports the nodes it is supposed to have. Each
     * container answers HTTP as soon as it is up, which says nothing about
     * whether it found the others.
     */
    private void awaitFormation(int expected) {
        long deadline = System.nanoTime() + FORMATION_TIMEOUT.toNanos();
        SQLException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (Connection conn = DriverManager.getConnection(urlFor(nodes.get(0)));
                 Statement statement = conn.createStatement();
                 ResultSet rows = statement.executeQuery("select count(*) from sys.nodes")) {
                rows.next();
                if (rows.getLong(1) == expected) {
                    return;
                }
            } catch (SQLException notYet) {
                lastFailure = notYet;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for the cluster to form", e);
            }
        }
        throw new IllegalStateException(
            "The cluster did not reach " + expected + " nodes within " + FORMATION_TIMEOUT,
            lastFailure);
    }

    /**
     * A URL naming every node, as a CrateDB URL is meant to: the driver
     * turns on {@code loadBalanceHosts}, so a connection opened through it
     * lands on one node or another.
     */
    String url() {
        StringJoiner hosts = new StringJoiner(",");
        for (CrateDBContainer node : nodes) {
            if (node.isRunning()) {
                hosts.add(node.getHost() + ":" + node.getMappedPort(5432));
            }
        }
        return "crate://" + hosts + "/doc?user=crate";
    }

    /** A URL naming one node, for asking that node about the cluster. */
    String urlFor(int node) {
        return urlFor(nodes.get(node));
    }

    private static String urlFor(CrateDBContainer node) {
        return "crate://" + node.getHost() + ":" + node.getMappedPort(5432) + "/doc?user=crate";
    }

    @Override
    public void close() {
        for (CrateDBContainer node : nodes) {
            try {
                node.stop();
            } catch (RuntimeException ignored) {
                // A node already gone is a node already stopped.
            }
        }
        network.close();
    }
}
