package com.kvstore.cluster;
import com.kvstore.api.ClusterStatus;
import com.kvstore.api.KVCluster;
import com.kvstore.api.KVStore;
import com.kvstore.api.NodeInfo;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
public class DistributedKVCluster implements KVCluster {

    private static final Logger logger = Logger.getLogger(DistributedKVCluster.class.getName());
    private static final int HEARTBEAT_INTERVAL = 5000; // 5 seconds
    private static final int ELECTION_TIMEOUT = 10000; // 10 seconds
    private static final int REPLICATION_TIMEOUT = 30000; // 30 seconds

    private final String nodeId;
    private final String host;
    private final int port;
    private final KVStore localStore;
    private final Map<String, NodeInfo> nodes;
    private final AtomicReference<NodeInfo> currentLeader;
    private final AtomicBoolean isLeader;
    private final AtomicReference<ClusterStatus> status;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService replicationExecutor;
    private final Map<String, Long> lastHeartbeat;
    private final Object clusterLock;

    public DistributedKVCluster(String nodeId, String host, int port, KVStore localStore) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.localStore = localStore;
        this.nodes = new ConcurrentHashMap<>();
        this.currentLeader = new AtomicReference<>();
        this.isLeader = new AtomicBoolean(false);
        this.status = new AtomicReference<>(ClusterStatus.FAILED);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.replicationExecutor = Executors.newCachedThreadPool();
        this.lastHeartbeat = new ConcurrentHashMap<>();
        this.clusterLock = new Object();

        // Add self to cluster
        addNode(nodeId, host, port);

        // Start background tasks
        startHeartbeat();
        startLeaderElection();
    }

    @Override
    public boolean addNode(String nodeId, String host, int port) {
        synchronized (clusterLock) {
            NodeInfo nodeInfo = new NodeInfo(nodeId, host, port, false, true);
            nodes.put(nodeId, nodeInfo);
            lastHeartbeat.put(nodeId, System.currentTimeMillis());

            logger.info("Added node to cluster: " + nodeInfo);

            // If this is the first node, become leader
            if (nodes.size() == 1) {
                electLeader();
            }

            return true;
        }
    }

    @Override
    public boolean removeNode(String nodeId) {
        synchronized (clusterLock) {
            NodeInfo removed = nodes.remove(nodeId);
            lastHeartbeat.remove(nodeId);

            if (removed != null) {
                logger.info("Removed node from cluster: " + removed);

                // If the removed node was the leader, start election
                if (removed.isLeader()) {
                    electLeader();
                }

                return true;
            }

            return false;
        }
    }

    @Override
    public List<NodeInfo> getNodes() {
        return new ArrayList<>(nodes.values());
    }

    @Override
    public Optional<NodeInfo> getLeader() {
        return Optional.ofNullable(currentLeader.get());
    }

    @Override
    public boolean isLeader() {
        return isLeader.get();
    }

    @Override
    public boolean electLeader() {
        synchronized (clusterLock) {
            // Simple leader election: node with lowest ID becomes leader
            Optional<NodeInfo> newLeader = nodes.values().stream()
                    .filter(NodeInfo::isHealthy)
                    .min(Comparator.comparing(NodeInfo::getNodeId));

            if (newLeader.isPresent()) {
                NodeInfo leader = newLeader.get();
                currentLeader.set(leader);
                isLeader.set(leader.getNodeId().equals(nodeId));

                // Update leader status in nodes map
                nodes.values().forEach(node -> {
                    NodeInfo updatedNode = new NodeInfo(
                            node.getNodeId(), node.getHost(), node.getPort(),
                            node.getNodeId().equals(leader.getNodeId()), node.isHealthy()
                    );
                    nodes.put(node.getNodeId(), updatedNode);
                });

                status.set(ClusterStatus.HEALTHY);
                logger.info("New leader elected: " + leader.getNodeId());
                return isLeader.get();
            } else {
                status.set(ClusterStatus.FAILED);
                logger.warning("No healthy nodes available for leader election");
                return false;
            }
        }
    }

    @Override
    public boolean replicateData(String key, String value) {
        if (!isLeader()) {
            logger.warning("Only leader can replicate data");
            return false;
        }

        List<CompletableFuture<Boolean>> replicationFutures = new ArrayList<>();

        // Replicate to all other nodes
        for (NodeInfo node : nodes.values()) {
            if (!node.getNodeId().equals(nodeId) && node.isHealthy()) {
                CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                    return replicateToNode(node, key, value);
                }, replicationExecutor);
                replicationFutures.add(future);
            }
        }

        // Wait for all replications to complete
        try {
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    replicationFutures.toArray(new CompletableFuture[0])
            );
            allFutures.get(REPLICATION_TIMEOUT, TimeUnit.MILLISECONDS);

            // Check if all replications were successful
            boolean allSuccessful = replicationFutures.stream()
                    .map(CompletableFuture::join)
                    .allMatch(success -> success);

            if (allSuccessful) {
                logger.info("Data replicated successfully to all nodes");
            } else {
                logger.warning("Some replications failed");
            }

            return allSuccessful;
        } catch (Exception e) {
            logger.severe("Replication failed: " + e.getMessage());
            return false;
        }
    }

    private boolean replicateToNode(NodeInfo node, String key, String value) {
        try (Socket socket = new Socket(node.getHost(), node.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send replication command
            String command = "REPLICATE|" + key + "|" + value;
            out.println(command);

            String response = in.readLine();
            return "OK".equals(response);
        } catch (IOException e) {
            logger.warning("Failed to replicate to node " + node.getNodeId() + ": " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean handleFailover() {
        synchronized (clusterLock) {
            NodeInfo currentLeaderNode = currentLeader.get();

            if (currentLeaderNode == null || !currentLeaderNode.isHealthy()) {
                logger.info("Leader is down, initiating failover");
                return electLeader();
            }

            return true;
        }
    }

    @Override
    public ClusterStatus getStatus() {
        return status.get();
    }

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Send heartbeat to all other nodes
                for (NodeInfo node : nodes.values()) {
                    if (!node.getNodeId().equals(nodeId)) {
                        sendHeartbeat(node);
                    }
                }

                // Check for failed nodes
                checkNodeHealth();

            } catch (Exception e) {
                logger.severe("Error in heartbeat: " + e.getMessage());
            }
        }, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void startLeaderElection() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Check if current leader is still healthy
                NodeInfo leader = currentLeader.get();
                if (leader != null && !leader.isHealthy()) {
                    logger.info("Leader is unhealthy, starting election");
                    handleFailover();
                }

            } catch (Exception e) {
                logger.severe("Error in leader election check: " + e.getMessage());
            }
        }, ELECTION_TIMEOUT, ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat(NodeInfo node) {
        try (Socket socket = new Socket(node.getHost(), node.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println("HEARTBEAT|" + nodeId);
            String response = in.readLine();

            if ("PONG".equals(response)) {
                lastHeartbeat.put(node.getNodeId(), System.currentTimeMillis());
            }
        } catch (IOException e) {
            logger.warning("Failed to send heartbeat to " + node.getNodeId() + ": " + e.getMessage());
        }
    }

    private void checkNodeHealth() {
        long now = System.currentTimeMillis();
        long timeout = HEARTBEAT_INTERVAL * 3; // 3 missed heartbeats = unhealthy

        for (NodeInfo node : nodes.values()) {
            Long lastHeartbeatTime = lastHeartbeat.get(node.getNodeId());
            boolean isHealthy = lastHeartbeatTime != null && (now - lastHeartbeatTime) < timeout;

            if (node.isHealthy() != isHealthy) {
                // Update node health status
                NodeInfo updatedNode = new NodeInfo(
                        node.getNodeId(), node.getHost(), node.getPort(),
                        node.isLeader(), isHealthy
                );
                nodes.put(node.getNodeId(), updatedNode);

                if (!isHealthy) {
                    logger.warning("Node " + node.getNodeId() + " is unhealthy");
                    if (node.isLeader()) {
                        handleFailover();
                    }
                } else {
                    logger.info("Node " + node.getNodeId() + " is healthy again");
                }
            }
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        replicationExecutor.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!replicationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                replicationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}