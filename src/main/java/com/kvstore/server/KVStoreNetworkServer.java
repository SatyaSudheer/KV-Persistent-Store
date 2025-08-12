package com.kvstore.server;
import com.kvstore.api.KVServer;
import com.kvstore.api.KVStore;
import com.kvstore.api.KVCluster;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
public class KVStoreNetworkServer implements KVServer {

    private static final Logger logger = Logger.getLogger(KVStoreNetworkServer.class.getName());
    private final KVStore kvStore;
    private final KVCluster cluster;
    private final ExecutorService executorService;
    private ServerSocket serverSocket;
    private volatile boolean running = false;

    public KVStoreNetworkServer(KVStore kvStore) {
        this.kvStore = kvStore;
        this.cluster = null;
        this.executorService = Executors.newCachedThreadPool();
    }

    public KVStoreNetworkServer(KVStore kvStore, KVCluster cluster) {
        this.kvStore = kvStore;
        this.cluster = cluster;
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void start(int port) throws Exception {
        serverSocket = new ServerSocket(port);
        running = true;

        logger.info("KV Server started on port " + port);

        // Start accepting connections
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> handleClient(clientSocket));
            } catch (IOException e) {
                if (running) {
                    logger.severe("Error accepting connection: " + e.getMessage());
                }
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                String response = processCommand(inputLine);
                out.println(response);
            }
        } catch (IOException e) {
            logger.warning("Error handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                logger.warning("Error closing client socket: " + e.getMessage());
            }
        }
    }

    private String processCommand(String command) {
        try {
            String[] parts = command.split("\\|");
            if (parts.length == 0) {
                return "ERROR|Invalid command format";
            }

            String operation = parts[0].toUpperCase();

            switch (operation) {
                case "PUT":
                    if (parts.length != 3) {
                        return "ERROR|PUT requires key and value";
                    }
                    boolean putResult = kvStore.put(parts[1], parts[2]);

                    // If cluster is enabled and this node is leader, replicate data
                    if (putResult && cluster != null && cluster.isLeader()) {
                        cluster.replicateData(parts[1], parts[2]);
                    }

                    return putResult ? "OK" : "ERROR|Put operation failed";

                case "GET":
                    if (parts.length != 2) {
                        return "ERROR|GET requires key";
                    }
                    var getResult = kvStore.read(parts[1]);
                    return getResult.map(value -> "OK|" + value).orElse("NOT_FOUND");

                case "RANGE":
                    if (parts.length != 3) {
                        return "ERROR|RANGE requires startKey and endKey";
                    }
                    var rangeResult = kvStore.readKeyRange(parts[1], parts[2]);
                    StringBuilder rangeResponse = new StringBuilder("OK");
                    for (var entry : rangeResult.entrySet()) {
                        rangeResponse.append("|").append(entry.getKey()).append("=").append(entry.getValue());
                    }
                    return rangeResponse.toString();

                case "BATCH":
                    if (parts.length < 3 || parts.length % 2 != 1) {
                        return "ERROR|BATCH requires pairs of keys and values";
                    }
                    var keys = new java.util.ArrayList<String>();
                    var values = new java.util.ArrayList<String>();
                    for (int i = 1; i < parts.length; i += 2) {
                        keys.add(parts[i]);
                        values.add(parts[i + 1]);
                    }
                    boolean batchResult = kvStore.batchPut(keys, values);

                    // If cluster is enabled and this node is leader, replicate batch data
                    if (batchResult && cluster != null && cluster.isLeader()) {
                        for (int i = 0; i < keys.size(); i++) {
                            cluster.replicateData(keys.get(i), values.get(i));
                        }
                    }

                    return batchResult ? "OK" : "ERROR|Batch operation failed";

                case "DELETE":
                    if (parts.length != 2) {
                        return "ERROR|DELETE requires key";
                    }
                    boolean deleteResult = kvStore.delete(parts[1]);
                    return deleteResult ? "OK" : "NOT_FOUND";

                case "PING":
                    return "PONG";

                // Cluster-specific commands
                case "REPLICATE":
                    if (parts.length != 3) {
                        return "ERROR|REPLICATE requires key and value";
                    }
                    boolean replicateResult = kvStore.put(parts[1], parts[2]);
                    return replicateResult ? "OK" : "ERROR|Replication failed";

                case "HEARTBEAT":
                    if (parts.length != 2) {
                        return "ERROR|HEARTBEAT requires nodeId";
                    }
                    return "PONG";

                case "CLUSTER_STATUS":
                    if (cluster == null) {
                        return "ERROR|Cluster not enabled";
                    }
                    return "OK|" + cluster.getStatus().name();

                case "CLUSTER_NODES":
                    if (cluster == null) {
                        return "ERROR|Cluster not enabled";
                    }
                    StringBuilder nodesResponse = new StringBuilder("OK");
                    for (var node : cluster.getNodes()) {
                        nodesResponse.append("|").append(node.getNodeId()).append("=")
                                .append(node.getHost()).append(":").append(node.getPort())
                                .append(":").append(node.isLeader()).append(":").append(node.isHealthy());
                    }
                    return nodesResponse.toString();

                case "CLUSTER_LEADER":
                    if (cluster == null) {
                        return "ERROR|Cluster not enabled";
                    }
                    var leader = cluster.getLeader();
                    return leader.map(l -> "OK|" + l.getNodeId()).orElse("NOT_FOUND");

                default:
                    return "ERROR|Unknown operation: " + operation;
            }
        } catch (Exception e) {
            logger.severe("Error processing command: " + e.getMessage());
            return "ERROR|Internal server error";
        }
    }

    @Override
    public void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.severe("Error closing server socket: " + e.getMessage());
        }

        executorService.shutdown();
        logger.info("KV Server stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
