package com.kvstore;

import com.kvstore.api.KVStore;
import com.kvstore.core.PersistentKVStore;
import com.kvstore.core.EnhancedKVStore;
import com.kvstore.server.KVStoreNetworkServer;
import com.kvstore.cluster.DistributedKVCluster;
import com.kvstore.client.KVClient;

import java.io.File;

public class KVStoreMain {
    
    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            return;
        }
        
        String mode = args[0].toLowerCase();
        
        try {
            switch (mode) {
                case "server":
                    runServer(args);
                    break;
                case "client":
                    runClient(args);
                    break;
                case "cluster":
                    runCluster(args);
                    break;
                default:
                    System.out.println("Unknown mode: " + mode);
                    printUsage();
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void runServer(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: KVStoreMain server <port> [data-directory] [cluster-mode] [store-type]");
            return;
        }
        
        int port = Integer.parseInt(args[1]);
        String dataDir = args.length > 2 ? args[2] : "./data";
        boolean clusterMode = args.length > 3 && "cluster".equals(args[3]);
        String storeType = args.length > 4 ? args[4] : "persistent"; // Default to persistent store
        
        // Create data directory if it doesn't exist
        File directory = new File(dataDir);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        
        // Initialize KV store based on type
        KVStore kvStore;
        if ("enhanced".equals(storeType)) {
            System.out.println("Using Enhanced KV Store with WAL and SSTable support");
            kvStore = new EnhancedKVStore(dataDir);
        } else {
            System.out.println("Using Persistent KV Store");
            kvStore = new PersistentKVStore(dataDir);
        }
        
        // Initialize server
        KVStoreNetworkServer server;
        if (clusterMode) {
            String nodeId = "node-" + port;
            DistributedKVCluster cluster = new DistributedKVCluster(nodeId, "localhost", port, kvStore);
            server = new KVStoreNetworkServer(kvStore, cluster);
            System.out.println("Starting server in cluster mode on port " + port + " with node ID: " + nodeId);
        } else {
            server = new KVStoreNetworkServer(kvStore);
            System.out.println("Starting standalone server on port " + port);
        }
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down server...");
            server.stop();
            kvStore.close();
        }));
        
        // Start server
        server.start(port);
    }
    
    private static void runClient(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: KVStoreMain client <host> <port> <command> [args...]");
            return;
        }
        
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        
        KVClient client = new KVClient(host, port);
        
        // Test connection
        if (!client.ping()) {
            System.err.println("Failed to connect to server at " + host + ":" + port);
            System.exit(1);
        }
        
        System.out.println("Connected to server at " + host + ":" + port);
        
        // Execute command
        String command = args[3].toLowerCase();
        
        switch (command) {
            case "put":
                if (args.length != 6) {
                    System.out.println("put requires key and value");
                    return;
                }
                boolean putResult = client.put(args[4], args[5]);
                System.out.println(putResult ? "OK" : "Failed");
                break;
                
            case "get":
                if (args.length != 5) {
                    System.out.println("get requires key");
                    return;
                }
                String value = client.get(args[4]);
                if (value != null) {
                    System.out.println(value);
                } else {
                    System.out.println("NOT_FOUND");
                }
                break;
                
            case "delete":
                if (args.length != 5) {
                    System.out.println("delete requires key");
                    return;
                }
                boolean deleteResult = client.delete(args[4]);
                System.out.println(deleteResult ? "OK" : "NOT_FOUND");
                break;
                
            case "ping":
                boolean pingResult = client.ping();
                System.out.println(pingResult ? "PONG" : "Failed");
                break;
                
            default:
                System.out.println("Unknown command: " + command);
        }
    }
    
    private static void runCluster(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: KVStoreMain cluster <node-id> <port> <data-directory> [peer-host:peer-port]...");
            return;
        }
        
        String nodeId = args[1];
        int port = Integer.parseInt(args[2]);
        String dataDir = args[3];
        
        // Create data directory if it doesn't exist
        File directory = new File(dataDir);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        
        // Initialize KV store and cluster
        KVStore kvStore = new PersistentKVStore(dataDir);
        DistributedKVCluster cluster = new DistributedKVCluster(nodeId, "localhost", port, kvStore);
        
        // Add peer nodes if specified
        for (int i = 4; i < args.length; i++) {
            String[] peerParts = args[i].split(":");
            if (peerParts.length == 2) {
                String peerHost = peerParts[0];
                int peerPort = Integer.parseInt(peerParts[1]);
                cluster.addNode("peer-" + peerPort, peerHost, peerPort);
            }
        }
        
        // Initialize server with cluster
        KVStoreNetworkServer server = new KVStoreNetworkServer(kvStore, cluster);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down cluster node...");
            server.stop();
            cluster.shutdown();
            kvStore.close();
        }));
        
        // Start server
        System.out.println("Starting cluster node " + nodeId + " on port " + port);
        server.start(port);
    }
    
    private static void printUsage() {
        System.out.println("KV Persistent Store - Usage:");
        System.out.println("  KVStoreMain server <port> [data-directory] [cluster-mode] [store-type]");
        System.out.println("  KVStoreMain client <host> <port> <command> [args...]");
        System.out.println("  KVStoreMain cluster <node-id> <port> <data-directory> [peer-host:peer-port]...");
        System.out.println("");
        System.out.println("Examples:");
        System.out.println("  KVStoreMain server 8080 ./data");
        System.out.println("  KVStoreMain server 8080 ./data cluster");
        System.out.println("  KVStoreMain server 8080 ./data cluster enhanced");
        System.out.println("  KVStoreMain client localhost 8080 put mykey myvalue");
        System.out.println("  KVStoreMain client localhost 8080 get mykey");
        System.out.println("  KVStoreMain cluster node1 8080 ./data1 localhost:8081 localhost:8082");
    }
}
