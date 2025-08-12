package com.kvstore.api;

public class NodeInfo {
    private final String nodeId;
    private final String host;
    private final int port;
    private final boolean isLeader;
    private final boolean isHealthy;

    public NodeInfo(String nodeId, String host, int port, boolean isLeader, boolean isHealthy) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.isLeader = isLeader;
        this.isHealthy = isHealthy;
    }

    public String getNodeId() { return nodeId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public boolean isLeader() { return isLeader; }
    public boolean isHealthy() { return isHealthy; }

    @Override
    public String toString() {
        return String.format("NodeInfo{nodeId='%s', host='%s', port=%d, leader=%s, healthy=%s}",
                nodeId, host, port, isLeader, isHealthy);
    }
}
