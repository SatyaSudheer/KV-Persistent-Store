package com.kvstore.api;

import java.util.List;
import java.util.Optional;

public interface KVCluster {
    boolean addNode(String nodeId, String host, int port);
    boolean removeNode(String nodeId);
    List<NodeInfo> getNodes();
    Optional<NodeInfo> getLeader();
    boolean isLeader();
    boolean electLeader();
    boolean replicateData(String key, String value);
    boolean handleFailover();
    ClusterStatus getStatus();
}
