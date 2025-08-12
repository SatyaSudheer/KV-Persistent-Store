package com.kvstore.api;

public enum ClusterStatus {
    HEALTHY,        // All nodes are healthy and leader is elected
    DEGRADED,       // Some nodes are down but cluster is functional
    FAILED,         // Cluster is not functional
    ELECTION        // Leader election in progress
}
