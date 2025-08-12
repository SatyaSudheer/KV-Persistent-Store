# KV Persistent Store

A high-performance, distributed key-value persistent store implementation in Java with clustering capabilities.

## Features

- **Persistent Storage**: Data is stored on disk with efficient indexing
- **Enhanced Storage**: Advanced storage with Write-Ahead Log (WAL) and SSTable support
- **Network Server**: TCP-based network interface for remote access
- **Clustering**: Distributed cluster with leader election and data replication
- **High Performance**: Optimized for read/write operations with concurrent access
- **Fault Tolerance**: Automatic failover and recovery in cluster mode
- **Data Recovery**: WAL-based recovery for crash resilience
- **Compaction**: Automatic SSTable compaction for optimal performance

## Architecture

The system consists of several key components:

- **Core Store**: `PersistentKVStore` - Basic persistent storage with file-based indexing
- **Enhanced Store**: `EnhancedKVStore` - Advanced storage with WAL, memtable, and SSTable support
- **Write-Ahead Log**: `WAL` - Ensures data durability and crash recovery
- **SSTable Manager**: `SSTableManager` - Manages immutable sorted string tables
- **SSTable**: `SSTable` - Immutable, sorted data files for efficient storage
- **Network Server**: `KVStoreNetworkServer` - Manages client connections and requests
- **Cluster Management**: `DistributedKVCluster` - Handles distributed operations
- **Client**: `KVClient` - Java client for interacting with the store

## Building

```bash
mvn clean compile
```

## Running

### Standalone Server

```bash
# Start a standalone server on port 8080
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain server 8080

# Start with custom data directory
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain server 8080 ./mydata

# Start with enhanced store (WAL + SSTable support)
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain server 8080 ./mydata standalone enhanced
```

### Cluster Mode

```bash
# Start first node
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain cluster node1 8080 ./data1

# Start second node (in another terminal)
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain cluster node2 8081 ./data2 localhost:8080

# Start third node (in another terminal)
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain cluster node3 8082 ./data3 localhost:8080 localhost:8081

# Start with enhanced store in cluster mode
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain server 8080 ./data cluster enhanced
```

### Client Usage

```bash
# Put a key-value pair
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain client localhost 8080 put mykey myvalue

# Get a value
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain client localhost 8080 get mykey

# Delete a key
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain client localhost 8080 delete mykey

# Ping server
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain client localhost 8080 ping
```

## Protocol

The server uses a simple text-based protocol over TCP:

- `PUT|key|value` - Store a key-value pair
- `GET|key` - Retrieve a value by key
- `DELETE|key` - Remove a key-value pair
- `PING` - Health check
- `RANGE|startKey|endKey` - Get all keys in a range
- `BATCH|key1|value1|key2|value2...` - Batch operations

## Cluster Commands

- `CLUSTER_STATUS` - Get cluster health status
- `CLUSTER_NODES` - List all nodes in the cluster
- `CLUSTER_LEADER` - Get current leader information

## Testing

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=PersistentKVStoreTest
```

## Configuration

The application can be configured using the `application.properties` file in the resources directory. Key configuration options include:

- Server port and thread pool size
- Data storage directory and file names
- Cluster heartbeat intervals and timeouts
- Logging levels and file locations

## Data Format

Data is stored in a binary format:
- Key length (4 bytes)
- Key data (UTF-8 encoded)
- Value length (4 bytes)
- Value data (UTF-8 encoded)

An index file maintains key-to-file-position mappings for fast lookups.

## Performance Considerations

- The store uses read-write locks for concurrent access
- Data is synced to disk on each write operation
- Batch operations are supported for better throughput
- Index is kept in memory for fast lookups

## Limitations

- Keys and values are stored as strings
- No built-in compression or encryption
- Limited to single-node data size
- No built-in backup/restore functionality

## Future Enhancements

- Support for different data types
- Compression and encryption
- Backup and restore capabilities
- Monitoring and metrics
- REST API interface
- Client libraries for other languages

## License

This project is licensed under the MIT License - see the LICENSE file for details.
