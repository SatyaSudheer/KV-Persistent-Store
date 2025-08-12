# KV Persistent Store - Quick Reference

## 🚀 Quick Start
```bash
make quick-start    # Build and start server
make quick-test     # Build, test, and verify
```

## 🔨 Build Commands
```bash
make compile        # Compile only
make test           # Run tests only
make package        # Build JAR
make build-all      # Full build cycle
```

## 🖥️ Server Management
```bash
make server         # Start standalone server (port 8081)
make server-enhanced # Start enhanced server with WAL/SSTable support
make server-cluster # Start in cluster mode
make server-cluster-enhanced # Start enhanced server in cluster mode
make server-custom PORT=8080  # Custom port
make stop           # Stop all servers
make status         # Check server status
```

## 📱 Client Operations
```bash
make client-put KEY=mykey VALUE=myvalue
make client-get KEY=mykey
make client-delete KEY=mykey
make client-ping
```

## 🏗️ Cluster Setup
```bash
make cluster-node1  # Start node 1 (port 8081)
make cluster-node2  # Start node 2 (port 8082)
make cluster-node3  # Start node 3 (port 8083)
```

## 🧹 Maintenance
```bash
make clean          # Clean build artifacts
make clean-data     # Remove data directories
make logs           # Show recent logs
```

## 🎯 Demo & Testing
```bash
make demo           # Full demo with put/get/delete
make benchmark      # Performance test (100 operations)
```

## 📋 Common Workflows

### Development Workflow
```bash
make compile        # Quick compile
make dev-server     # Start dev server
# ... make changes ...
make test           # Run tests
make package        # Build for deployment
```

### Testing Workflow
```bash
make build-all      # Ensure everything builds
make test           # Run all tests
make quick-test     # End-to-end test
```

### Production Deployment
```bash
make build-all      # Build with tests
make server         # Start production server
# ... monitor with make status ...
make stop           # Graceful shutdown
```

## 🔧 Customization

### Change Default Port
```bash
# Edit Makefile and change DEFAULT_PORT = 8081
# Or use custom port command:
make server-custom PORT=9090
```

### Change Data Directory
```bash
# Edit Makefile and change DEFAULT_DATA_DIR = ./data
# Or use custom server command:
java -cp target/kv-persistent-store-1.0.0.jar com.kvstore.KVStoreMain server 8081 ./mydata
```

## 📊 Monitoring
```bash
make status         # Check if server is running
make logs           # View recent logs
ps aux | grep KVStoreMain  # Detailed process info
```

## 🆘 Troubleshooting

### Server Won't Start
```bash
make status         # Check if already running
make stop           # Stop any existing servers
make clean-data     # Clear corrupted data
make server         # Try starting again
```

### Tests Failing
```bash
make clean          # Clean build artifacts
make compile        # Recompile
make test           # Run tests again
```

### Port Already in Use
```bash
lsof -i :8081       # Check what's using the port
make server-custom PORT=8082  # Use different port
```

## 📚 Full Documentation
For complete documentation, see `README.md` and run `make help`.
