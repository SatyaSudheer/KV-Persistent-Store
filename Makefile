# KV Persistent Store Makefile
# Quick commands for building, testing, and running the application

# Variables
JAR_NAME = kv-persistent-store-1.0.0.jar
TARGET_DIR = target
MAIN_CLASS = com.kvstore.KVStoreMain
DEFAULT_PORT = 8081
DEFAULT_DATA_DIR = ./data

# Colors for output
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

.PHONY: help clean compile test package run-server run-client run-cluster clean-data build-all

# Default target
help:
	@echo "$(GREEN)KV Persistent Store - Available Commands:$(NC)"
	@echo ""
	@echo "$(YELLOW)Build Commands:$(NC)"
	@echo "  make compile     - Compile the project"
	@echo "  make test        - Run all tests"
	@echo "  make package     - Build JAR package"
	@echo "  make build-all   - Clean, compile, test, and package"
	@echo ""
	@echo "$(YELLOW)Server Commands:$(NC)"
	@echo "  make server      - Start standalone server on port $(DEFAULT_PORT)"
	@echo "  make server-enhanced - Start enhanced server with WAL/SSTable support"
	@echo "  make server-cluster - Start server in cluster mode on port $(DEFAULT_PORT)"
	@echo "  make server-cluster-enhanced - Start enhanced server in cluster mode"
	@echo "  make server-custom PORT=8080 - Start server on custom port"
	@echo ""
	@echo "$(YELLOW)Client Commands:$(NC)"
	@echo "  make client-put KEY=testkey VALUE=testvalue - Put key-value pair"
	@echo "  make client-get KEY=testkey               - Get value by key"
	@echo "  make client-delete KEY=testkey            - Delete key"
	@echo "  make client-ping                          - Ping server"
	@echo ""
	@echo "$(YELLOW)Cluster Commands:$(NC)"
	@echo "  make cluster-node1 - Start first cluster node on port 8081"
	@echo "  make cluster-node2 - Start second cluster node on port 8082"
	@echo "  make cluster-node3 - Start third cluster node on port 8083"
	@echo ""
	@echo "$(YELLOW)Utility Commands:$(NC)"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make clean-data   - Remove data directories"
	@echo "  make status       - Check if server is running"
	@echo "  make stop         - Stop all running servers"
	@echo "  make logs         - Show recent logs"

# Build Commands
clean:
	@echo "$(GREEN)Cleaning build artifacts...$(NC)"
	mvn clean

compile:
	@echo "$(GREEN)Compiling project...$(NC)"
	mvn compile

test:
	@echo "$(GREEN)Running tests...$(NC)"
	mvn test

package:
	@echo "$(GREEN)Building JAR package...$(NC)"
	mvn package

build-all: clean compile test package
	@echo "$(GREEN)Build completed successfully!$(NC)"

# Server Commands
server:
	@echo "$(GREEN)Starting standalone server on port $(DEFAULT_PORT)...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) server $(DEFAULT_PORT) $(DEFAULT_DATA_DIR)

server-enhanced:
	@echo "$(GREEN)Starting enhanced server on port $(DEFAULT_PORT)...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) server $(DEFAULT_PORT) $(DEFAULT_DATA_DIR) standalone enhanced

server-cluster:
	@echo "$(GREEN)Starting server in cluster mode on port $(DEFAULT_PORT)...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) server $(DEFAULT_PORT) $(DEFAULT_DATA_DIR) cluster

server-cluster-enhanced:
	@echo "$(GREEN)Starting enhanced server in cluster mode on port $(DEFAULT_PORT)...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) server $(DEFAULT_PORT) $(DEFAULT_DATA_DIR) cluster enhanced

server-custom:
	@echo "$(GREEN)Starting server on port $(PORT)...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) server $(PORT) $(DEFAULT_DATA_DIR)

# Client Commands
client-put:
	@echo "$(GREEN)Putting key '$(KEY)' with value '$(VALUE)'...$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) client localhost $(DEFAULT_PORT) put $(KEY) $(VALUE)

client-get:
	@echo "$(GREEN)Getting value for key '$(KEY)'...$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) client localhost $(DEFAULT_PORT) get $(KEY)

client-delete:
	@echo "$(GREEN)Deleting key '$(KEY)'...$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) client localhost $(DEFAULT_PORT) delete $(KEY)

client-ping:
	@echo "$(GREEN)Pinging server...$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) client localhost $(DEFAULT_PORT) ping

# Cluster Commands
cluster-node1:
	@echo "$(GREEN)Starting cluster node 1 on port 8081...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) cluster node1 8081 ./data1

cluster-node2:
	@echo "$(GREEN)Starting cluster node 2 on port 8082...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) cluster node2 8082 ./data2 localhost:8081

cluster-node3:
	@echo "$(GREEN)Starting cluster node 3 on port 8083...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/$(JAR_NAME) $(MAIN_CLASS) cluster node3 8083 ./data3 localhost:8081 localhost:8082

# Utility Commands
clean-data:
	@echo "$(GREEN)Cleaning data directories...$(NC)"
	rm -rf ./data* ./testdata
	@echo "$(GREEN)Data directories cleaned$(NC)"

status:
	@echo "$(GREEN)Checking server status...$(NC)"
	@if pgrep -f "KVStoreMain.*server" > /dev/null; then \
		echo "$(GREEN)Server is running$(NC)"; \
		ps aux | grep "KVStoreMain.*server" | grep -v grep; \
	else \
		echo "$(RED)No server is running$(NC)"; \
	fi

stop:
	@echo "$(GREEN)Stopping all KV Store servers...$(NC)"
	@pkill -f "KVStoreMain.*server" || true
	@pkill -f "KVStoreMain.*cluster" || true
	@echo "$(GREEN)All servers stopped$(NC)"

logs:
	@echo "$(GREEN)Recent server logs:$(NC)"
	@if [ -f ./logs/kvstore.log ]; then \
		tail -20 ./logs/kvstore.log; \
	else \
		echo "$(YELLOW)No log file found$(NC)"; \
	fi

# Quick Demo Commands
demo: build-all
	@echo "$(GREEN)Starting demo...$(NC)"
	@echo "$(YELLOW)1. Starting server in background...$(NC)"
	@make server > /dev/null 2>&1 &
	@sleep 3
	@echo "$(YELLOW)2. Testing put operation...$(NC)"
	@make client-put KEY=demo_key VALUE=demo_value
	@echo "$(YELLOW)3. Testing get operation...$(NC)"
	@make client-get KEY=demo_key
	@echo "$(YELLOW)4. Testing delete operation...$(NC)"
	@make client-delete KEY=demo_key
	@echo "$(YELLOW)5. Verifying deletion...$(NC)"
	@make client-get KEY=demo_key
	@echo "$(YELLOW)6. Stopping server...$(NC)"
	@make stop
	@echo "$(GREEN)Demo completed!$(NC)"

# Development Commands
dev-server: compile
	@echo "$(GREEN)Starting development server...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	java -cp $(TARGET_DIR)/classes:$(TARGET_DIR)/test-classes $(MAIN_CLASS) server $(DEFAULT_PORT) $(DEFAULT_DATA_DIR)

dev-client: compile
	@echo "$(GREEN)Starting development client...$(NC)"
	java -cp $(TARGET_DIR)/classes:$(TARGET_DIR)/test-classes $(MAIN_CLASS) client localhost $(DEFAULT_PORT) ping

# Performance Testing
benchmark: build-all
	@echo "$(GREEN)Running performance benchmark...$(NC)"
	@echo "$(YELLOW)Starting server...$(NC)"
	@make server > /dev/null 2>&1 &
	@sleep 3
	@echo "$(YELLOW)Running benchmark operations...$(NC)"
	@for i in {1..100}; do \
		make client-put KEY=bench_key_$$i VALUE=bench_value_$$i > /dev/null 2>&1; \
	done
	@echo "$(GREEN)Benchmark completed!$(NC)"
	@make stop

# Quick Start Commands
quick-start: build-all server
	@echo "$(GREEN)Quick start completed!$(NC)"

quick-test: build-all
	@echo "$(GREEN)Running quick test...$(NC)"
	@make server > /dev/null 2>&1 &
	@sleep 3
	@make client-ping
	@make stop
	@echo "$(GREEN)Quick test completed!$(NC)"
