# Binary names
BINARY_NAME=redis-clone
TEST_BINARY_NAME=redis-clone-test

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Build flags
LDFLAGS=-ldflags "-w -s"

# Directories
BUILD_DIR=build
TMP_DIR=tmp

.PHONY: all build clean test run-master run-replica deps tidy

all: clean deps build

build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) -v

clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

test:
	$(GOTEST) -v ./...

deps:
	$(GOMOD) download

tidy:
	$(GOMOD) tidy

# Run master instance
run-master: build
	./$(BUILD_DIR)/$(BINARY_NAME) -role master -replication-port 6380

# Run replica instance
run-replica: build
	./$(BUILD_DIR)/$(BINARY_NAME) -role replica -master localhost:6380 -port 5379

# Run multiple replicas (ports 6381, 6382)
run-replicas: build
	./$(BUILD_DIR)/$(BINARY_NAME) -role replica -master localhost:6380 -port 5379 & \
	./$(BUILD_DIR)/$(BINARY_NAME) -role replica -master localhost:6380 -port 5380

# Stop all running instances
stop:
	pkill -f $(BINARY_NAME) || true

# Install binary to system
install: build
	cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/

# Uninstall binary from system
uninstall:
	rm -f /usr/local/bin/$(BINARY_NAME)