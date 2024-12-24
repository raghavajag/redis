package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type ReplicationConfig struct {
	Port      int    // Replication listening port
	ReplicaOf string // Master host:port if this is a replica
}

type ReplicationState struct {
	role       string   // "master" or "replica"
	offset     int64    // Replication offset
	masterConn net.Conn // Connection to master if replica
	replicas   map[string]*Replica
	mux        sync.RWMutex
}

type Replica struct {
	ID       string
	incoming chan Value // Channel for receiving commands from master
	outgoing chan Value // Channel for sending responses/acknowledgments to master
	conn     net.Conn
	writer   *RESPWriter
	reader   *RESPReader
	offset   int64
	lastACK  time.Time
}

func (s *Store) initReplication() error {
	s.replState = &ReplicationState{
		role:   s.config.Role,
		offset: 0,
	}
	if s.replConfig.ReplicaOf != "" {
		s.replState.role = "replica"
		return s.startAsReplica()
	}
	go s.startPropagationWorker()
	s.replState.replicas = make(map[string]*Replica)
	s.replState.role = "master"
	return s.startAsMaster()
}

func (s *Store) startAsReplica() error {
	conn, err := net.Dial("tcp", s.replConfig.ReplicaOf)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}

	replica := &Replica{
		ID:       "replica",
		incoming: make(chan Value, 1000), // Buffered channel for incoming commands from master
		outgoing: make(chan Value, 1000), // Buffered channel for outgoing responses to master
		conn:     conn,
		writer:   NewRESPWriter(conn),
		reader:   NewRESPReader(conn),
		lastACK:  time.Now(),
	}

	s.replState.masterConn = conn

	// Perform handshake with the master
	if err := s.performHandshake(replica); err != nil {
		conn.Close()
		return fmt.Errorf("handshake failed: %v", err)
	}

	if err := s.sendPSYNCCommand(replica); err != nil {
		conn.Close()
		return fmt.Errorf("PSYNC failed: %v", err)
	}

	// Start goroutines for handling communication with the master
	go s.receiveCommandsFromMaster(replica)
	go s.sendResponsesToMaster(replica)
	go s.processIncomingCommands(replica)

	return nil
}

func (s *Store) sendPSYNCCommand(replica *Replica) error {
	// Send PSYNC command with current replication offset
	psyncCmd := Value{
		Type: TypeArray,
		Array: []Value{
			{Type: TypeBulkString, BulkString: CMD_PSYNC},
			{Type: TypeBulkString, BulkString: replica.ID},                            // Use replica.ID instead of ReplID
			{Type: TypeBulkString, BulkString: strconv.FormatInt(replica.offset, 10)}, // Current offset
		},
	}

	s.logger.Info("Sending PSYNC command to master with offset %d", replica.offset)
	if err := replica.writer.Write(psyncCmd); err != nil {
		return fmt.Errorf("failed to send PSYNC command: %v", err)
	}

	// Read master's response
	response, err := replica.reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read PSYNC response: %v", err)
	}

	switch response.BulkString {
	case FULLSYNC:
		s.logger.Info("Received FULLSYNC response from master")
		return s.receiveRDBFromMaster(replica) // Perform full synchronization
	case CONTINUE:
		s.logger.Info("Received CONTINUE response from master")
		go s.processIncomingCommands(replica) // Start processing incremental updates
	default:
		return fmt.Errorf("unexpected PSYNC response: %v", response)
	}

	return nil
}

func isFullSyncResponse(v Value) bool {
	return v.Type == TypeBulkString && v.BulkString == FULLSYNC
}

func (s *Store) generateReplicaID() string {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.replicaCounter++
	return fmt.Sprintf("replica_%d", s.replicaCounter)
}

func (s *Store) performHandshake(replica *Replica) error {
	// Step 1: Send PING
	pingCmd := Value{
		Type: TypeArray,
		Array: []Value{
			{Type: TypeBulkString, BulkString: "PING"},
		},
	}
	s.logger.Info("Sending PING to master")
	if err := replica.writer.Write(pingCmd); err != nil {
		return fmt.Errorf("failed to send PING during handshake")
	}

	// Wait for PONG response
	response, err := replica.reader.Read()
	if err != nil || !isPongResponse(response) {
		return fmt.Errorf("invalid PING response: expected PONG, got %v", response)
	}
	s.logger.Info("Received PONG from master")

	// Step 2: Send REPLCONF listening-port
	portCmd := Value{
		Type: TypeArray,
		Array: []Value{
			{Type: TypeBulkString, BulkString: "REPLCONF"},
			{Type: TypeBulkString, BulkString: "listening-port"},
			{Type: TypeBulkString, BulkString: strconv.Itoa(s.replConfig.Port)},
		},
	}

	s.logger.Info("Sending REPLCONF listening-port to master")
	if err := replica.writer.Write(portCmd); err != nil {
		return fmt.Errorf("failed to send REPLCONF during handshake")
	}

	// Wait for OK response
	response, err = replica.reader.Read()
	if err != nil || !isOKResponse(response) {
		return fmt.Errorf("invalid REPLCONF response: expected OK, got %v", response)
	}
	s.logger.Info("Received OK from master for REPLCONF")

	return nil
}

func isPongResponse(v Value) bool {
	return (v.Type == TypeString && v.String == "PONG") ||
		(v.Type == TypeBulkString && v.BulkString == "PONG")
}

func isOKResponse(v Value) bool {
	return (v.Type == TypeString && v.String == "OK") ||
		(v.Type == TypeBulkString && v.BulkString == "OK")
}

// Goroutine to receive commands from the master and enqueue them into the incoming channel.
func (s *Store) receiveCommandsFromMaster(replica *Replica) {
	defer func() {
		if s.replState.masterConn != nil {
			s.replState.masterConn.Close()
			close(replica.incoming) // Close the incoming channel when done
			s.logger.Info("Closed connection with master")
		}
	}()

	for {
		command, err := replica.reader.Read()
		if err != nil {
			if err == io.EOF {
				s.logger.Error("Master connection closed")
				return
			}
			s.logger.Error("Error reading from master: %v", err)
			continue
		}

		if len(command.Array) == 0 { // Validate received command
			// s.logger.Error("Received invalid or empty command from master %v\n", command)
			continue // Skip invalid commands
		}

		s.logger.Info("Received command from master: %v", command)

		select {
		case replica.incoming <- command: // Enqueue command into the incoming channel
			s.logger.Info("Queued command from master for processing")
		default:
			s.logger.Error("Incoming command queue is full; dropping command")
		}
	}
}

// Goroutine to process incoming commands asynchronously.
func (s *Store) processIncomingCommands(replica *Replica) {
	for cmd := range replica.incoming {
		s.logger.Info("Processing command from master: %v", cmd)

		// Process the command
		_, err := handleCommand(cmd, s)
		if err != nil {
			s.logger.Error("Error processing command from master: %v", err)
			continue
		}

		// Update replication offset
		replica.offset++
		s.logger.Info("Updated replication offset to %d", replica.offset)

		// Send acknowledgment back to the master
		ackCmd := Value{
			Type: TypeArray,
			Array: []Value{
				{Type: TypeBulkString, BulkString: "REPLCONF"},
				{Type: TypeBulkString, BulkString: "ACK"},
				{Type: TypeBulkString, BulkString: strconv.FormatInt(replica.offset, 10)},
			},
		}
		select {
		case replica.outgoing <- ackCmd:
			s.logger.Info("Sent acknowledgment to master with offset %d", replica.offset)
		default:
			s.logger.Error("Failed to send acknowledgment; outgoing queue is full")
		}
	}
}

func (s *Store) sendResponsesToMaster(replica *Replica) {
	defer func() {
		if s.replState.masterConn != nil {
			s.replState.masterConn.Close()
			close(replica.outgoing) // Close the outgoing channel when done.
			s.logger.Info("Closed outgoing response channel to master")
		}
	}()

	for response := range replica.outgoing { // Listen for responses on the outgoing channel.
		s.logger.Info("Sending response to master: %v", response)

		if err := replica.writer.Write(response); err != nil {
			s.logger.Error("Failed to send response to master: %v", err)
			return // Exit the goroutine on error
		}

		s.logger.Info("Successfully sent response to master: %v", response)
	}
}

func (s *Store) receiveRDBFromMaster(replica *Replica) error {
	// Read RDB length first (as a RESP integer)
	lengthResp, err := replica.reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read RDB length: %v", err)
	}

	length, err := strconv.ParseInt(lengthResp.BulkString, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid RDB length: %v", err)
	}

	// Create temporary file
	tempPath := fmt.Sprintf("./%s/%s.temp", s.config.Dir, s.config.DBFilename)
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp RDB file: %v", err)
	}
	defer tempFile.Close()

	// Read raw bytes directly from connection
	bytesReceived := int64(0)
	buffer := make([]byte, 8192) // 8KB buffer

	for bytesReceived < length {
		toRead := min(int64(len(buffer)), length-bytesReceived)
		n, err := io.ReadFull(replica.reader.reader, buffer[:toRead])
		if err != nil {
			return fmt.Errorf("error reading RDB data: %v", err)
		}

		if _, err := tempFile.Write(buffer[:n]); err != nil {
			return fmt.Errorf("error writing RDB chunk: %v", err)
		}

		bytesReceived += int64(n)
		s.logger.Info("RDB receive progress: %d/%d bytes", bytesReceived, length)
	}

	// Ensure all data is written
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("error syncing RDB file: %v", err)
	}

	// Atomically rename temp file
	finalPath := fmt.Sprintf("./%s/%s", s.config.Dir, s.config.DBFilename)
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("error finalizing RDB file: %v", err)
	}

	// Load the new RDB file
	return s.loadRDBFile(finalPath)
}

func (s *Store) loadRDBFile(path string) error {
	rdbReader, err := NewRDBReader(s.config.Dir, s.config.DBFilename)
	if err != nil {
		return fmt.Errorf("failed to create RDB reader: %v", err)
	}
	defer rdbReader.Close()

	database, err := rdbReader.ReadDatabase()
	if err != nil {
		return fmt.Errorf("failed to load RDB data: %v", err)
	}

	s.mux.Lock()
	defer s.mux.Unlock()

	s.items = make(map[string]V)
	for key, value := range database {
		s.items[key] = V{value: value, savedTime: time.Now()}
	}

	return nil
}
