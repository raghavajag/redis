package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"
	"os"
)
const (
    CMD_PSYNC = "PSYNC"
    FULLSYNC  = "FULLSYNC"
    CONTINUE  = "CONTINUE"
)
const (
    RDB_CHUNK_SIZE = 8192
)
func (s *Store) startAsMaster() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.replConfig.Port))
	if err != nil {
		s.logger.Error("Failed to start listener on port %d: %v", s.replConfig.Port, err)
		return err
	}
	go s.processIncomingCommandsFromReplicas()
	go s.acceptReplicas(listener)
	s.logger.Info("Master started and listening for replicas on port %d", s.replConfig.Port)
	return nil
}

func (s *Store) acceptReplicas(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.logger.Error("Error accepting replica connection: %v", err)
			continue
		}
		go s.handleReplicaConnection(conn)
	}
}

func (s *Store) handleReplicaConnection(conn net.Conn) {
	replicaID := generateReplicaID()
	replica := &Replica{
		ID:       replicaID,
		incoming: make(chan Value, 1000), // Buffered channel for incoming commands
		outgoing: make(chan Value, 1000), // Buffered channel for outgoing commands
		conn:     conn,
		writer:   NewRESPWriter(conn),
		reader:   NewRESPReader(conn),
		lastACK:  time.Now(),
	}

	s.replState.mux.Lock()
	s.replState.replicas[replicaID] = replica
	s.replState.mux.Unlock()

	// Start goroutines for handling this replica
	go s.receiveCommandsFromReplica(replica)
	go s.sendCommandsToReplica(replica)

	s.logger.Info("Replica connected: %s", replicaID)
}

func (s *Store) receiveCommandsFromReplica(replica *Replica) {
	defer func() {
		s.cleanupReplica(replica)
	}()

	for {
		command, err := replica.reader.Read()
		if err != nil {
			if err == io.EOF {
				s.logger.Error("Connection closed by replica %s", replica.ID)
				return
			}
			s.logger.Error("Error reading command from replica %s: %v", replica.ID, err)
			return
		}

		if len(command.Array) == 0 { // Validate received command
			s.logger.Error("Received invalid or empty command from replica %s", replica.ID)
			continue // Skip invalid commands
		}

		s.logger.Info("Received command from replica %s: %v", replica.ID, command)

		select {
		case replica.incoming <- command: // Enqueue command into the incoming channel
			s.logger.Info("Queued command from replica %s for processing", replica.ID)
		default:
			s.logger.Error("Incoming command queue is full for replica %s; dropping command", replica.ID)
		}
	}
}

func (s *Store) sendCommandsToReplica(replica *Replica) {
	defer func() {
		s.cleanupReplica(replica)
	}()

	for cmd := range replica.outgoing { // Listen for commands on the outgoing channel
		if err := replica.writer.Write(cmd); err != nil {
			s.logger.Error("Failed to send command to replica %s: %v", replica.ID, err)
			return
		}
		s.logger.Info("Sent command to replica %s: %v", replica.ID, cmd)
	}
}

func (s *Store) processIncomingCommandsFromReplicas() {
	for {
		s.replState.mux.RLock()
		for _, replica := range s.replState.replicas {
			go func(r *Replica) {
				for cmd := range r.incoming {
					// Check and handle the type of command
					response, err := s.processReplicaCommand(r, cmd)
					if err != nil {
						s.logger.Error("Error processing command from replica %s: %v", r.ID, err)
						continue
					}

					// Pipe the response into the outgoing channel
					select {
					case r.outgoing <- response:
					default:
						s.logger.Error("Failed to send response to replica %s; outgoing queue is full", r.ID)
					}
				}
			}(replica)
		}
		s.replState.mux.RUnlock()
		time.Sleep(100 * time.Millisecond) // Avoid busy looping
	}
}

func (s *Store) processReplicaCommand(replica *Replica, cmd Value) (Value, error) {
	// Handle PING command
	if isPingCommand(cmd) {
		s.logger.Info("Received PING from replica %s", replica.ID)
		return Value{Type: TypeBulkString, BulkString: "PONG"}, nil
	}

	// Handle REPLCONF command
	if isReplConfCommand(cmd) {
		return s.handleReplConfCommand(replica, cmd)
	}

	// Handle other commands n// Handle PSYNC command
	if isPSYNCCommand(cmd) {
		return s.handlePSYNCCommand(replica, cmd)
	}
	result, err := handleCommand(cmd, s)
	if err != nil {
		return Value{}, fmt.Errorf("error processing command: %v", err)
	}
	return result, nil
}
func (s *Store) handleReplConfCommand(replica *Replica, cmd Value) (Value, error) {
	if len(cmd.Array) < 2 {
		return Value{}, fmt.Errorf("invalid REPLCONF command from replica %s", replica.ID)
	}

	subcommand := strings.ToLower(cmd.Array[1].BulkString)

	switch subcommand {
	case "listening-port":
		if len(cmd.Array) < 3 {
			return Value{}, fmt.Errorf("missing port in REPLCONF listening-port from replica %s", replica.ID)
		}
		port := cmd.Array[2].BulkString
		s.logger.Info("Replica %s reported listening port: %s", replica.ID, port)
	case "ack":
		replica.lastACK = time.Now()
		s.logger.Info("Received ACK from replica %s at offset %d", replica.ID, replica.offset)
	default:
		return Value{}, fmt.Errorf("unknown REPLCONF subcommand '%s' from replica %s", subcommand, replica.ID)
	}

	return Value{Type: TypeBulkString, BulkString: "OK"}, nil
}
func (s *Store) handlePSYNCCommand(replica *Replica, cmd Value) (Value, error) {
    if len(cmd.Array) < 3 {
        return Value{}, fmt.Errorf("invalid PSYNC command format")
    }

    // Send FULLSYNC response directly through RESP
    fullsyncResponse := Value{
        Type: TypeBulkString,
        BulkString: "FULLSYNC",
    }
    if err := replica.writer.Write(fullsyncResponse); err != nil {
        return Value{}, fmt.Errorf("failed to send FULLSYNC: %v", err)
    }

    // Send RDB file directly through socket
    if err := s.sendRDBToReplica(replica); err != nil {
        return Value{}, fmt.Errorf("RDB transfer failed: %v", err)
    }

    // Send CONTINUE directly through RESP
    continueCmd := Value{
        Type: TypeBulkString,
        BulkString: "CONTINUE",
    }
    if err := replica.writer.Write(continueCmd); err != nil {
        return Value{}, fmt.Errorf("failed to send CONTINUE: %v", err)
    }

    return Value{Type: TypeString, String: "OK"}, nil
}

func (s *Store) cleanupReplica(replica *Replica) {
	s.replState.mux.Lock()
	defer s.replState.mux.Unlock()

	if _, exists := s.replState.replicas[replica.ID]; exists {
		close(replica.incoming) // Close channels to stop goroutines
		close(replica.outgoing)
		replica.conn.Close() // Close the connection
		delete(s.replState.replicas, replica.ID)
		s.logger.Info("Cleaned up resources for disconnected replica %s", replica.ID)
	}
}

// Helper functions for command validation
func isPingCommand(v Value) bool {
	return len(v.Array) > 0 &&
		v.Array[0].Type == TypeBulkString &&
		strings.ToUpper(v.Array[0].BulkString) == "PING"
}

func isReplConfCommand(v Value) bool {
	return len(v.Array) > 0 &&
		v.Array[0].Type == TypeBulkString &&
		strings.ToUpper(v.Array[0].BulkString) == "REPLCONF"
}

func isPSYNCCommand(v Value) bool {
	return len(v.Array) > 0 &&
		v.Array[0].Type == TypeBulkString &&
		strings.ToUpper(v.Array[0].BulkString) == "PSYNC"
}

// createRDBSnapshot serializes the database into an RDB format and returns the byte slice
func (s *Store) createRDBSnapshot() ([]byte, error) {
	var rdbData []byte

	// Write header
	header := []byte("REDIS0012")
	rdbData = append(rdbData, header...)

	// Write key-value pairs
	for key, v := range s.items {
		rdbData = append(rdbData, 0x00) // Start of key-value pair marker

		keyBytes, err := encodeString(key)
		if err != nil {
			return nil, err
		}
		rdbData = append(rdbData, keyBytes...)

		valueBytes, err := encodeString(v.value)
		if err != nil {
			return nil, err
		}
		rdbData = append(rdbData, valueBytes...)
	}

	// Write EOF marker
	rdbData = append(rdbData, 0xFF)

	return rdbData, nil
}

// encodeString encodes a string with its length prefixed as a single byte
func encodeString(s string) ([]byte, error) {
	length := len(s)
	if length > 255 { // Ensure length fits in one byte for simplicity
		return nil, fmt.Errorf("string too long to encode: %s", s)
	}

	data := make([]byte, length+1)
	data[0] = byte(length) // Length prefix as one byte
	copy(data[1:], s)
	return data, nil
}
func (s *Store) sendRDBToReplica(replica *Replica) error {
    // Create RDB snapshot
    if err := s.Save(); err != nil {
        return fmt.Errorf("failed to create RDB snapshot: %v", err)
    }

    path := fmt.Sprintf("./%s/%s", s.config.Dir, s.config.DBFilename)
    rdbFile, err := os.Open(path)
    if err != nil {
        return fmt.Errorf("failed to open RDB file: %v", err)
    }
    defer rdbFile.Close()

    fileInfo, err := rdbFile.Stat()
    if err != nil {
        return fmt.Errorf("failed to get RDB file info: %v", err)
    }

    // Send file size as RESP integer
    sizeCmd := Value{
        Type: TypeBulkString,
        BulkString: fmt.Sprintf("%d", fileInfo.Size()),
    }
    if err := replica.writer.Write(sizeCmd); err != nil {
        return fmt.Errorf("failed to send size: %v", err)
    }

    // Stream raw bytes directly through socket
    buffer := make([]byte, 8192)
    for {
        n, err := rdbFile.Read(buffer)
        if err == io.EOF {
            break
        }
        if err != nil {
            return fmt.Errorf("error reading RDB chunk: %v", err)
        }

        if _, err := replica.conn.Write(buffer[:n]); err != nil {
            return fmt.Errorf("error sending RDB chunk: %v", err)
        }
    }

    return nil
}

