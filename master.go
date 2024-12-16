package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"
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
	if len(cmd.Array) < 2 {
		return Value{}, fmt.Errorf("invalid PSYNC command from replica %s", replica.ID)
	}

	offset := cmd.Array[1].BulkString // Replica's replication offset

	s.logger.Info("Received PSYNC from replica %s with offset %s", replica.ID, offset)

	// Simulate creating an RDB snapshot
	// Send RDB data to the replica

	return Value{Type: TypeBulkString, BulkString: "CONTINUE"}, nil // Indicate incremental updates will follow
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
