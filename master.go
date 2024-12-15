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
					// Handle PING command specially
					if isPingCommand(cmd) {
						pongCmd := Value{Type: TypeBulkString, BulkString: "PONG"}
						select {
						case r.outgoing <- pongCmd:
							s.logger.Info("Sent PONG response to replica %s", r.ID)
						default:
							s.logger.Error("Failed to send PONG to replica %s", r.ID)
						}
						continue
					}

					// Handle REPLCONF command specially
					if isReplConfCommand(cmd) {
						okCmd := Value{Type: TypeBulkString, BulkString: "OK"}
						select {
						case r.outgoing <- okCmd:
							s.logger.Info("Sent OK response to replica %s", r.ID)
						default:
							s.logger.Error("Failed to send OK to replica %s", r.ID)
						}
						continue
					}

					// Process other commands normally
					result, err := handleCommand(cmd, s)
					if err != nil {
						s.logger.Error("Error processing command from replica %s: %v", r.ID, err)
						continue
					}

					// Send response back to replica
					select {
					case r.outgoing <- result:
						s.logger.Info("Sent response to replica %s", r.ID)
					default:
						s.logger.Error("Failed to send response to replica %s", r.ID)
					}
				}
			}(replica)
		}
		s.replState.mux.RUnlock()
		time.Sleep(100 * time.Millisecond)
	}
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
