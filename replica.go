package main

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type ReplicationConfig struct {
	Port      int    // Replication listening port
	ReplicaOf string // Master host:port if this is a replica
	ReplID    string // Unique replication ID
}

type ReplicationState struct {
	role       string   // "master" or "replica"
	offset     int64    // Replication offset
	masterConn net.Conn // Connection to master if replica
	replicas   map[string]*Replica
	mux        sync.RWMutex
}

type Replica struct {
	conn    net.Conn
	writer  *RESPWriter
	reader  *RESPReader
	offset  int64
	lastACK time.Time
}

func (s *Store) initReplication() error {
	s.logger.Info("Initializing replication with role: %s", s.config.Role)
	s.replState = &ReplicationState{
		replicas: make(map[string]*Replica),
		role:     s.config.Role,
		offset:   0,
	}
	if s.replConfig.ReplicaOf != "" {
		s.logger.Info("Starting as replica of %s", s.replConfig.ReplicaOf)
		s.replState.role = "replica"
		return s.startAsReplica()
	}

	s.logger.Info("Starting as master on port %d", s.replConfig.Port)
	s.replState.role = "master"
	return s.startAsMaster()
}

func (s *Store) startAsReplica() error {
	conn, err := net.Dial("tcp", s.replConfig.ReplicaOf)
	if err != nil {
		s.logger.Error("Failed to connect to master: %v", err)
		return fmt.Errorf("failed to connect to master: %v", err)
	}

	s.logger.Info("Connected to master at %s", s.replConfig.ReplicaOf)
	s.replState.masterConn = conn

	replica := &Replica{
		conn:    conn,
		writer:  NewRESPWriter(conn),
		reader:  NewRESPReader(conn),
		lastACK: time.Now(),
	}

	// Perform handshake with the master
	if err := s.performHandshake(replica); err != nil {
		conn.Close()
		return fmt.Errorf("handshake failed: %v", err)
	}

	// Start Receive and Send routines
	// process incoming commands

	return nil
}

func (s *Store) startAsMaster() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.replConfig.Port))
	if err != nil {
		s.logger.Error("Failed to start listener on port %d: %v", s.replConfig.Port, err)
		return err
	}

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
	replica := &Replica{
		conn:    conn,
		writer:  NewRESPWriter(conn),
		reader:  NewRESPReader(conn),
		offset:  0,
		lastACK: time.Now(),
	}

	command, err := replica.reader.Read()
	if err != nil || command.Type != TypeArray ||
		len(command.Array) == 0 || command.Array[0].BulkString != PING {
		s.logger.Error("Invalid handshake from replica at %s: %v", conn.RemoteAddr(), err)
		conn.Close()
		return
	}
	s.logger.Info("Received PING from replica at %s", conn.RemoteAddr())

	pongResponse := Value{
		Type:       TypeBulkString,
		BulkString: "PONG",
	}
	if err := replica.writer.Write(pongResponse); err != nil {
		s.logger.Error("Failed to send PONG to replica at %s: %v", conn.RemoteAddr(), err)
		return
	}

	s.replState.mux.Lock()
	defer s.replState.mux.Unlock()
	s.replState.replicas[conn.RemoteAddr().String()] = replica

	s.logger.Info("Replica connected successfully from %s", conn.RemoteAddr())
}
func generateRandomID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
func (s *Store) performHandshake(replica *Replica) error {
	// Step 1: Send PING

	pingCmd := Value{
		Type: TypeArray,
		Array: []Value{
			{Type: TypeBulkString, BulkString: PING},
		},
	}
	s.logger.Info("Sending PING to master")
	if err := replica.writer.Write(pingCmd); err != nil {
		s.logger.Error("Failed to send PING during handshake: %v", err)
		return fmt.Errorf("failed to send PING during handshake")
	}

	// Wait for PONG response
	response, err := replica.reader.Read()
	if err != nil || !isPongResponse(response) {
		return fmt.Errorf("invalid PING response: expected PONG, got %v", response)
	}
	s.logger.Info("Received PONG from master")

	// Step 2: Send REPLCONF listening-port
	// Wait for OK response

	return nil
}
func isPongResponse(v Value) bool {
	return (v.Type == TypeString && v.String == "PONG") ||
		(v.Type == TypeBulkString && v.BulkString == "PONG")
}
