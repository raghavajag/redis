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
		role:     s.config.Role,
		replicas: make(map[string]*Replica),
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
	masterConn, err := net.Dial("tcp", s.replConfig.ReplicaOf)
	if err != nil {
		s.logger.Error("Failed to connect to master: %v", err)
		return fmt.Errorf("failed to connect to master: %v", err)
	}

	s.logger.Info("Connected to master at %s", s.replConfig.ReplicaOf)
	s.replState.masterConn = masterConn
	reader := NewRESPReader(masterConn)
	writer := NewRESPWriter(masterConn)

	pingCmd := Value{
		Type: TypeArray,
		Array: []Value{
			{Type: TypeBulkString, BulkString: "PING"},
		},
	}
	if err := writer.Write(pingCmd); err != nil {
		masterConn.Close()
		s.logger.Error("Failed to send PING to master: %v", err)
		return fmt.Errorf("failed to send PING: %v", err)
	}

	response, err := reader.Read()
	if err != nil || (response.Type != TypeString && response.Type != TypeBulkString) ||
		(response.String != "PONG" && response.BulkString != "PONG") {
		masterConn.Close()
		s.logger.Error("Invalid PONG response from master: %v", err)
		return fmt.Errorf("invalid PONG response: %v", err)
	}
	s.logger.Info("Received PONG from master")
	s.replState.masterConn = masterConn
	s.replState.role = "replica"

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
