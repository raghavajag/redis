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
	s.replState = &ReplicationState{
		role:     s.config.Role,
		replicas: make(map[string]*Replica),
		offset:   0,
	}
	if s.replConfig.ReplicaOf != "" {
		s.replState.role = "replica"
		return s.startAsReplica()
	}

	s.replState.role = "master"
	return s.startAsMaster()
}
func (s *Store) startAsMaster() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.replConfig.Port))
	if err != nil {
		return err
	}

	go s.acceptReplicas(listener)
	return nil
}
func (s *Store) startAsReplica() error {
	masterConn, err := net.Dial("tcp", s.replConfig.ReplicaOf)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}

	s.replState.masterConn = masterConn
	reader := NewRESPReader(masterConn)
	writer := NewRESPWriter(masterConn)

	// Send PING
	pingCmd := Value{
		Type: TypeArray,
		Array: []Value{
			{Type: TypeBulkString, BulkString: "PING"},
		},
	}
	if err := writer.Write(pingCmd); err != nil {
		masterConn.Close()
		return fmt.Errorf("failed to send PING: %v", err)
	}

	// Wait for PONG
	response, err := reader.Read()
	if (response.Type != TypeString && response.Type != TypeBulkString) ||
		(response.String != "PONG" && response.BulkString != "PONG") {
		masterConn.Close()
		return fmt.Errorf("invalid PONG response: %v", err)
	}
	fmt.Printf("Received PONG from master: %v\n", response)
	// Store master connection
	s.replState.masterConn = masterConn
	s.replState.role = "replica"

	// Start processing commands from master
	// go s.handleMasterConnection(conn)

	return nil

}

func (s *Store) acceptReplicas(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting replica: %v\n", err)
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

	// Wait for PING
	command, err := replica.reader.Read()
	if err != nil || command.Type != TypeArray ||
		len(command.Array) == 0 || command.Array[0].BulkString != PING {
		fmt.Printf("Invalid handshake from replica: %v\n", err)
		conn.Close()
		return
	}
	fmt.Printf("Received PING from replica: %v\n", command)

	// Send PONG
	pongResponse := Value{
		Type:       TypeBulkString,
		BulkString: "PONG",
	}
	if err := replica.writer.Write(pongResponse); err != nil {
		fmt.Printf("Failed to send PONG: %v\n", err)
		// conn.Close()
		return
	}

	// Add replica to list after successful handshake
	s.replState.mux.Lock()
	s.replState.replicas[conn.RemoteAddr().String()] = replica
	s.replState.mux.Unlock()

	fmt.Printf("Replica connected successfully: %s\n", conn.RemoteAddr())
}

func generateRandomID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
