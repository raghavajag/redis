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

	// Perform handshake and initial synchronization here

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
		conn:   conn,
		writer: NewRESPWriter(conn),
		reader: NewRESPReader(conn),
	}

	// Handle PING
	// if err := s.handleReplicaPing(replica); err != nil {
	// 	conn.Close()
	// 	return
	// }

	// Add replica to list
	s.replState.mux.Lock()
	s.replState.replicas[conn.RemoteAddr().String()] = replica
	s.replState.mux.Unlock()

	fmt.Println("Replica connected:", conn.RemoteAddr())

	// Start replica command processing
	// go s.handleReplicaCommands(replica)
}
func generateRandomID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
