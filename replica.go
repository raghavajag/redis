package main

import (
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
	return nil
}
