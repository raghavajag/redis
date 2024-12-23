package main

import (
	"sync"
	"time"
)

type CommandEntry struct {
	command     Value
	offset      int64
	timestamp   time.Time
	waitingACKs map[string]bool // Track ACKs by replicaID
	done        chan struct{}   // Signal when all required ACKs received
}

type PropagationQueue struct {
	commands chan *CommandEntry
	done     chan struct{}
}

type AckTracker struct {
	entries map[int64]*CommandEntry // Track commands by offset
	mux     sync.RWMutex
}

func (s *Store) startPropagationWorker() {
	for {
		select {
		case cmd := <-s.propQueue.commands:
			s.propagateCommandToReplicas(cmd)
		case <-s.propQueue.done:
			return
		}
	}
}

func (s *Store) propagateCommandToReplicas(entry *CommandEntry) {
	s.replState.mux.RLock()
	defer s.replState.mux.RUnlock()

	for _, replica := range s.replState.replicas {
		go func(r *Replica) {
			select {
			case r.outgoing <- entry.command:
				s.logger.Info("Propagated command to replica %s at offset %d", r.ID, entry.offset)
				entry.waitingACKs[r.ID] = false // Initialize ACK status as false
			default:
				s.logger.Error("Failed to propagate command to replica %s: channel full", r.ID)
			}
		}(replica)
	}

	// If no replicas are connected, mark as done immediately
	if len(s.replState.replicas) == 0 {
		close(entry.done)
	}
}

func (s *Store) queueCommandForPropagation(cmd Value) *CommandEntry {
	s.mux.Lock()
	offset := s.cmdOffset + 1
	s.cmdOffset++
	s.mux.Unlock()

	entry := &CommandEntry{
		command:     cmd,
		offset:      offset,
		timestamp:   time.Now(),
		waitingACKs: make(map[string]bool),
		done:        make(chan struct{}),
	}

	// Initialize waitingACKs for all connected replicas
	s.replState.mux.RLock()
	for id := range s.replState.replicas {
		entry.waitingACKs[id] = false
	}
	s.replState.mux.RUnlock()

	// Add entry to AckTracker
	s.ackTracker.mux.Lock()
	s.ackTracker.entries[offset] = entry
	s.ackTracker.mux.Unlock()

	// Enqueue command for propagation
	select {
	case s.propQueue.commands <- entry:
		s.logger.Info("Queued command for propagation, offset: %d", offset)
	default:
		s.logger.Error("Propagation queue full, command dropped")
		close(entry.done)
	}

	return entry
}
