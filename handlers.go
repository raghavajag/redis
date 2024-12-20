package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"time"
)

func setHandlerWithExpiry(store *Store, key string, val string, expiry uint64) Value {
	store.mux.Lock()
	defer store.mux.Unlock()

	duration := time.Duration(expiry) * time.Second

	store.items[key] = V{
		expiry:    duration,
		savedTime: time.Now(),
		value:     val,
	}

	fmt.Printf("Storing key: %s with value: %s and expiry: %v\n", key, val, duration)

	return Value{Type: TypeString, String: "OK"}
}

func setHandler(store *Store, key string, val string) Value {
	store.mux.Lock()
	fmt.Printf("Set key: %s val: %s\n", key, val)
	defer store.mux.Unlock()
	store.items[key] = V{savedTime: time.Now(), value: val}
	return Value{Type: TypeString, String: "OK"}
}

func getHandler(store *Store, key string) Value {
	store.mux.Lock()
	defer store.mux.Unlock()

	val, exists := store.items[key]
	if !exists {
		// Key does not exist in memory
		return Value{Type: TypeNullBulkString}
	}
	fmt.Printf("%v\n", val.value)
	// Check if the key has expired
	if val.expiry > 0 && time.Since(val.savedTime) > val.expiry {
		delete(store.items, key)               // Remove expired key from memory
		return Value{Type: TypeNullBulkString} // Return null for expired keys
	}

	// Return the value if it exists and has not expired
	return Value{Type: TypeString, String: val.value}
}

func configGetHandler(store *Store, param string) Value {
	var value string
	switch strings.ToLower(param) {
	case "dir":
		value = store.config.Dir
	case "dbfilename":
		value = store.config.DBFilename
	default:
		return Value{Type: TypeArray, Array: []Value{}} // Empty array for unknown parameter
	}

	return Value{
		Type: TypeArray,
		Array: []Value{
			{Type: TypeBulkString, BulkString: param},
			{Type: TypeBulkString, BulkString: value},
		},
	}
}

func handleReplConfCommand(store *Store, args []Value) Value {
	if len(args) < 2 || args[0].Type != TypeBulkString || args[1].Type != TypeBulkString {
		return Value{Type: TypeBulkString, BulkString: "-ERR invalid REPLCONF arguments"}
	}

	subCommand := strings.ToLower(args[0].BulkString)
	switch subCommand {
	case "listening-port":
		port := args[1].BulkString
		store.logger.Info("Replica reported listening port: %s", port)
		return Value{Type: TypeBulkString, BulkString: "OK"}

	case "ack":
		offset := args[1].BulkString
		store.logger.Info("Received ACK with offset %s", offset)
		return Value{Type: TypeBulkString, BulkString: "OK"}

	default:
		return Value{Type: TypeBulkString, BulkString: "-ERR unknown REPLCONF subcommand"}
	}
}

func saveHandler(store *Store) Value {
	err := store.Save()
	if err != nil {
		return Value{Type: TypeString, String: "-ERR " + err.Error()}
	}
	return Value{Type: TypeString, String: "+OK"}
}

func infoComandHandler(store *Store) Value {
	info := strings.Builder{}
	info.WriteString("# Replication\r\n")
	info.WriteString(fmt.Sprintf("role:%s\r\n", store.replState.role))

	if store.replState.role == "master" {
		store.replState.mux.RLock()
		info.WriteString(fmt.Sprintf("connected_slaves:%d\r\n", len(store.replState.replicas)))
		for addr := range store.replState.replicas {
			info.WriteString(fmt.Sprintf("slave:%s\r\n", addr))
		}
		store.replState.mux.RUnlock()
	} else {
		info.WriteString(fmt.Sprintf("master_host:%s\r\n", store.config.MasterAddr))
		// Additional details for replica
		if store.replState.masterConn != nil {
			info.WriteString(fmt.Sprintf("master_link_status:up\r\n"))
		} else {
			info.WriteString(fmt.Sprintf("master_link_status:down\r\n"))
		}
	}

	return Value{Type: TypeBulkString, BulkString: info.String()}
}

func (s *Store) Save() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	fmt.Println("Saving current items:", s.items) // Debugging log

	path := fmt.Sprintf("./%s/%s", s.config.Dir, s.config.DBFilename)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	err = s.writeRDB(writer)
	if err != nil {
		return err
	}

	return writer.Flush()
}

func (s *Store) writeRDB(w *bufio.Writer) error {
	// Write header
	_, err := w.Write([]byte("REDIS0012"))
	if err != nil {
		return err
	}

	// Write key-value pairs
	for key, v := range s.items {
		err := s.writeKeyValue(w, key, v)
		if err != nil {
			return err
		}
	}

	// Write EOF
	_, err = w.Write([]byte{0xFF})
	return err
}

func (s *Store) writeKeyValue(w *bufio.Writer, key string, v V) error {
	// Write key
	_, err := w.Write([]byte{0x00})
	if err != nil {
		return err
	}
	err = s.writeString(w, key)
	if err != nil {
		return err
	}

	// Write value
	err = s.writeString(w, v.value)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) writeString(w *bufio.Writer, str string) error {
	fmt.Printf("Writing string %s\n", str)
	// Write string length
	size := len(str)
	err := binary.Write(w, binary.BigEndian, uint8(size))
	if err != nil {
		return err
	}

	// Write string data
	_, err = w.WriteString(str)
	return err
}
