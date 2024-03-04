package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	// Uncomment this block to pass the first stage
	// "net"
	// "os"
)

type V struct {
	expiry    time.Duration
	savedTime time.Time
	value     string
}
type Store struct {
	items map[string]V
	mux   sync.Mutex
}

const (
	PING = "PING"
	ECHO = "ECHO"
	GET  = "GET"
	SET  = "SET"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	store := Store{items: make(map[string]V)}
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn, &store)
	}
}
func handleConnection(conn net.Conn, store *Store) {
	defer conn.Close()
	respReader := NewRESPReader(conn)
	respWriter := NewRESPWriter(conn)

	for {
		commands, err := respReader.Read()
		fmt.Printf("Commands: %v\n", commands)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
				return
			}
			fmt.Println("err = ", err)
			return
		}
		result, err := handleCommand(commands, store)
		fmt.Printf("Response: %v\n", result)
		if err != nil {
			fmt.Printf("Failed to handle command %s", err.Error())
		}
		err = respWriter.Write(result)
		if err != nil {
			fmt.Printf("Failed to write response: %s\n", err.Error())
			return
		}
	}

}
func setHandlerWithExpiry(store *Store, key string, val string, expiry uint64) Value {
	store.mux.Lock()
	defer store.mux.Unlock()
	store.items[key] = V{expiry: time.Duration(expiry), savedTime: time.Now(), value: val}
	return Value{Type: TypeString, String: "OK"}
}
func setHandler(store *Store, key string, val string) Value {
	fmt.Printf("key: %s val: %s\n", key, val)
	store.mux.Lock()
	defer store.mux.Unlock()
	store.items[key] = V{savedTime: time.Now(), value: val}
	return Value{Type: TypeString, String: "OK"}
}
func getHandler(store *Store, key string) Value {
	store.mux.Lock()
	defer store.mux.Unlock()
	val := store.items[key]
	if time.Since(val.savedTime) > val.expiry {
		delete(store.items, key)
	}
	if val.value == "" {
		return Value{Type: TypeNullBulkString}
	}
	return Value{Type: TypeString, String: val.value}
}
func handleCommand(commands Value, store *Store) (Value, error) {
	cmds := commands.Array
	switch strings.ToUpper(cmds[0].BulkString) {
	case PING:
		return Value{Type: TypeString, String: "PONG"}, nil
	case ECHO:
		return Value{Type: TypeString, String: cmds[1].BulkString}, nil
	case SET:
		key := cmds[1].BulkString
		val := cmds[2].BulkString
		if len(cmds) == 4 {
			exp := cmds[3].Number
			return setHandlerWithExpiry(store, key, val, uint64(exp)), nil
		}
		return setHandler(store, key, val), nil
	case GET:
		key := cmds[1].BulkString
		return getHandler(store, key), nil
	default:
		return Value{}, fmt.Errorf("command not found")
	}
}
