package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

type Store struct {
	items map[string]string
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
	store := Store{items: make(map[string]string)}
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Listening on 6379...")
	for {
		conn, err := l.Accept()
		fmt.Printf("Accepted connection %s \n", conn.RemoteAddr().String())
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

func getHandler(store *Store, key string) Value {
	store.mux.Lock()
	defer store.mux.Unlock()
	val := store.items[key]
	return Value{Type: TypeString, String: val}
}
func setHandler(store *Store, key string, val string) Value {
	fmt.Printf("key: %s val: %s\n", key, val)
	store.mux.Lock()
	defer store.mux.Unlock()
	store.items[key] = val
	return Value{Type: TypeString, String: "OK"}
}
func handleCommand(commands Value, store *Store) (Value, error) {
	cmds := commands.Array
	switch strings.ToUpper(cmds[0].BulkString) {
	case PING:
		return Value{Type: TypeString, String: "PONG"}, nil
	case ECHO:
		return Value{Type: TypeString, String: cmds[1].BulkString}, nil
	case SET:
		return setHandler(store, cmds[1].BulkString, cmds[2].BulkString), nil
	case GET:
		return getHandler(store, cmds[1].BulkString), nil
	default:
		return Value{}, fmt.Errorf("command not found")
	}

}
