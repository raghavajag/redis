package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Dir        string
	DBFilename string
}
type V struct {
	expiry    time.Duration
	savedTime time.Time
	value     string
}
type Store struct {
	items  map[string]V
	mux    sync.Mutex
	config Config
}

const (
	PING = "PING"
	ECHO = "ECHO"
	GET  = "GET"
	SET  = "SET"
)

func NewStore(config Config) (*Store, error) {
	rdbReader, err := NewRDBReader(config.Dir, config.DBFilename)
	if err != nil {
		return nil, err
	}
	defer rdbReader.Close()

	database, err := rdbReader.ReadDatabase()
	if err != nil {
		return nil, err
	}

	items := make(map[string]V)
	for key, value := range database {
		items[key] = V{value: value}
	}

	return &Store{
		items:  items,
		config: config,
	}, nil
}
func main() {
	config := Config{}

	flag.StringVar(&config.Dir, "dir", "/tmp", "Directory for RDB file")
	flag.StringVar(&config.DBFilename, "dbfilename", "dump.rdb", "RDB filename")
	flag.Parse()

	store, err := NewStore(config)
	if err != nil {
		fmt.Printf("Failed to initialize store: %s\n", err)
		os.Exit(1)
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
		go handleConnection(conn, store)
	}
}
func handleConnection(conn net.Conn, store *Store) {
	defer conn.Close()
	respReader := NewRESPReader(conn)
	respWriter := NewRESPWriter(conn)

	for {
		commands, err := respReader.Read()
		fmt.Println("Commands:")
		printValue(commands, "  ")

		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
				return
			}
			fmt.Println("err = ", err)
			return
		}
		result, err := handleCommand(commands, store)
		fmt.Println("Response:")
		printValue(result, "  ")
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
	case "CONFIG":
		if len(cmds) < 3 || strings.ToUpper(cmds[1].BulkString) != "GET" {
			return Value{}, fmt.Errorf("invalid CONFIG command")
		}
		return configGetHandler(store, cmds[2].BulkString), nil

	default:
		return Value{}, fmt.Errorf("command not found")
	}
}
func printValue(v Value, indent string) {
	fmt.Printf("%sType: %s\n", indent, v.Type)
	if v.String != "" {
		fmt.Printf("%sString: %q\n", indent, v.String)
	}
	if v.Number != 0 {
		fmt.Printf("%sNumber: %d\n", indent, v.Number)
	}
	if v.BulkString != "" {
		fmt.Printf("%sBulkString: %q\n", indent, v.BulkString)
	}
	if len(v.Array) > 0 {
		fmt.Printf("%sArray:\n", indent)
		for i, item := range v.Array {
			fmt.Printf("%s  [%d]:\n", indent, i)
			printValue(item, indent+"    ")
		}
	}
}
