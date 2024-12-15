package main

import (
	"bufio"
	"encoding/binary"
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
	Dir             string
	DBFilename      string
	Port            int    // Regular Redis port
	ReplicationPort int    // Port for replication
	Role            string // "master" or "replica"
	MasterAddr      string // Master address if running as replica
}
type V struct {
	expiry    time.Duration
	savedTime time.Time
	value     string
}
type Store struct {
	items      map[string]V
	mux        sync.Mutex
	config     Config
	replConfig ReplicationConfig
	replState  *ReplicationState
	logger     *Logger
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
	replConfig := ReplicationConfig{
		Port:      config.ReplicationPort,
		ReplicaOf: config.MasterAddr,
		ReplID:    generateReplicaID(),
	}
	store := &Store{
		items:      items,
		config:     config,
		replConfig: replConfig,
		logger:     NewLogger(true),
	}
	if err := store.initReplication(); err != nil {
		return nil, err
	}
	return store, nil
}

func main() {
	config := Config{}

	flag.StringVar(&config.Dir, "dir", "tmp", "Directory for RDB file")
	flag.StringVar(&config.DBFilename, "dbfilename", "dump.rdb", "RDB filename")
	flag.IntVar(&config.Port, "port", 6379, "Redis port")
	flag.IntVar(&config.ReplicationPort, "replication-port", 6380, "Replication port")
	flag.StringVar(&config.Role, "role", "master", "Role: master or replica")
	flag.StringVar(&config.MasterAddr, "master", "", "Master address (host:port)")
	flag.Parse()

	store, err := NewStore(config)
	if err != nil {
		fmt.Printf("Failed to initialize store: %s\n", err)
		os.Exit(1)
	}
	// Start regular Redis server
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
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

func handleInfoCommand(store *Store) Value {
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
	case "INFO":
		return handleInfoCommand(store), nil
	case "SAVE":
		return saveHandler(store), nil
	case "REPLCONF":
		return handleReplConfCommand(store, cmds[1:]), nil

	default:
		return Value{}, fmt.Errorf("command not found")
	}
}

func saveHandler(store *Store) Value {
	err := store.Save()
	if err != nil {
		return Value{Type: TypeString, String: "-ERR " + err.Error()}
	}
	return Value{Type: TypeString, String: "+OK"}
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

func (s *Store) Save() error {
	s.mux.Lock()
	defer s.mux.Unlock()

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

func (s *Store) writeString(w *bufio.Writer, str string) error {
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
