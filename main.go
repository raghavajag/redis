package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
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
	propQueue  *PropagationQueue
	ackTracker *AckTracker
	cmdOffset  int64
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
		return nil, fmt.Errorf("failed to create RDB reader: %v", err)
	}
	defer rdbReader.Close()

	items := make(map[string]V)

	if rdbReader.file != nil {
		database, err := rdbReader.ReadDatabase()
		if err != nil {
			return nil, fmt.Errorf("failed to read database: %v", err)
		}

		// Populate items map with data from RDB
		for key, value := range database {
			items[key] = V{
				value:     value,
				savedTime: time.Now(),
			}
		}
	}

	// Initialize replication config
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
		propQueue: &PropagationQueue{
			commands: make(chan *CommandEntry, 10000),
			done:     make(chan struct{}),
		},
		ackTracker: &AckTracker{
			entries: make(map[int64]*CommandEntry),
		},
		cmdOffset: 0,
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
		// fmt.Println("Commands:")
		// printValue(commands, "  ")
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
				return
			}
			fmt.Println("err = ", err)
			return
		}
		result, err := handleCommand(commands, store)
		// fmt.Println("Response:")
		// printValue(result, "  ")
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

func handleCommand(commands Value, store *Store) (Value, error) {
	cmds := commands.Array

	// Get command type
	cmdType := strings.ToUpper(cmds[0].BulkString)

	// Execute the command
	var result Value
	var err error

	switch cmdType {
	case PING:
		result = Value{Type: TypeString, String: "PONG"}
	case ECHO:
		result = Value{Type: TypeString, String: cmds[1].BulkString}

	case SET:
		key := cmds[1].BulkString
		val := cmds[2].BulkString

		if len(cmds) == 4 {
			exp, err := strconv.Atoi(cmds[3].BulkString)
			if err != nil {
				return Value{}, fmt.Errorf("invalid expiry value: %s", cmds[3].BulkString)
			}
			result = setHandlerWithExpiry(store, key, val, uint64(exp))
		} else if len(cmds) == 5 && strings.ToUpper(cmds[3].BulkString) == "EX" {
			exp, err := strconv.Atoi(cmds[4].BulkString) // Convert bulk string to integer
			if err != nil {
				return Value{}, fmt.Errorf("invalid expiry value: %s", cmds[4].BulkString)
			}
			result = setHandlerWithExpiry(store, key, val, uint64(exp))
		} else {
			result = setHandler(store, key, val)
		}

	case GET:
		key := cmds[1].BulkString
		result = getHandler(store, key)
	case "CONFIG":
		if len(cmds) < 3 || strings.ToUpper(cmds[1].BulkString) != "GET" {
			return Value{}, fmt.Errorf("invalid CONFIG command")
		}
		result = configGetHandler(store, cmds[2].BulkString)
	case "INFO":
		result = infoComandHandler(store)
	case "SAVE":
		result = saveHandler(store)
	default:
		return Value{}, fmt.Errorf("command not found")
	}

	// Propagate write commands to replicas if running as master
	if store.replState.role == "master" && isWriteCommand(cmdType) {
		store.queueCommandForPropagation(commands)
	}

	return result, err
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
