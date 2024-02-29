package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	respReader := NewRESPReader(conn)
	for {
		commands, err := respReader.Read()
		fmt.Printf("%v\n", commands)
		result, err := handleCommand(commands)
		if err != nil {
			fmt.Println("Error handling command: ", err.Error())
			return
		}
		fmt.Printf("Write Command: %v", result)
	}
}
func handleCommand(commands Value) (Value, error) {
	cmds := commands.Array
	switch strings.ToUpper(cmds[0].BulkString) {
	case "PING":
		return Value{Type: TypeString, String: "PONG"}, nil
	default:
		return Value{}, fmt.Errorf("command not found")
	}
}
