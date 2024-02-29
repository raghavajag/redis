package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

const (
	PING = "PING"
	ECHO = "ECHO"
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
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
				return
			}
			fmt.Printf("Error reading command: %v\n", err)
			continue
		}
		fmt.Printf("Received commands: %v\n", commands)

		result, err := handleCommand(commands)
		if err != nil {
			fmt.Println("Error handling command: ", err.Error())
			return
		}
		fmt.Printf("Sending response: %v\n", result)
		_, err = conn.Write([]byte(result.String + "\r\n"))
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			return
		}
	}
}
func handleCommand(commands Value) (Value, error) {
	cmds := commands.Array
	switch strings.ToUpper(cmds[0].BulkString) {
	case PING:
		return Value{Type: TypeString, String: "PONG"}, nil
	case ECHO:
		return Value{Type: TypeString, String: cmds[1].BulkString}, nil
	default:
		return Value{}, fmt.Errorf("command not found")
	}
}
