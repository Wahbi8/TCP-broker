package main

import (
	"net"
	"log"
	"bufio"
	"strings"
	"fmt"
	"sync"
)

var mu sync.RWMutex
var brokerMap = make(map[string][]net.Conn)

type serviceType int
const (
	Order serviceType = iota
	Email 
	Inventory
	Analytics
)

func main() {
	
	listener, err := net.Listen("tcp", ":8090")
    if err != nil {
        log.Fatal("Error listening:", err)
    }

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {

	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Read error: %v", err)
			break
		}
	
		ackMsg := strings.TrimSpace(message)

		err = processMessage(ackMsg)
		if err != nil {
			log.Printf("Message processing error: %v", err)
			break
		}

		response := fmt.Sprintf("ACK: %s\n", ackMsg)
		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Printf("Server write error: %v", err)
			break
		}
	}
}

func processMessage(msg string) error {

	switch {
	case strings.HasPrefix(msg, "PUB"):
		// to do
	case strings.HasPrefix(msg, "SUB"):
		// todo
	case strings.HasPrefix(msg, "UNSUB"):
		//todo
	}

	return nil
}