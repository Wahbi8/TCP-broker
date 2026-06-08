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

		processMessage(ackMsg, conn)

		response := "ACK: Ok"
		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Printf("Server write error: %v", err)
			break
		}
	}
}

func processMessage(msg string, conn net.Conn) {

	switch {
	case strings.HasPrefix(msg, "SUB"):
		parts := strings.Split(msg, " ")
		topic := parts[1]

		mu.Lock()
		if conns, exists := brokerMap[topic]; exists {
			for _, c := range conns {
				if conn == c {
					fmt.Printf("Connection already exists: %v", conn)
					return
				}
			}
		}
		brokerMap[topic] = append(brokerMap[topic], conn)
		mu.Unlock()

	case strings.HasPrefix(msg, "PUB"):
		parts := strings.Split(msg, " ")
		topic := parts[1]

		mu.Lock()
		if conns, exists := brokerMap[topic]; exists {
			for _, conn := range conns {
				conn.Write([]byte(strings.Join(parts[2:], " ")))
			}
		}
		mu.Unlock()

	case strings.HasPrefix(msg, "UNSUB"):
		parts := strings.Split(msg, " ")
		topic := parts[1]

		temp := []net.Conn{}
		mu.Lock()
		for _, c := range brokerMap[topic] {
			if c != conn {
				temp = append(temp, c)
			}
		}
		brokerMap[topic] = temp
		mu.Unlock()
	}
}