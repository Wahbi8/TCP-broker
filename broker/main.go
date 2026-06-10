package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var mu sync.RWMutex
var brokerMap = make(map[string][]net.Conn)
var msgBackup = make(map[int][]string)

type consumerIdentification struct{
	id int
	conn net.Conn
}

var consumerIds = []consumerIdentification{}

func main() {
	
	listener, err := net.Listen("tcp", ":8090")
    if err != nil {
        log.Fatal("Error listening:", err)
    }

	// defer listener.Close()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
		
			go handleConnection(conn)
		}
	}()

	<-sigChan
	fmt.Println("\nShutting down gracefully...")

	listener.Close()
	
	mu.Lock()
	for _, conns := range brokerMap {
		for _, c := range conns {
			c.Close()
		}
	}
	mu.Unlock()
	
	fmt.Println("Server stopped.")
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
		idConsumer, err := strconv.Atoi(parts[2])
		if err != nil {
			fmt.Printf("failed to convert the id to int")
			return
		}

		mu.Lock()
		exists := false
		for i := range consumerIds {
			if idConsumer == consumerIds[i].id && conn == consumerIds[i].conn {
				exists = true
				break
			} else if idConsumer == consumerIds[i].id && conn != consumerIds[i].conn {
				consumerIds[i].conn = conn
				exists = true
				break
			}
		}

		if !exists {
			consumerIds = append(consumerIds, consumerIdentification{
				id: idConsumer,
				conn: conn,
			})
		}
		
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
				_, err := conn.Write([]byte(strings.Join(parts[2:], " ")))
				if err != nil {
					for i := range consumerIds {
						if conn == consumerIds[i].conn {
							msgBackup[consumerIds[i].id] = append(msgBackup[consumerIds[i].id], msg) 
						}
					}
				}
			}
		}
		// read the return from consumer if err add the msg to the backup
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