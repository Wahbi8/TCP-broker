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
// var msgBackup = make(map[int][]string)

type consumerState struct{
	conn net.Conn
	msgBackup []string
}

var consumers = make(map[int]*consumerState)

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
		idConsumer, err := strconv.Atoi(parts[2]) //pase to int
		if err != nil {
			fmt.Printf("failed to convert the id to int")
			return
		}

		mu.Lock()
		exists := false
		if consumerData, ok := consumers[idConsumer]; ok {
			if consumerData.conn == conn {
				exists = true
				sendLateMsgs(idConsumer)
			} else {
				consumerData.conn = conn
				exists = true
				sendLateMsgs(idConsumer)
			}
		}

		if !exists {
			consumers[idConsumer] = &consumerState{
				conn: conn,
				msgBackup: nil,
			}
		}
		
		if conns, exists := brokerMap[topic]; exists {
			for _, c := range conns {
				if conn == c {
					fmt.Printf("Connection already exists: %v", conn)
					mu.Unlock()
					return
				}
			}
		}
		brokerMap[topic] = append(brokerMap[topic], conn)
		mu.Unlock()

	case strings.HasPrefix(msg, "PUB"):
		parts := strings.Split(msg, " ")
		topic := parts[1]
		var msgBackup []string

		mu.Lock()
		if conns, exists := brokerMap[topic]; exists {
			for _, conn := range conns {
				_, err := conn.Write([]byte(strings.Join(parts[2:], " ")))
				if err != nil {
					for i := range consumers {
						if conn == consumers[i].conn {
							if state, e := consumers[i]; e {
								msgBackup = state.msgBackup
							}
							if len(msgBackup) > 9 {
								msgBackup = msgBackup[1:]
							}
							consumers[i] = &consumerState{
								conn: conn,
								msgBackup: append(msgBackup, msg),
							}
							break
						}
					}
				}
			}
		}

		// this is wrong i should do a for loop for each conn in the topic
		reader := bufio.NewReader(conn)
		rsp, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Issue reading response: %v", err)
			mu.Unlock()
			return
		}

		if strings.HasPrefix(rsp, "KO") {
			rspParts := strings.Split(rsp, " ")
			id, err := strconv.Atoi(rspParts[2])
			if err != nil {
				mu.Unlock()
				return
			}

			if state, exists := consumers[id]; exists {
				msgBackup = state.msgBackup
			}

			if len(msgBackup) > 9 {
				msgBackup = msgBackup[1:]
			}

			consumers[id] = &consumerState{
				conn: conn,
				msgBackup: append(msgBackup, msg),
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

func sendLateMsgs(idConsumer int) {
	 
	msgsNum := len(consumers[idConsumer].msgBackup)
	conn := consumers[idConsumer].conn
	if msgsNum > 0 {
		for i := 0; i < msgsNum; i++ {
			msg := consumers[idConsumer].msgBackup[i]
			parts := strings.Split(msg, " ")

			_, err := conn.Write([]byte(strings.Join(parts[2:], " ")))
			if err != nil {
				continue
			}
			consumers[idConsumer].msgBackup = consumers[idConsumer].msgBackup[1:]
		}
	}
}