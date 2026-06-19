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
	// "time"
)

var mu sync.RWMutex
var brokerMap = make(map[string][]*consumerState)

// var msgBackup = make(map[int][]string)

type consumerState struct {
	id 		  int 
	conn      net.Conn
	msgBackup []string
	queue chan string
	reconnectCh chan net.Conn
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
	for _, consumer := range brokerMap {
		for _, c := range consumer {
			c.conn.Close()
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

		response := "ACK: Ok\n"
		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Printf("Server write error: %v", err)
			break
		}
	}
}

func processMessage(msg string, conn net.Conn) {

	// var connSlice []net.Conn
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
		state := &consumerState{
			id:          idConsumer,
			conn:        conn,
			queue:       make(chan string, 100),
			reconnectCh: make(chan net.Conn, 1),
			msgBackup:   nil,
		}

		exists := false
		if consumerData, ok := consumers[idConsumer]; ok {
			if consumerData.conn == conn {
				exists = true
				// sendLateMsgs(idConsumer)
			} else {
				consumerData.reconnectCh <- conn
				exists = true
				// sendLateMsgs(idConsumer)
			}
		}

		if !exists {
			consumers[idConsumer] = state
			go state.delivery()
		}


		if consumer, exists := brokerMap[topic]; exists {
			for _, c := range consumer {
				if conn == c.conn {
					fmt.Printf("Connection already exists: %v", conn)
					mu.Unlock()
					return
				}
			}
		}
		brokerMap[topic] = append(brokerMap[topic], state)
		mu.Unlock()

	case strings.HasPrefix(msg, "PUB"):
		parts := strings.Split(msg, " ")
		topic := parts[1]
		// var msgBackup []string

		mu.Lock()
		if consumer, exists := brokerMap[topic]; exists {
			for _, c := range consumer {
				c.queue <- msg
			}
		}

		mu.Unlock()

	case strings.HasPrefix(msg, "UNSUB"):
		parts := strings.Split(msg, " ")
		topic := parts[1]

		temp := []*consumerState{}
		mu.Lock()
		for _, c := range brokerMap[topic] {
			if c.conn != conn {
				temp = append(temp, c)
			}
		}
		brokerMap[topic] = temp
		mu.Unlock()

	case strings.HasPrefix(msg, "LOG"):
		mu.Lock()
		var msgBackup []string
		rspParts := strings.Split(msg, " ")
		// timeLimit := 1 * time.Second

		if rspParts[1] == "KO" {
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
				conn:      conn,
				msgBackup: append(msgBackup, msg),
			}
		}
		mu.Unlock()
	}
}

func (consumer *consumerState) delivery(){
	
	for {
		select {
		case q := <- consumer.queue:
			parts := strings.Split(q, " ")

			_, err := consumer.conn.Write([]byte(strings.Join(parts[2:], " ") + "\n"))
				
			if err != nil {

				spaceLimit := 10 
				if len(consumer.msgBackup) >= spaceLimit {
					consumer.msgBackup = consumer.msgBackup[1:]
				}
				consumer.msgBackup = append(consumer.msgBackup, q)
			}
			// start timer
			// wait for the return from consumer (not sure where i should wait in here or in processMessage())

		case <-ticker.C:
			
		}
	}
}

func (consumer *consumerState) sendLateMsgs() {

	msgsNum := len(consumer.msgBackup)
	conn := consumer.conn
	if msgsNum > 0 {
		for i := 0; i < msgsNum; i++ {
			msg := consumer.msgBackup[i]
			parts := strings.Split(msg, " ")

			_, err := conn.Write([]byte(strings.Join(parts[2:], " ") + "\n"))
			if err != nil {
				continue
			}
			consumer.msgBackup = consumer.msgBackup[1:]
		}
	}
}
