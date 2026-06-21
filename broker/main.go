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
	"time"
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
	inFlight map[int]*inFlightStruct
	ackCh       chan int 
	msgSeq      int 
}


type inFlightStruct struct{
	msg string
	time time.Time
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

		if !strings.HasPrefix(ackMsg, "LOG") {
			response := "ACK: Ok\n"
			conn.Write([]byte(response))
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
			inFlight:    make(map[int]*inFlightStruct),
			ackCh:       make(chan int, 100),
			msgSeq:      0,  
		}

		exists := false
		if consumerData, ok := consumers[idConsumer]; ok {
			if consumerData.conn == conn {
				exists = true
			} else {
				consumerData.reconnectCh <- conn
				mu.Unlock()
				return
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
		rspParts := strings.Split(msg, " ")

		// if rspParts[1] == "KO" {
		// 	id, err := strconv.Atoi(rspParts[2])
		// 	if err != nil {
		// 		mu.Unlock()
		// 		return
		// 	}

		// 	state, exists := consumers[id]
		// 	if !exists {
		// 		mu.Unlock()
		// 		return
		// 	}

		// 	if len(state.msgBackup) >= 10 {
		// 		state.msgBackup = state.msgBackup[1:]
		// 	}
		// 	state.msgBackup = append(state.msgBackup, msg)
			
		// }

		if rspParts[1] == "OK" {
			id, _ := strconv.Atoi(rspParts[2])
			msgID, _ := strconv.Atoi(rspParts[3]) 

			if state, ok := consumers[id]; ok {
				state.ackCh <- msgID
			}
		}
		mu.Unlock()
	}
}

func (consumer *consumerState) delivery(){
	
	ticker := time.NewTicker(2 * time.Second) 
	defer ticker.Stop() 
	for {
		select {
		case q := <- consumer.queue:
			consumer.msgSeq++ 
			currentMsgID := consumer.msgSeq

			parts := strings.Split(q, " ")
			payload := strings.Join(parts[2:], " ")

			outMsg := fmt.Sprintf("%d %s\n", currentMsgID, payload)

			_, err := consumer.conn.Write([]byte(outMsg))
				
			if err != nil {

				spaceLimit := 10 
				if len(consumer.msgBackup) >= spaceLimit {
					consumer.msgBackup = consumer.msgBackup[1:]
				}
				consumer.msgBackup = append(consumer.msgBackup, q)
			} else {
				consumer.inFlight[currentMsgID] = &inFlightStruct{
					msg:  q,
					time: time.Now(),
				}
			}

		case reConn := <- consumer.reconnectCh:
			consumer.conn = reConn
			consumer.sendLateMsgs()
		
		case ackedMsgID := <-consumer.ackCh:
			delete(consumer.inFlight, ackedMsgID)
			
		case <-ticker.C:
			now := time.Now()
			for id, inFlight := range consumer.inFlight {
				if now.Sub(inFlight.time) > 2*time.Second {
					parts := strings.Split(inFlight.msg, " ")
					payload := strings.Join(parts[2:], " ")

					_, err := consumer.conn.Write([]byte(payload + "\n"))
					if err != nil {
						delete(consumer.inFlight, id)
						if len(consumer.msgBackup) >= 10 {
							consumer.msgBackup = consumer.msgBackup[1:]
						}
						consumer.msgBackup = append(consumer.msgBackup, inFlight.msg)
					} else {
						consumer.inFlight[id] = &inFlightStruct{
							msg: inFlight.msg,
							time: time.Now(),
						}
					}
				}
			}
		}
		
	}
}

func (consumer *consumerState) sendLateMsgs() {
	msgs := consumer.msgBackup
	consumer.msgBackup = nil

	for _, msg := range msgs {
		consumer.msgSeq++
		currentMsgID := consumer.msgSeq

		parts := strings.Split(msg, " ")
		payload := strings.Join(parts[2:], " ")

		outMsg := fmt.Sprintf("%d %s\n", currentMsgID, payload)
		_, err := consumer.conn.Write([]byte(outMsg))

		if err != nil {
			if len(consumer.msgBackup) >= 10 {
				consumer.msgBackup = consumer.msgBackup[1:]
			}
			consumer.msgBackup = append(consumer.msgBackup, msg)
		} else {
			consumer.inFlight[currentMsgID] = &inFlightStruct{
				msg:  msg,
				time: time.Now(),
			}
		}
	}
}
