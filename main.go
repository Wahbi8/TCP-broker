package main

import (
	"net"
	"log"
	"bufio"
)

type listenerStruct struct{
	listener net.Listener
	success bool
}

func main() {
	
	

	
	defer listner.Close()

	for {
		conn, err := listner.Accept()
		if err != nil {
			log.Fatal("Error accepting connection: ", err)
			continue
		}

		go handleConnection(conn)
	}
}
func createListeners() []listenerStruct {
	listeners := []listenerStruct{} 
	listner1, err := net.Listen("tcp", ":8081")
	if err == nil {
		listeners = append(listeners, listenerStruct{
			listener: listner1,
			success: true,
		})
	} else {
		listeners = append(listeners, listenerStruct{
			listener: nil,
			success: false,
		})
		log.Fatal("Error listening on server '8081': ", err)
	}

	listner2, err := net.Listen("tcp", ":8082")
	if err == nil {
		listeners = append(listeners, listenerStruct{
			listener: listner2,
			success: true,
		})
	} else {
		listeners = append(listeners, listenerStruct{
			listener: nil,
			success: false,
		})
		log.Fatal("Error listening on server '8082': ", err)
	}

	listner3, err := net.Listen("tcp", ":8083")
	if err == nil {
		listeners = append(listeners, listenerStruct{
			listener: listner3,
			success: true,
		})
	} else {
		listeners = append(listeners, listenerStruct{
			listener: nil,
			success: false,
		})
		log.Fatal("Error listening on server '8083': ", err)
	}

	listner4, err := net.Listen("tcp", ":8084")
	if err == nil {
		listeners = append(listeners, listenerStruct{
			listener: listner4,
			success: true,
		})
	} else {
		listeners = append(listeners, listenerStruct{
			listener: nil,
			success: false,
		})
		log.Fatal("Error listening on server '8084': ", err)
	}
	
	return listeners
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("Read error: ", err)
		return
	}

	ackMsg := 
}
