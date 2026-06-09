package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: consumer <topic>")
		return
	}

	topic := os.Args[1]

	for {
		conn, err := net.Dial("tcp", ":8090")
		if err != nil {
			fmt.Println("Error connecting:", err)
			time.Sleep(5* time.Second)
			continue
		}
		// defer conn.Close()
	
		// _, err = conn.Write([]byte(fmt.Sprintf("SUB %s\n", topic)))
		// if err != nil {
		// 	fmt.Println("Error subscribing:", err)
		// 	return
		// }
	
		conn.Write([]byte(fmt.Sprintf("SUB %s\n", topic)))
		fmt.Println("Connected and subscribed to:", topic)
		
		reader := bufio.NewReader(conn)
		for {
			msg, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Connection closed:", err)
				conn.Close()
				break
			}
			fmt.Print(msg)
		}
	}
}
