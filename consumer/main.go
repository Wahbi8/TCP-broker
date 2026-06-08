package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: consumer <topic>")
		return
	}

	topic := os.Args[1]

	conn, err := net.Dial("tcp", ":8090")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(fmt.Sprintf("SUB %s\n", topic)))
	if err != nil {
		fmt.Println("Error subscribing:", err)
		return
	}

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Connection closed:", err)
			return
		}
		fmt.Print(msg)
	}
}
