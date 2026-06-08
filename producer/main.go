package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: producer <topic> <message>")
		return
	}

	topic := os.Args[1]
	message := strings.Join(os.Args[2:], " ")

	conn, err := net.Dial("tcp", ":8090")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()

	payload := fmt.Sprintf("PUB %s %s\n", topic, message)
	_, err = conn.Write([]byte(payload))
	if err != nil {
		fmt.Println("Error sending:", err)
		return
	}

	ack, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response:", err)
		return
	}

	fmt.Print("Broker response: ", ack)
}
