package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
	"strings"
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
			time.Sleep(2* time.Second)
			continue
		}
		// defer conn.Close()
	
		// _, err = conn.Write([]byte(fmt.Sprintf("SUB %s\n", topic)))
		// if err != nil {
		// 	fmt.Println("Error subscribing:", err)
		// 	return
		// }
		id := 1
		
		conn.Write([]byte(fmt.Sprintf("SUB %s %v\n", topic, id)))
		fmt.Println("Connected and subscribed to:", topic)
		
		reader := bufio.NewReader(conn)
		for {
			msg, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Issue reading msg:", err)
				conn.Write([]byte(fmt.Sprintf("LOG KO %v\n", id)))
				conn.Close()
				break
			}

			if strings.HasPrefix(msg, "ACK:") {
				continue
			}

			parts := strings.Split(msg, " ")

			_, err = conn.Write([]byte(fmt.Sprintf("LOG OK %v %v\n", id, parts[0])))
			if err != nil {
				fmt.Println("Issue sending ack:", err)
				conn.Close()
				break
			}

			fmt.Print(msg)
		}
	}
}
