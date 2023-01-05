package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func handleRequest(conn net.Conn) {
	for {
		buf := make([]byte, 64)
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("disconnected", conn.RemoteAddr())
				return
			}
			fmt.Println("Error read response: ", err.Error())
			os.Exit(1)
		}

		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error write response: ", err.Error())
			os.Exit(1)
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}
