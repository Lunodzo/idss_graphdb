package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	checkError(err)
	defer listener.Close()
	fmt.Println("Listening on port 8080")

	// Listen for an incoming connection
	for {
		conn, err := listener.Accept()
		checkError(err)
		fmt.Println("Accepted connection")
		go handleRequest(conn)
	}
}


// Function to check for errors
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	_, err := conn.Write([]byte("Welcome to the InnoCyPES Data Storage Service!\n"))
	checkError(err)

	// Read data from client
	buffer := make([]byte, 1024)
	length, err := conn.Read(buffer)
	checkError(err)

	// Convert byte array to string and compute sum
	data := string(buffer[:length])
	numbers := strings.Split(data, ",")
	var sum int
	for _, numberString := range numbers {
		number, err := strconv.Atoi(numberString)
		checkError(err)
		sum += number
	}

	// Send sum back to client
	_, err = conn.Write([]byte(fmt.Sprintf("Sum of numbers is %d", sum)))
	checkError(err)
}
