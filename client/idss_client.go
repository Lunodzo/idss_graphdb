package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")

	// If there is an error connecting, print it and exit
	checkError(err)
	defer conn.Close()

	// Numbers to send to server
	numbers := "1,2,3,4,5"
	_, err = conn.Write([]byte(numbers))
	checkError(err)

	buffer := make([]byte, 1024)
	length, err := conn.Read(buffer)
	checkError(err)
	fmt.Println(string(buffer[:length]))
	fmt.Println("Received the sum from server")
}

// Function to check for errors
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

