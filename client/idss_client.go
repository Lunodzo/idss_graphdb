package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	// Enable logging and save logs into server.log file
	setupLogging()

	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:8080")
	checkError(err)
	log.Printf("Connected to server %s", conn.RemoteAddr())
	defer conn.Close()

	// Create a buffer for incomings from the server
	buffer := make([]byte, 4096)
	reader := bufio.NewReader(conn)
	
	for {
		// Read query from the user
		fmt.Println("Enter your query or 'exit' to quit: ")
		query, _ := bufio.NewReader(os.Stdin).ReadString('\n')

		// Exit the loop if the server sends the exit message
		if strings.TrimSpace(query) == "exit" {
			log.Println("Connection closed by the client...")
			defer conn.Close()
			break
		}

		// Send a query to the server
		_, err = conn.Write([]byte(query))
		checkError(err)
		log.Println("Query sent to server")

		// Read the response from the server
		response, err := reader.Read(buffer)
		if err != nil {
			fmt.Println("Error reading data: ", err)
			log.Println("Error reading data: ", err)
			break
		}
		fmt.Println(string(buffer[:response]))
	}
}

func setupLogging() {
	logfile, err := os.OpenFile("../server/idss.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error starting logging: ", err)

	}

	defer logfile.Close()
	log.SetOutput(&lumberjack.Logger{
		Filename: "../server/idss.log",
		MaxSize: 10, // megabytes
		MaxBackups: 3,
		MaxAge: 1, //days
	})
}

// Function to check for errors
func checkError(err error) {
	if err != nil {
		log.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

