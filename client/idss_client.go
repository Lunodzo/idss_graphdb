package main

import (
	"log"
	"net"
	"os"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	// Enable logging and save logs into server.log file
	logfile, err := os.OpenFile("idss.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error starting logging: ", err)

	}

	defer logfile.Close()
	log.SetOutput(&lumberjack.Logger{
		Filename: "idss.log",
		MaxSize: 10, // megabytes
		MaxBackups: 3,
		MaxAge: 1, //days
	})

	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:8080")
	log.Println("Connecting to server...")

	// If there is an error connecting, print it and exit
	checkError(err)
	log.Println("Connected to server")
	defer conn.Close()

	/*
	Here you can program what the client should do after connecting to the server. In the current implementation, the client sends a list of numbers to the server and waits for the server to compute the sum of the numbers and send it back.
	*/

	//TODO: Implement the client's logic here focusing on the IDSS service
	//TODO: Compose queries to be sent to the server
	LOG_QUERY := "get client where name = 'Client1'"
	DATA_QUERY1 := "get client where name = 'Client1'"
	DATA_QUERY2 := "get client where name = 'Client2'"
	DATA_QUERY3 := "get client where name = 'Client3'"

	// Send the queries to the server
	log.Printf("Client %s queries to server...", conn.LocalAddr())
	database_queries(conn, LOG_QUERY, DATA_QUERY1, DATA_QUERY2, DATA_QUERY3)


	/* numbers := "1,2,3,4,5"
	_, err = conn.Write([]byte(numbers))
	checkError(err)

	buffer := make([]byte, 1024)
	length, err := conn.Read(buffer)
	checkError(err)
	fmt.Println(string(buffer[:length]))
	log.Println("Received the sum from server") */

	// Close the connection and clear everything
	conn.Close()
	log.Println("Connection closed")
}

// Function to check for errors
func checkError(err error) {
	if err != nil {
		log.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

func database_queries(conn net.Conn, log_query, query1, query2, query3 string) {
	queries := []string{log_query, query1, query2, query3}
	for _, query := range queries {
		_, err := conn.Write([]byte(query))
		checkError(err)

		buffer := make([]byte, 1024)
		length, err := conn.Read(buffer)
		checkError(err)
		log.Println(string(buffer[:length]))
	}
}

