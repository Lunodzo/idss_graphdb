package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"

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

	// Read query from the user
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Commands: \n ADD_CLIENT to ADD data \n ADD_CONSUMPTION to ADD cosnumption\n GET_CLIENT to retrieve clients\n GET_CONSUMPTION to retrieve consumption\n \n")

	fmt.Println("Enter your query: ")
	query, _ := reader.ReadString('\n')

	// Send a query to the server
	_, err = conn.Write([]byte(query))
	checkError(err)
	log.Println("Query sent to server")


	// Create a buffer for incomings from the server
	buffer := make([]byte, 4096)
	log.Println("Entering the loop to read data from the server...")
	for {
		length, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading data: ", err)
			log.Println("Error reading data: ", err)
			break
		}
		fmt.Println(string(buffer[:length]))

		// Exit the loop if the server sends the exit message
		if string(buffer[:length]) == "exit" {
			log.Println("Server closed connection")
			conn.Close()
			break
		}
	}
	

	/*
	Here you can program what the client should do after connecting to the server. In the current implementation, the client sends a list of numbers to the server and waits for the server to compute the sum of the numbers and send it back.
	*/
	// Create the queries to be sent to the server, that follows the format of the server


	//LOG_QUERY := "get client where name = 'Client1'"
	//ADD_CLIENT := "ADD_CLIENT 123 Client2 244 343"
	//ADD_CONSUMPTION := "ADD_CONSUMPTION 123 123 343 2021-09-01 12:00:00"
	GET_CLIENT := "GET_CLIENT get client where kind = 'client'"
	//GET_CONSUMPTION := "GET_CONSUMPTION get consumption where consumption_id = 123"

	// Send the queries to the server
	log.Printf("Client %s queries to server...", conn.LocalAddr())

	/* _, err = conn.Write([]byte(LOG_QUERY))
	checkError(err) */

	/* _, err = conn.Write([]byte(ADD_CLIENT))
	checkError(err)

	_, err = conn.Write([]byte(ADD_CONSUMPTION))
	checkError(err) */

	_, err = conn.Write([]byte(GET_CLIENT))
	checkError(err)

	/* _, err = conn.Write([]byte(GET_CONSUMPTION))
	checkError(err) */
	_, err = conn.Write([]byte("exit"))
	checkError(err)

	fmt.Println("Client queries response received from server")
}

func setupLogging() {
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
}

// Function to check for errors
func checkError(err error) {
	if err != nil {
		log.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

/* func database_queries(conn net.Conn, log_query, query1, query2, query3 string) {
	queries := []string{log_query, query1, query2, query3}
	for _, query := range queries {
		_, err := conn.Write([]byte(query))
		checkError(err)

		buffer := make([]byte, 1024)
		length, err := conn.Read(buffer)
		checkError(err)
		log.Println(string(buffer[:length]))
	}
} */

