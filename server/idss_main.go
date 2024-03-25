package main

import (
	"fmt"
	"log"
	"gopkg.in/natefinch/lumberjack.v2"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	//Enable logging by saving logs into server.log file
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
	
	// Listen for incoming connections
	listener, err := net.Listen("tcp", ":8080")
	checkError(err)
	defer listener.Close()
	log.Println("Listening on port 8080")

	// Accept incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			continue // Continue listening for more connections without exiting the server
		}
		log.Println("Connection Accepted: ", conn.RemoteAddr().String())
		go handleRequest(conn)
	}
}

// Function to check for errors
func checkError(err error) {
	if err != nil {
		log.Fatal("Fatal error ", err.Error())
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

// A function to handle incoming requests from clients
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
