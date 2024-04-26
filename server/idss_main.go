package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/krotik/eliasdb/eql"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/graphstorage"
	//"google.golang.org/protobuf/proto"
	"gopkg.in/natefinch/lumberjack.v2"
)

const DB_PATH = "../server/idss_graph_db"
const PORT = ":8080"

func setupLogging() {
	logfile, err := os.OpenFile("idss.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	CheckError(err)

	defer logfile.Close()
	log.SetOutput(&lumberjack.Logger{
		Filename:   "idss.log",
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     2, //days
	})
}

func main() {
	// Enable logging and save logs into idss.log file
	setupLogging()

	// Listen for incoming connections
	listener, err := net.Listen("tcp", PORT)
	CheckError(err)
	defer listener.Close()
	log.Println("Server listening on port ", PORT)

	// Calling functions from idss_db_init.go ONCE to create the database nodes and edges
	// SAMPLE_DATA := "sample_data.json"
	// Database_init(SAMPLE_DATA)


	// Accept incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			continue // Continue listening for more connections without exiting the server
		}
		// Get the client address
		CLIENT_ADDRESS := conn.RemoteAddr().String()
		log.Println("Client connected: ", CLIENT_ADDRESS)
		go handleRequest(conn, CLIENT_ADDRESS)
	}
}

// A function to handle incoming requests from clients
func handleRequest(conn net.Conn, CLIENT_ADDRESS string) {
	defer conn.Close()

	// Create a buffer to hold the incoming data
	for {
		buffer := make([]byte, 4096)
		length, err := conn.Read(buffer)
		
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("Client %s exit: ", CLIENT_ADDRESS)
			}else{
				log.Println("Error reading input from client: ", err)
			}
			conn.Close()
			break
		}else if length > 0 {
			log.Printf("Received input from client %s: %s", CLIENT_ADDRESS, string(buffer[:length]))
			// Send a response to the client
			_, err := conn.Write([]byte("Input received by the server, sending to process\n"))
			CheckError(err)
		}else if strings.ContainsAny(string(buffer[:length]), "\x00"){
			log.Println("Input contains non-printable characters")
			_, err := conn.Write([]byte("Input contains non-printable characters\n"))
			CheckError(err)
			conn.Close()
			break
		}else if strings.TrimSpace(string(buffer[:length])) == "exit"{
			log.Printf("Client %s exit: ", CLIENT_ADDRESS)
			 _, err := conn.Write([]byte("Client exit\n"))
            CheckError(err, "Writing client exit message")
			conn.Close()
			break
		}else if length > len(buffer){
			log.Println("Input exceeds buffer size")
			_, err := conn.Write([]byte("Input exceeds buffer size\n"))
			CheckError(err, "Writing input exceeds buffer size")
			conn.Close()
			break
		} else {
			log.Println("No data received from client: ", CLIENT_ADDRESS)
			_, err := conn.Write([]byte("No data received\n"))
			CheckError(err, "Writing no data received")
		}

		// Process the received query
		processCommand(string(buffer[:length]), conn)
	}
}

// Function to check for errors
func CheckError(err error, message ...string) {
	if err != nil {
		log.Fatal("Error during operation '%s': %v", message, err)
	}
}

// Function to process the received command
func processCommand(command string, conn net.Conn) {
	// Add support for Graph Database using EliasDB
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	CheckError(err)
	defer GRAPH_DB.Close()
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)
	log.Printf("Processing command: %s", command)

	/* 
	SAMPLE QUERIES get and lookup
	TRAVERSAL SYNTAX: 
	<source role>:<relationship kind>:<destination role>:<destination kind>

	SAMPLE QUERIES
	>> get Consumption traverse ::: where name = "Alice" (WORKING)
	>> lookup client '3' traverse ::: (WORKING)
	>> get Client traverse owner:belongs_to:usage:Consumption (WORKING)
	

	COUNT FUNCTION IN EQL
	>> get Client where @count(owner:belongs_to:usage:Consumption) > 2 (WORKING)
	>> get Client where @count(owner:belongs_to:usage:Consumption) > 4 (WORKING)
	        NOTE: Only Alice and Bob will be returned as they have more than 4 connections
					They have more 4 consumption nodes each
	>> get Consumption where @count(:::) > 0 (WORKING) 
			NOTE: Returns all the nodes in the graph who are connected to each other 
			and its count is greater than 0, so any connected node will be returned
	*/
	result, err := eql.RunQuery("myQuery", "main", command, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
		// This error will be printed on the client side sometimes because of quoting issues in the query. 
		// It is recommended to use double quotes for the query
		_, err := conn.Write([]byte("Error querying data: " + err.Error() + "\n"))
		CheckError(err)
		return
	}

	// Check if the result is empty
	if len(result.Rows()) == 0 {
		if _, err := conn.Write([]byte("No result found\n")) ; err != nil {
			log.Println("Error sending data to client: ", err)
			conn.Close()
		}
		return
	}

	// Print the result
	// Results can be printed in any format as per the requirement
	log.Printf("Query result: %v", result)
	_, err = conn.Write([]byte("Command processed successfully\n"))
	CheckError(err)
}