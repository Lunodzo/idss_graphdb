package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/krotik/eliasdb/eql"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	"google.golang.org/protobuf/proto"
	"gopkg.in/natefinch/lumberjack.v2"
)

const DB_PATH = "../server/idss_graph_db"
const PORT = ":8080"


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
		MaxAge: 30, //days
	})
}

// Function to query the database
//NOTE: This function is not used in the main function, it is for testing
func Database_query() {
	log.Println("Entering Database Query function...")

	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	CheckError(err)
	defer GRAPH_DB.Close()
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)


	//TODO: To be read from the client
	//TODO: Modify variables to be read from the client using conn.Read()
	// Query the data using EQL

	log.Println("Querying data using EQL")
	var SELECT_QUERY = "get client where kind = 'client'"
	var SELECT_QUERY2 = "get consumption where kind = 'consumption'"


	result, err := eql.RunQuery("myQuery", "main", SELECT_QUERY, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}
	log.Println(result)

	// Iterate over the rows in the result
	for _, row := range result.Rows() {
		// Get the marshalled data
		data, ok := row[1].([]byte)
		if !ok {
			log.Println("Row is not a node")
			continue
		}

		// Unmarshal the data
		client := &Client{}
		err := proto.Unmarshal(data, client)
		if err != nil {
			log.Println("Error unmarshalling data: ", err)
			continue
		}
		log.Println(client)
	}


	result2, err := eql.RunQuery("myQuery", "main", SELECT_QUERY2, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}
	log.Println(result2)

	// Iterate over the rows in the result2
	for _, row := range result2.Rows() {
		// Get the marshalled data
		data, ok := row[1].([]byte)
		if !ok {
			log.Println("Row is not a node")
			continue
		}

		// Unmarshal the data
		consumption := &Consumption{}
		err := proto.Unmarshal(data, consumption)
		if err != nil {
			log.Println("Error unmarshalling data: ", err)
			continue
		}
		log.Println(consumption)
	}
}


func main() {
	// Enable logging and save logs into server.log file
	setupLogging()
	//Database_init()

	// Listen for incoming connections
	listener, err := net.Listen("tcp", PORT)
	CheckError(err)
	defer listener.Close()
	log.Println("Server listening on port 8080")

	// Add support for Graph Database using EliasDB
	// Calling functions from idss_db_init.go
	//Database_init()

	// Running sample queries
	//Database_query()

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


// A function to handle incoming requests from clients
func handleRequest(conn net.Conn) {
	defer conn.Close()

	// Get the client address
	CLIENT_ADDRESS := conn.RemoteAddr().String()
	log.Println("Client connected: ", CLIENT_ADDRESS)

	_, err := conn.Write([]byte("Welcome to the InnoCyPES Data Storage Service!\n"))
	if err != nil {
		log.Println("Error sending data to client: ", err)
		conn.Close()
	}

	// Create a buffer to hold the incoming data
	buffer := make([]byte, 4096)

	for{
		length, err := conn.Read(buffer)
		if err != nil {
			log.Println("Error reading data from client: ", err)
			conn.Close()
			break
		}

		// Check if the client wants to exit
		if strings.TrimSpace(string(buffer[:length])) == "exit" {
			log.Println("Client exited: ", CLIENT_ADDRESS)
			conn.Close()
			break
		}

		// Print the received data
		log.Printf("Received data from client %s: %s\n", CLIENT_ADDRESS, string(buffer[:length]))

		// Process the received query
		processCommand(string(buffer[:length]), conn)
		log.Println("Command processed successfully")

		// Clear the buffer
		buffer = make([]byte, 4096)
	}											
}

// Function to check for errors
func CheckError(err error) {
	if err != nil {
		log.Fatal("Fatal error: ", err.Error())
		os.Exit(1)
	}
}


// Function to process the received command
func processCommand(command string, conn net.Conn) {
	/* Pass the command and decide what to do with it
	if the command starts with ADD_CLIENT,  extract the client data from the 
	command and call a function to add the client to the database. */
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	CheckError(err)
	defer GRAPH_DB.Close()
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)

	log.Printf("Processing command: %s", command)

	// Check command type and call appropiate function
	if strings.HasPrefix(command, "ADD_CLIENT") {
		// Extract client data from the command
		client_data := strings.Split(command, " ")
		client_id, _ := strconv.Atoi(client_data[1])
		client_name := client_data[2]
		contract_number, _ := strconv.Atoi(client_data[3])
		power, _ := strconv.Atoi(client_data[4])

		// Add the client to the database
		addClient(client_id, client_name, contract_number, power)
	}else if strings.HasPrefix(command, "ADD_CONSUMPTION") {
		// Extract consumption data from the command
		consumption_data := strings.Split(command, " ")
		consumption_id, _ := strconv.Atoi(consumption_data[1])
		client_id, _ := strconv.Atoi(consumption_data[2])
		consumption, _ := strconv.Atoi(consumption_data[3])
		timestamp := consumption_data[4]

		// Add the consumption to the database
		addConsumption(consumption_id, client_id, consumption, timestamp)
	}else if strings.HasPrefix(command, "GET_CLIENT") {
		// Remove the GET_CLIENT prefix from the command
		client_query := strings.TrimPrefix(command, "GET_CLIENT ")

		getClient(client_query, GRAPH_MANAGER, conn)
	}else if strings.HasPrefix(command, "GET_CONSUMPTION") {
		// Extract consumption id from the command
		consumption_data := strings.Split(command, " ")
		consumption_id, _ := strconv.Atoi(consumption_data[1])

		// Get the consumption from the database
		getConsumption(consumption_id, GRAPH_MANAGER)
	}else {
		log.Println("Invalid command", command)
		// Send response back to client
		_, err := conn.Write([]byte("Invalid command "+command+"\n"))
		CheckError(err)
	}
}

//TODO: Implement the following functions
func getConsumption(consumption_id int, GER *graph.Manager) {
	// Get the consumption from the database
	// Query the data using EQL
	SELECT_QUERY := "get consumption where id = " + strconv.Itoa(consumption_id)
	result, err := eql.RunQuery("myQuery", "main", SELECT_QUERY, GER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}
	log.Println(result)

	// Iterate over the rows in the result
	for _, row := range result.Rows() {
		// Get the marshalled data
		data, ok := row[1].([]byte)
		if !ok {
			log.Println("Row is not a node")
			continue
		}

		// Unmarshal the data
		consumption := &Consumption{}
		err := proto.Unmarshal(data, consumption)
		if err != nil {
			log.Println("Error unmarshalling data: ", err)
			continue
		}
		fmt.Println(consumption)
	}
}

func getClient(client_query string, GER *graph.Manager, conn net.Conn) {
	// Get the client from the database 
	log.Println("Getting client...")
	result, err := eql.RunQuery("myQuery", "main", client_query, GER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}

	// Iterate over the rows in the result
	for _, row := range result.Rows() {
		// Get the marshalled data
		data, ok := row[1].([]byte)
		if !ok {
			log.Println("Row is not a node")
			continue
		}

		// Unmarshal the data
		client := &Client{}
		err := proto.Unmarshal(data, client)
		if err != nil {
			log.Println("Error unmarshalling data: ", err)
			continue
		}


		// Send the results to the client
		_, err = conn.Write([]byte("\nResults: \n"+client.String()+" \n"))
		if err != nil {
			log.Println("Error sending unmarshalled result to client: ", err)
			conn.Close()
		}
	}
}

func addConsumption(consumption_id, client_id, consumption int, timestamp string) {
	// Add received consumption data to the database (consider using eliasdb and protobuf)
	// Create a new consumption
	
}

func addClient(client_id int, client_name string, contract_number, power int) {
	// Add received client data to the database (consider using eliasdb and protobuf)
	// Create a new client
	client := &Client{
		ClientId:    int32(client_id),
		ClientName: client_name,
		ContractNumber: int64(contract_number),
		Power: int32(power),
	}

	// Marshal the client data
	dataa, err := proto.Marshal(client)
	if err != nil {
		log.Fatal("Error marshalling data: ", err)
	}

	// Print the marshalled data
	log.Println("Marshalled client data: ", dataa)

	// Add the client to the database
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	CheckError(err)
	defer GRAPH_DB.Close()
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)

	// Store the client in the database
	clientNode := data.NewGraphNode()
	clientNode.SetAttr("kind", "client")
	clientNode.SetAttr("data", dataa)

	// Store the client node
	GRAPH_MANAGER.StoreNode("data", clientNode)

	log.Println("Client added successfully")
}