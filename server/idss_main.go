package main

import (
	"fmt"
	"github.com/krotik/eliasdb/eql"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

const dbPath string = "idss_graph_db"

func main() {
	//Enable logging by saving logs into server.log file
	logfile, err := os.OpenFile("idss.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error starting logging: ", err)
	}

	defer logfile.Close()
	log.SetOutput(&lumberjack.Logger{
		Filename:   "idss.log",
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     1, //days
	})

	// Add support for Graph Database using EliasDB
	// Initialize the graph database
	graphDB, err := graphstorage.NewDiskGraphStorage(dbPath, false)
	checkError(err)
	defer graphDB.Close()

	// Create graph manager
	graphManager := graph.NewGraphManager(graphDB)

	// Database Usage Example
	//TODO: Implement the graph DB here reflecting InnoCyPES use case
	//TODO: Remember to use Transaction for multiple operations to implement the all the required nodes and edges
	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("name", "Node1")
	node1.SetAttr("text", "The first stored node")
	graphManager.StoreNode("main", node1)

	node2 := data.NewGraphNode()
	node2.SetAttr(data.NodeKey, "456")
	node2.SetAttr(data.NodeKind, "mynode")
	node2.SetAttr(data.NodeName, "Node2")
	graphManager.StoreNode("main", node2)

	// Link the nodes
	edge := data.NewGraphEdge()
	edge.SetAttr(data.NodeKey, "abc")
	edge.SetAttr(data.NodeKind, "myedge")

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "node1")
	edge.SetAttr(data.EdgeEnd1Cascading, "true")

	edge.SetAttr(data.EdgeEnd2Key, node2.Key())
	edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "node2")
	edge.SetAttr(data.EdgeEnd2Cascading, "true")

	edge.SetAttr(data.NodeName, "Edge1")
	graphManager.StoreEdge("main", edge)

	// Traverse the nodes
	graphManager.Traverse("main", node1.Key(), node1.Kind(), "Father:Family:Child:Person", true)

	// Query the data using lookup
	n, err := graphManager.FetchNode("main", "123", "mynode")
	checkError(err)
	log.Println("Node fetched: ", n)

	log.Println("Querying data using EQL")
	result, err := eql.RunQuery("myQuery", "main", "get mynode where name = 'Node1'", graphManager)
	checkError(err)
	fmt.Println(result)

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
		log.Fatal("Fatal error: ", err.Error())
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
