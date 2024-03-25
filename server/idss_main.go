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
	"gopkg.in/natefinch/lumberjack.v2"
)

const DB_PATH = "idss_graph_db"


func Database_init() {
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	if err != nil {
		log.Fatal("Error starting graph database: ", err)
	}
	defer GRAPH_DB.Close()
	//TODO: Add node and edge definitions here
	// Database Usage Example
	//TODO: Implement the graph DB here reflecting InnoCyPES use case
	//TODO: Remember to use Transaction for multiple operations to implement the all the required nodes and edges
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)
	// Create transaction
	trans := graph.NewGraphTrans(GRAPH_MANAGER)

	// Store node 1
	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "client")
	node1.SetAttr("name", "Client1")
	node1.SetAttr("contract", "244")
	node1.SetAttr("location", "Nairobi")
	node1.SetAttr("phone", "0712345678")
	node1.SetAttr("power", "3432")

	if err := trans.StoreNode("main", node1); err != nil {
		log.Fatal(err)
	}
	log.Println("Stored node 1")

	// Store node 2
	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "client")
	node2.SetAttr("name", "Client2")
	node2.SetAttr("contract", "245")
	node2.SetAttr("location", "Mombasa")
	node2.SetAttr("phone", "0712345679")
	node2.SetAttr("power", "3433")

	if err := trans.StoreNode("main", node2); err != nil {
		log.Fatal(err)
	}
	log.Println("Stored node 2")

	// Store node 3
	node3 := data.NewGraphNode()
	node3.SetAttr("key", "789")
	node3.SetAttr("kind", "client")
	node3.SetAttr("name", "Client3")
	node3.SetAttr("contract", "246")
	node3.SetAttr("location", "Kisumu")
	node3.SetAttr("phone", "0712345680")
	node3.SetAttr("power", "3434")

	if err := trans.StoreNode("main", node3); err != nil {
		log.Fatal(err)
	}
	log.Println("Stored node 3")

	// Store node 4
	node4 := data.NewGraphNode()
	node4.SetAttr("key", "101")
	node4.SetAttr("kind", "consumption")
	node4.SetAttr("time", "2021-09-01T00:00:00Z")
	node4.SetAttr("value", "100")
	node4.SetAttr("client", "123")

	if err := trans.StoreNode("main", node4); err != nil {
		log.Fatal(err)
	}
	log.Println("Stored node 4")

	// Store node 5
	node5 := data.NewGraphNode()
	node5.SetAttr("key", "102")
	node5.SetAttr("kind", "consumption")
	node5.SetAttr("time", "2021-09-01T00:00:00Z")
	node5.SetAttr("value", "200")
	node5.SetAttr("client", "456")

	if err := trans.StoreNode("main", node5); err != nil {
		log.Fatal(err)
	}
	log.Println("Stored node 5")

	// Store node 6
	node6 := data.NewGraphNode()
	node6.SetAttr("key", "103")
	node6.SetAttr("kind", "consumption")
	node6.SetAttr("time", "2021-09-01T00:00:00Z")
	node6.SetAttr("value", "300")
	node6.SetAttr("client", "789")

	if err := trans.StoreNode("main", node6); err != nil {
		log.Fatal(err)
	}
	log.Println("Stored node 6")

	if err := trans.Commit(); err != nil {
		log.Fatal(err)
	}
	log.Println("Committed transaction")

	trans = graph.NewGraphTrans(GRAPH_MANAGER)
	
	// Store edge
	edge := data.NewGraphEdge()
	edge.SetAttr(data.NodeKey, "client1consumption1")
	edge.SetAttr(data.NodeKind, "clientconsumption")

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "client")
	edge.SetAttr(data.EdgeEnd1Cascading, true)

	edge.SetAttr(data.EdgeEnd2Key, node4.Key())
	edge.SetAttr(data.EdgeEnd2Kind, node4.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "consumption")
	edge.SetAttr(data.EdgeEnd2Cascading, false)

	log.Println("Testing edge 1")
	edge.SetAttr(data.NodeName, "Client1Consumption1")
	if err := GRAPH_MANAGER.StoreEdge("main", edge); err != nil {
		log.Fatal(err)
	}
	log.Println("Created edge 1")

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		log.Fatal(err)
	}
	// node1 := data.NewGraphNode()
	// node1.SetAttr("key", "123")
	// node1.SetAttr("kind", "mynode")
	// node1.SetAttr("name", "Node1")
	// node1.SetAttr("text", "The first stored node")
	// GRAPH_MANAGER.StoreNode("main", node1)

	// node2 := data.NewGraphNode()
	// node2.SetAttr(data.NodeKey, "456")
	// node2.SetAttr(data.NodeKind, "mynode")
	// node2.SetAttr(data.NodeName, "Node2")
	// GRAPH_MANAGER.StoreNode("main", node2)

	// // Link the nodes
	// edge := data.NewGraphEdge()
	// edge.SetAttr(data.NodeKey, "abc")
	// edge.SetAttr(data.NodeKind, "myedge")

	// edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	// edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	// edge.SetAttr(data.EdgeEnd1Role, "node1")
	// edge.SetAttr(data.EdgeEnd1Cascading, "true")

	// edge.SetAttr(data.EdgeEnd2Key, node2.Key())
	// edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
	// edge.SetAttr(data.EdgeEnd2Role, "node2")
	// edge.SetAttr(data.EdgeEnd2Cascading, "true")

	// edge.SetAttr(data.NodeName, "Edge1")
	// GRAPH_MANAGER.StoreEdge("main", edge)

	// // Traverse the nodes
	// GRAPH_MANAGER.Traverse("main", node1.Key(), node1.Kind(), "Father:Family:Child:Person", true)
}

func Database_query() {
	//TODO: Add query logic here
	// Query the data using lookup
	var SELECT_QUERY = "get client where name = 'Client1'"
	var SELECT_QUERY2 = "get client where name = 'Client2'"
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	CheckError(err)
	defer GRAPH_DB.Close()
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)

	log.Println("Querying data using EQL (First Query)")
	result, err := eql.RunQuery("myQuery", "main", SELECT_QUERY, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}
	log.Println(result)

	log.Println("Querying data using EQL (Second Query)")
	result2, err := eql.RunQuery("myQuery", "main", SELECT_QUERY2, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}
	log.Println(result2)
}


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

	// Listen for incoming connections
	listener, err := net.Listen("tcp", ":8080")
	CheckError(err)
	defer listener.Close()
	log.Println("Listening on port 8080")

	// Add support for Graph Database using EliasDB
	// Calling functions from idss_db_init.go
	Database_init()

	// Running sample queries
	Database_query()

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
func CheckError(err error) {
	if err != nil {
		log.Fatal("Fatal error: ", err.Error())
		os.Exit(1)
	}
}

// A function to handle incoming requests from clients
func handleRequest(conn net.Conn) {
	defer conn.Close()
	_, err := conn.Write([]byte("Welcome to the InnoCyPES Data Storage Service!\n"))
	CheckError(err)

	// Read data from client
	buffer := make([]byte, 1024)
	length, err := conn.Read(buffer)
	CheckError(err)

	// Convert byte array to string and compute sum
	data := string(buffer[:length])
	numbers := strings.Split(data, ",")
	var sum int
	for _, numberString := range numbers {
		number, err := strconv.Atoi(numberString)
		CheckError(err)
		sum += number
	}

	// Send sum back to client
	_, err = conn.Write([]byte(fmt.Sprintf("Sum of numbers is %d", sum)))
	CheckError(err)
}
