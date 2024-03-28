package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/krotik/eliasdb/eql"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/natefinch/lumberjack.v2"
)

const DB_PATH = "idss_graph_db"


func Database_init() {
	log.Println("Entering Database Initialization function...")
	gob.Register(&timestamppb.Timestamp{})
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	if err != nil {
		log.Fatal("Error starting graph database: ", err)
	}
	defer GRAPH_DB.Close()
	log.Println("Graph database started initiated")

	// Create a new graph manager
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)

	// Creating instances from the proto file and marshalling the data
	//TODO: Parse actual data from the client
	log.Println("Creating instances from the proto schema and marshalling the data...")
	query_mng := &QueryManager{
		IdQuery:         12,
		Uqid:            123,
		Query:           "get client where name = 'Client1'",
		Ttl:             100,
		ArrivedAt:       timestamppb.New(time.Now()),
		SenderId:        "123",
		LocalExecution:  232,
		Completed:       timestamppb.New(time.Now()),
		SentBack:        false,
		Failed:          false,
	}
	query_data, err := proto.Marshal(query_mng)
	if err != nil {
		log.Fatal("Error marshalling data: ", err)
	}
	// Print the marshalled data
	log.Println("Marshalled query manager data: ", query_data)

	client1 := &Client{
		ClientId:    	123,
		ClientName: 	"Lunodzo",
		ContractNumber: 244,
		Power: 			343,
	}

	data1, err := proto.Marshal(client1)
	if err != nil {
		log.Fatal("Error marshalling data: ", err)
	}
	// Print the marshalled data
	log.Println("Marshalled client 1 data: ", data1)

	client2 := &Client{
		ClientId:    456,
		ClientName: "Juma",
		ContractNumber: 245,
		Power: 344,
	}
	data2, err := proto.Marshal(client2)
	if err != nil {
		log.Fatal("Error marshalling data: ", err)
	}
	// Print the marshalled data
	log.Println("Marshalled client 2 data: ", data2)

	client3 := &Client{
		ClientId:    789,
		ClientName: "Mwana",
		ContractNumber: 246,
		Power: 345,
	}
	data3, err := proto.Marshal(client3)
	if err != nil {
		log.Fatal("Error marshalling data: ", err)
	}
	// Print the marshalled data
	log.Println("Marshalled client 3 data: ", data3)

	consumption1 := &Consumption{
		Timestamp:    timestamppb.New(time.Now()),
		Measurement: 100,
		Client: &Client{ClientId: 123},
	}

	consumption_data1, err := proto.Marshal(consumption1)
	if err != nil {
		log.Fatal("Error marshalling data: ", err)
	}
	// Print the marshalled data
	log.Println("Marshalled consumption 1 data: ", consumption_data1)

	consumption2 := &Consumption{
		Timestamp:    timestamppb.New(time.Now()),
		Measurement: 200,
		Client: &Client{ClientId: 456},
	}

	consumption_data2, err := proto.Marshal(consumption2)
	if err != nil {
		log.Fatal("Error marshalling data: ", err)
	}
	// Print the marshalled data
	log.Println("Marshalled  consumption 2 data: ", consumption_data2)


	log.Println("Creating nodes and edges in the graph database based on marshalled data...")
	// Create transaction
	trans := graph.NewGraphTrans(GRAPH_MANAGER)

	// Create all nodes
	queriesNode := data.NewGraphNode()

	client1Node := data.NewGraphNode()
	client2Node := data.NewGraphNode()
	client3Node := data.NewGraphNode()

	consumption1Node := data.NewGraphNode()
	consumption2Node := data.NewGraphNode()

	// Create the node kinds
	queriesNode.SetAttr("kind", "query")
	queriesNode.SetAttr("data", query_data)
	queriesNode.SetAttr("key", "123")

	client1Node.SetAttr("key", "124")
	client1Node.SetAttr("kind", "client")
	client1Node.SetAttr("data", data1)

	client2Node.SetAttr("key", "125")
	client2Node.SetAttr("kind", "client")
	client2Node.SetAttr("data", data2)

	client3Node.SetAttr("key", "126")
	client3Node.SetAttr("kind", "client")
	client3Node.SetAttr("data", data3)

	consumption1Node.SetAttr("key", "127")
	consumption1Node.SetAttr("kind", "consumption")
	consumption1Node.SetAttr("data", consumption_data1)

	consumption2Node.SetAttr("key", "128")
	consumption2Node.SetAttr("kind", "consumption")
	consumption2Node.SetAttr("data", consumption_data2)


	// Insert nodes
	if err := trans.StoreNode("main", queriesNode); err != nil {
		log.Fatal(err)
	}

	if err := trans.StoreNode("main", client1Node); err != nil {
		log.Fatal(err)
	}

	if err := trans.StoreNode("main", client2Node); err != nil {
		log.Fatal(err)
	}

	if err := trans.StoreNode("main", client3Node); err != nil {
		log.Fatal(err)
	}

	if err := trans.StoreNode("main", consumption1Node); err != nil {
		log.Fatal(err)
	}

	if err := trans.StoreNode("main", consumption2Node); err != nil {
		log.Fatal(err)
	}

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		log.Fatal(err)
	}
	log.Println("Committed Node store transaction")

	trans = graph.NewGraphTrans(GRAPH_MANAGER)
	
	// Store edge
	edge := data.NewGraphEdge()
	edge.SetAttr(data.NodeKey, "client1consumption1")
	edge.SetAttr(data.NodeKind, "clientconsumption")

	edge.SetAttr(data.EdgeEnd1Key, client1Node.Key())
	edge.SetAttr(data.EdgeEnd1Kind, client1Node.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "client1")
	edge.SetAttr(data.EdgeEnd1Cascading, true)

	edge.SetAttr(data.EdgeEnd2Key, consumption1Node.Key())
	edge.SetAttr(data.EdgeEnd2Kind, consumption1Node.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "consumption")
	edge.SetAttr(data.EdgeEnd2Cascading, false)

	edge.SetAttr(data.NodeName, "Client1Consumption1")

	log.Println("Testing edge 1")
	if err := GRAPH_MANAGER.StoreEdge("main", edge); err != nil {
		log.Fatal(err)
	}
	log.Println("Created edge 1")

	// Edge 2
	edge2 := data.NewGraphEdge()
	edge2.SetAttr(data.NodeKey, "client2consumption2")
	edge2.SetAttr(data.NodeKind, "clientconsumption")

	edge2.SetAttr(data.EdgeEnd1Key, client2Node.Key())
	edge2.SetAttr(data.EdgeEnd1Kind, client2Node.Kind())
	edge2.SetAttr(data.EdgeEnd1Role, "client2")
	edge2.SetAttr(data.EdgeEnd1Cascading, true)

	edge2.SetAttr(data.EdgeEnd2Key, consumption2Node.Key())
	edge2.SetAttr(data.EdgeEnd2Kind, consumption2Node.Kind())
	edge2.SetAttr(data.EdgeEnd2Role, "consumption")
	edge2.SetAttr(data.EdgeEnd2Cascading, false)

	edge2.SetAttr(data.NodeName, "Client2Consumption2")

	log.Println("Testing edge 2")
	if err := GRAPH_MANAGER.StoreEdge("main", edge2); err != nil {
		log.Fatal(err)
	}

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		log.Fatal(err)
	}
	log.Println("Committed transaction")


	// Query the data using EQL
	log.Println("Querying data using EQL")
	var SELECT_QUERY = "get client where kind = 'client'"
	var SELECT_QUERY2 = "get consumption where kind = 'consumption'"
	result, err := eql.RunQuery("myQuery", "main", SELECT_QUERY, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}
	log.Println(result)

	result2, err := eql.RunQuery("myQuery", "main", SELECT_QUERY2, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}
	log.Println(result2)


}

func Database_query() {
	log.Println("Entering Database Query function...")

	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	CheckError(err)
	defer GRAPH_DB.Close()
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)


	//TODO: To be read from the client
	//TODO: Modify variables to be read from the client using conn.Read()
	// Query the data using EQL

	var SELECT_QUERY = "get client where kind = 'client'"
	var SELECT_QUERY2 = "get client where kind = 'consumption'"


	log.Println("Querying data using EQL (First Query)")
	result, err := eql.RunQuery("myQuery", "main", SELECT_QUERY, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
	}
	log.Println(result)

	// Iterate over the nodes in the result
	/* for _, row := range result.Rows() {
		// Get the marshalled data
		node, ok := row.(*data.Node)
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
		log.Println(client) */
	//}

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
		MaxAge: 30, //days
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
	//TODO: FIXME
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
