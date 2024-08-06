package main

import (
	"encoding/json"
	"os"

	eliasdb "github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	//"google.golang.org/protobuf/proto"
	//"google.golang.org/protobuf/encoding/protojson"
	//"google.golang.org/protobuf/types/known/timestamppb"
)

// Function to initialise the database
func Database_init(filename string, DB_PATH string) {
	log.Info("Entering the database initialisation function...")

	type Node struct {
		Kind           string `json:"kind"`
		Key            int    `json:"key,omitempty"`
		Name           string `json:"name,omitempty"`
		ContractNumber int    `json:"contract_number,omitempty"`
		Power          int    `json:"power,omitempty"`
		Timestamp      int    `json:"timestamp,omitempty"`
		Measurement	   int    `json:"measurement,omitempty"`
	}

	type Edge struct {
		Key           string `json:"key"`
		Kind          string `json:"kind"`
		End1Key       int    `json:"end1key"`
		End1Kind      string `json:"end1kind"`
		End1Role      string `json:"end1role"`
		End1Cascading bool   `json:"end1cascading"`
		End2Key       int    `json:"end2key"`
		End2Kind      string `json:"end2kind"`
		End2Role      string `json:"end2role"`
		End2Cascading bool   `json:"end2cascading"`
	}

	type GraphData struct {
		Nodes []Node `json:"nodes"`
		Edges []Edge `json:"edges"`
	}


	// Create an instance of graph storage in the disk
	// (false parameter means that the database is not read-only)
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	if err != nil {
		log.Error("Error creating graph storage: ", err)
	}

	defer GRAPH_DB.Close()
	log.Info("Database storage initialised successfully")

	// Create graph manager
	GRAPH_MANAGER := eliasdb.NewGraphManager(GRAPH_DB)

	// Create instances from the protofile and marshal the data
	//log.Println("Creating instances from the protofile...")

	// Query manager instance creation with empty values for the first time to create the instance
	/* query_mng := &QueryManager{
		IdQuery:         12, // Generate autoincremented ID
		Uqid:            123, // Generate unique ID based on the client port number and the current time
		Query:           "get client where name = 'Client1'",
		Ttl:             100,
		ArrivedAt:       timestamppb.New(time.Now()),
		SenderId:        "123", // Call client address coming from the established connection
		LocalExecution:  232,
		Completed:       false,
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
	log.Println("Marshalled  consumption 2 data: ", consumption_data2) */

	log.Info("Creating nodes and edges in the graph database based on json data...")
	
	// Read the sample data
	myData, err := os.ReadFile(filename)

	if err != nil {
		log.Fatal(err)
	}

	// Unmarshal the json data
	var graphData GraphData
	err = json.Unmarshal(myData, &graphData) 
	if err != nil {
		log.Fatal("Can't unmarshal json data", err)
	}

	// Create transaction
	trans := eliasdb.NewGraphTrans(GRAPH_MANAGER)

	// Create nodes
	log.Info("Creating client and consumption nodes...")
	for _, node := range graphData.Nodes {
		graphNode := data.NewGraphNode()
		// Start creating nodes of type Client and Consumption
		if node.Kind == "Client" {
			
			graphNode.SetAttr("key", node.Key)
			graphNode.SetAttr("kind", node.Kind)
			graphNode.SetAttr("name", node.Name)
			graphNode.SetAttr("contract", node.ContractNumber)
			graphNode.SetAttr("power", node.Power)
		}else if node.Kind == "Consumption" {
			//log.Println("Creating consumption node...")
			graphNode.SetAttr("key", node.Key)
			graphNode.SetAttr("kind", node.Kind)
			graphNode.SetAttr("timestamp", node.Timestamp)
			graphNode.SetAttr("measurement", node.Measurement)
		}else {
			log.Println("Unknown node kind")
		}

		trans.StoreNode("main", graphNode)
		//GRAPH_MANAGER.StoreNode("main", graphNode)

		// Verify that nodes have been created
		//log.Println("Created node: ", node.Key)
	}


	// Create edges
	log.Println("Creating edges...")
	for _, edge := range graphData.Edges {
		
		graphEdge := data.NewGraphEdge()
		graphEdge.SetAttr("key", edge.Key)
		graphEdge.SetAttr("kind", edge.Kind)
		graphEdge.SetAttr("end1key", edge.End1Key)
		graphEdge.SetAttr("end1kind", edge.End1Kind)
		graphEdge.SetAttr("end1role", edge.End1Role)
		graphEdge.SetAttr("end1cascading", edge.End1Cascading)
		graphEdge.SetAttr("end2key", edge.End2Key)
		graphEdge.SetAttr("end2kind", edge.End2Kind)
		graphEdge.SetAttr("end2role", edge.End2Role)
		graphEdge.SetAttr("end2cascading", edge.End2Cascading)

		trans.StoreEdge("main", graphEdge)
	}

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		log.Fatal(err)
	}
	/* queriesNode := data.NewGraphNode()
	queriesNode.SetAttr("kind", "query")
	queriesNode.SetAttr("key", "123")
	queriesNode.SetAttr("data", query_data)

	client1Node := data.NewGraphNode()
	client1Node.SetAttr("key", "124")
	client1Node.SetAttr("kind", "client")
	client1Node.SetAttr("name", "client1")
	client1Node.SetAttr("data", data1)

	client2Node := data.NewGraphNode()
	client2Node.SetAttr("key", "125")
	client2Node.SetAttr("kind", "client")
	client2Node.SetAttr("name", "client2")
	client2Node.SetAttr("data", data2)

	client3Node := data.NewGraphNode()
	client3Node.SetAttr("key", "126")
	client3Node.SetAttr("kind", "client")
	client3Node.SetAttr("name", "client3")
	client3Node.SetAttr("data", data3)

	consumption1Node := data.NewGraphNode()
	consumption1Node.SetAttr("key", "127")
	consumption1Node.SetAttr("kind", "consumption")
	consumption1Node.SetAttr("name", "consumption1")
	consumption1Node.SetAttr("data", consumption_data1)

	consumption2Node := data.NewGraphNode()
	consumption2Node.SetAttr("key", "128")
	consumption2Node.SetAttr("kind", "consumption")
	consumption2Node.SetAttr("name", "consumption2")
	consumption2Node.SetAttr("data", consumption_data2) */
	log.Info("Committed Node and Edge store transaction")
}
