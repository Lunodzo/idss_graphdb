package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"time"

	eliasdb "github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
)

func GenFakeDataAndInit(dataFilePath string, dbPath string, graphDB graphstorage.Storage, graphManager *eliasdb.Manager) error {
	// Execute python script to generate fake data and store it in the peer-specific data file
	cmd := exec.Command("python3", "generate_data.py", dataFilePath)
	err := cmd.Run()
	if err != nil {
		return err
	}

	// Read the generated data from the JSON file specific to this peer
	myData, err := os.ReadFile(dataFilePath)
	if err != nil {
		return err
	}

	// Initialize the graph database with the generated data
	return LoadGraphData(string(myData), dbPath, graphDB, graphManager)
}

func LoadGraphData(dataa string, dbPath string, graphDB graphstorage.Storage, graphManager *eliasdb.Manager) error {
	trans := eliasdb.NewGraphTrans(graphManager)

	// Unmarshal the JSON data
	var myData map[string]interface{}
	err := json.Unmarshal([]byte(dataa), &myData)
	if err != nil {
		return err
	}

	// Process nodes and edges
	nodes := myData["nodes"].([]interface{})
	edges := myData["edges"].([]interface{})

	// Store nodes
	for _, node := range nodes {
		nodeData := node.(map[string]interface{})
		graphNode := data.NewGraphNode()

		for key, value := range nodeData {
			graphNode.SetAttr(key, value)
		}
		trans.StoreNode("main", graphNode)
	}

	// Store edges
	for _, edge := range edges {
		edgeData := edge.(map[string]interface{})
		graphEdge := data.NewGraphEdge()

		for key, value := range edgeData {
			graphEdge.SetAttr(key, value)
		}
		trans.StoreEdge("main", graphEdge)
	}

	// Commit the transaction to store the nodes and edges
	return trans.Commit()
}

// Function to initialise the database
/* func Database_init(filename string, DB_PATH string, GRAPH_DB graphstorage.Storage, GRAPH_MANAGER *eliasdb.Manager) {
	logger.Info("Entering the data loading function...")

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

	logger.Info("Creating nodes and edges in the graph database based on json data...")
	
	// Read the sample data
	myData, err := os.ReadFile(filename)

	if err != nil {
		// if there is an empty file, return
		if os.IsNotExist(err) {
			logger.Info("No data file found, peer will start with empty graph")
			//myData = []byte(`{"nodes":[],"edges":[]}`)
			return
		}else{
			logger.Fatal(err)
		}
	}

	// Unmarshal the json data
	var graphData GraphData
	err = json.Unmarshal(myData, &graphData) 
	if err != nil {
		logger.Fatal("Can't unmarshal json data", err)
	}

	// Create transaction
	trans := eliasdb.NewGraphTrans(GRAPH_MANAGER)

	// Create nodes
	logger.Info("Creating client and consumption nodes...")
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
			//logger.Println("Creating consumption node...")
			graphNode.SetAttr("key", node.Key)
			graphNode.SetAttr("kind", node.Kind)
			graphNode.SetAttr("timestamp", node.Timestamp)
			graphNode.SetAttr("measurement", node.Measurement)
		}else {
			logger.Infof("Unknown node kind")
		}

		trans.StoreNode("main", graphNode)
	}


	// Create edges
	logger.Infof("Creating edges...")
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
		logger.Fatal(err)
	}
	logger.Info("Committed Node and Edge store transaction")
} */

// Function to initialise the Query Manager node
func QueryManager_init(GRAPH_MANAGER *eliasdb.Manager) {
	logger.Info("Creating the Query Manager node...")

	trans := eliasdb.NewGraphTrans(GRAPH_MANAGER)

	// Create the query node
	queryNode := data.NewGraphNode()
	queryNode.SetAttr("key", "sample") // key is a string
	queryNode.SetAttr("kind", "Query") // kind is a string
	queryNode.SetAttr("name", "Query") // name is a string
	queryNode.SetAttr("query_string", "sample query") // holds the query string
	queryNode.SetAttr("arrival_time", time.Now().Unix()) // holds the arrival time of the query
	queryNode.SetAttr("ttl", 0) // holds the time to live of the query/how long a client can wait for a response
	queryNode.SetAttr("originator", "sample") // holds a peer ID of the peer that received the query from client
	queryNode.SetAttr("sender_address", "") // holds the address of the peer that sent the query. This will be changing as a query is being propagated
	queryNode.SetAttr("state", "new") // holds the state of the query. We have QUEUED, LOCALLY_EXECUTED, SENT_BACK, COMPLETED and FAILED

	// Store the query node
	trans.StoreNode("main", queryNode)

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		logger.Errorf("Error committing transaction: %v", err)
		return
	}

	logger.Info("Committed Query Manager node store transaction")
}
