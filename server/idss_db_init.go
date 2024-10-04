package main

import (
	"encoding/json"
	"os"
	"time"

	eliasdb "github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
)

// Function to initialise the database
func Database_init(filename string, DB_PATH string, GRAPH_DB graphstorage.Storage, GRAPH_MANAGER *eliasdb.Manager) {
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
}

// Function to initialise the Query Manager node
func QueryManager_init(GRAPH_MANAGER *eliasdb.Manager) {
	logger.Info("Creating the Query Manager node...")

	// Create a new transaction
	trans := eliasdb.NewGraphTrans(GRAPH_MANAGER)

	// Create the query node
	queryNode := data.NewGraphNode()
	queryNode.SetAttr("key", "sample")
	queryNode.SetAttr("kind", "Query")
	queryNode.SetAttr("name", "Query")
	queryNode.SetAttr("query_string", "sample query")
	queryNode.SetAttr("arrival_time", time.Now().Unix())
	queryNode.SetAttr("ttl", 0)
	queryNode.SetAttr("sender_address", "")
	queryNode.SetAttr("state", "new")

	// Store the query node
	trans.StoreNode("main", queryNode)

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		logger.Errorf("Error committing transaction: %v", err)
		return
	}

	logger.Info("Committed Query Manager node store transaction")
}
