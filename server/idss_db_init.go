/*
This file contains functions to generate fake data and initialise the graph 
database by loading the generated data into the graph database. It also contains 
a function to initialise the Query Manager graph node.
*/

package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"time"

	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
)

// Function to generate fake data and initialize the graph database by loading the generated data into the graph database
func GenFakeDataAndInit(dataFilePath string, dbPath string, graphDB graphstorage.Storage, graphManager *graph.Manager) error {
	// Execute python script to generate fake data and store it in the peer-specific data file
	err := generateData(dataFilePath)
	if err != nil {
		logger.Fatalf("Failed to generate data: %v", err)
	}

	logger.Infof("Data generation completed, now loading data into the graph database...")

	// Read the generated data from the JSON file specific to this peer
	myData, err := os.ReadFile(dataFilePath)
	if err != nil {
		return err
	}

	return LoadGraphData(string(myData), dbPath, graphDB, graphManager)
}

// Function to generate fake data using a python script
func generateData(dataFilePath string) error {
	cmd := exec.Command("python3", "generate_data.py", dataFilePath)
	err := cmd.Run()
	if err != nil {
		logger.Fatalf("Data generation command execution failed: %v", err)
		return err
	}
	return nil
}


// Function to load graph data into the graph database using a JSON string of nodes and edges and graph transaction
func LoadGraphData(dataa string, dbPath string, graphDB graphstorage.Storage, graphManager *graph.Manager) error {
	trans := graph.NewGraphTrans(graphManager)

	// Unmarshal the JSON data
	var myData map[string]interface{}
	err := json.Unmarshal([]byte(dataa), &myData)
	if err != nil {
		logger.Errorf("Error unmarshalling JSON data: %v", err)
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

// Function to initialise the Query Manager graph node
func QueryManager_init(GRAPH_MANAGER *graph.Manager) {
	logger.Info("Creating the Query Manager node...")

	trans := graph.NewGraphTrans(GRAPH_MANAGER)

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
	queryNode.SetAttr("result", "") // holds the response to the query

	// Store the query node
	trans.StoreNode("main", queryNode)
	if err := trans.Commit(); err != nil {
		logger.Errorf("Error committing transaction: %v", err)
		return
	}
	logger.Info("Committed Query Manager node store transaction")
}
