package main

import (
	"testing"
	"time"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Test_Database_init(t *testing.T) {
	// Create an instance of graph storage in the disk 
	// (false parameter means that the database is not read-only)
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	if err != nil {
		t.Errorf("Error creating graph storage: %v", err)
	}
	defer GRAPH_DB.Close()
	t.Log("Database storage initialised successfully")

	// Create graph manager
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)

	// Create instances from the protofile and marshal the data
	t.Log("Creating instances from the protofile...")

	// Query manager instance creation with empty values for the first time to create the instance
	query_mng := &QueryManager{
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
		t.Errorf("Error marshalling data: %v", err)
	}
	// Print the marshalled data
	t.Log("Marshalled query manager data: ", query_data)

	client1 := &Client{
		ClientId:    	123,
		ClientName: 	"Lunodzo",
		ContractNumber: 244,
		Power: 			343,
	}

	data1, err := proto.Marshal(client1)
	if err != nil {
		t.Errorf("Error marshalling data: %v", err)
	}
	// Print the marshalled data
	t.Log("Marshalled client data: ", data1)

	// Create nodes
	node1 := data.NewGraphNode()
	node1.SetAttr("name", "Client1")
	node1.SetAttr("data", data1)
	node1.SetAttr("kind", "client")

	node2 := data.NewGraphNode()
	node2.SetAttr("name", "QueryManager1")
	node2.SetAttr("data", query_data)
	node2.SetAttr("kind", "query_manager")
	
	// Create edges
	edge1 := data.NewGraphEdge()
	edge1.SetAttr(data.NodeKey, "Client1")
	edge1.SetAttr(data.NodeKey, "QueryManager1")
	edge1.SetAttr("kind", "query")

	edge2 := data.NewGraphEdge()
	edge2.SetAttr(data.NodeKey, "QueryManager1")
	edge2.SetAttr(data.NodeKey, "Client1")
	edge2.SetAttr("kind", "response")

	// Add nodes and edges to the graph
	GRAPH_MANAGER.StoreNode("main", node1)
	GRAPH_MANAGER.StoreNode("main", node2)
	GRAPH_MANAGER.StoreEdge("main", edge1)
	GRAPH_MANAGER.StoreEdge("main", edge2)

	// Get the nodes and edges from the graph
	nodes, _ := GRAPH_MANAGER.FetchNode("main", "kind", "client")
	edges, _ := GRAPH_MANAGER.FetchEdge("main", "kind", "query")

	// Print the nodes and edges
	t.Log("Nodes: ", nodes)
	t.Log("Edges: ", edges)
}