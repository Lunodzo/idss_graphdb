package main

import (
	"testing"
	//"time"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	//"google.golang.org/protobuf/proto"
	//"google.golang.org/protobuf/types/known/timestamppb"
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
	/* query_mng := &QueryMessage{
		Uqi:            "123", // Generate unique ID based on the client port number and the current time
		Query:           "get client where name = 'Client1'",
		Ttl:             100,
	}
	query_data, err := proto.Marshal(query_mng)
	if err != nil {
		t.Errorf("Error marshalling data: %v", err)
	}
	// Print the marshalled data
	t.Log("Marshalled query manager data: ", query_data) */

	

	node2 := data.NewGraphNode()
	node2.SetAttr("name", "QueryManager1")
	//node2.SetAttr("data", query_data)
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


	// Get the nodes and edges from the graph
	nodes, _ := GRAPH_MANAGER.FetchNode("main", "kind", "client")
	edges, _ := GRAPH_MANAGER.FetchEdge("main", "kind", "query")

	// Print the nodes and edges
	t.Log("Nodes: ", nodes)
	t.Log("Edges: ", edges)
}