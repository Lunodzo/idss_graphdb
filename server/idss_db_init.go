package main

import (
	"log"
	"time"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Database_init() {
	log.Println("Entering the database initialisation from...")

	// Create an instance of graph storage in the disk 
	// (false parameter means that the database is not read-only)
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	CheckError(err)
	defer GRAPH_DB.Close()
	log.Println("Database storage initialised successfully")

	// Create graph manager
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)

	// Create instances from the protofile and marshal the data
	log.Println("Creating instances from the protofile if it does not exist...")
	
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
}