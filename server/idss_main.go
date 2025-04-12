/*
 * ! \file idss_main.go
 * main server
 *
 *
 * IDSS (InnoCyPES Data Storage Service) is distributed data storage and query execution service
 * that allows clients to submit queries to an overlay network of peers. The queries are executed
 * in a decentralized manner, with results merged and returned to the client. The system uses a
 * best-effort approach with a time-to-live (TTL) mechanism to ensure efficient query execution.
 *
 * The server is a peer in the overlay network that listens for incoming connections and processes
 * queries from clients and other peers. The server maintains a graph database to store data and
 * query information, and uses a distributed hash table (DHT) to discover and connect to other peers
 * in the network. The DHT is also used to support running of distributed db queries across the network.
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"regexp"
	"strings"

	"flag"
	"fmt"
	"idss/graphdb/broadcast"
	"idss/graphdb/common"
	"idss/graphdb/flags"
	"idss/graphdb/helpers"
	"idss/graphdb/kaddht"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/p2p/protocol/ping"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/ipfs/go-log/v2"

	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	"github.com/krotik/eliasdb/eql"
	

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/security/noise"

	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

// OverlayMetrics contains latency and reliability stats for a peer.
type OverlayMetrics struct {
	PeerID      string        `json:"peer_id"`
	AvgLatency  time.Duration `json:"avg_latency"`
	SuccessRate float64       `json:"success_rate"`
	TotalPings  int           `json:"total_pings"`
	Successes   int           `json:"successes"`
}

var (
	activeConnections int64
	logger            = common.Logger
	msg common.QueryMessage 
)

// Pprof for profiling and resource monitoring
func init() {
    go func() {
        logger.Info("Starting pprof server on :6060") 
        logger.Info(http.ListenAndServe("localhost:6060", nil))
		//Visit http://localhost:6060/debug/pprof/ to view the pprof server
    }()
}

// GatherOverlayMetrics pings every peer in the DHT routing table 'pingCount' times
// and returns a slice with the metrics.
func GatherOverlayMetrics(h host.Host, kadDHT *dht.IpfsDHT, pingCount int) []OverlayMetrics {
	var metrics []OverlayMetrics

	peers := kadDHT.RoutingTable().ListPeers()
	for _, p := range peers {
		var successes int
		var total int
		var sumRTT time.Duration

		// Use a timeout context for the pings.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resultsCh := ping.NewPingService(h).Ping(ctx, p)

		// Attempt pingCount pings
		for i := 0; i < pingCount; i++ {
			res, ok := <-resultsCh
			if !ok {
				break
			}
			total++
			if res.Error == nil {
				successes++
				sumRTT += res.RTT
			}
		}

		var avgLatency time.Duration
		if successes > 0 {
			avgLatency = sumRTT / time.Duration(successes)
		}
		var successRate float64
		if total > 0 {
			successRate = float64(successes) / float64(total)
		}
		metrics = append(metrics, OverlayMetrics{
			PeerID:      p.String(),
			AvgLatency:  avgLatency,
			SuccessRate: successRate,
			TotalPings:  total,
			Successes:   successes,
		})
	}
	return metrics
}

/*=====================
MAIN FUNCTION
=====================*/
func main() {
	log.SetLogLevel("IDSS", "info") // Change to debug for more verbose logging

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Generate key pair for the server/host
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		logger.Fatal("Error generating RSA key pair:", err)
	}

	// Dynamic port allocation
	listenAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		logger.Fatal("Error creating listening address:", err)
	}

	// Create libp2p host
	host, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrs(listenAddr),
		libp2p.Security(noise.ID, noise.New),// Ensure secure communication
	)
	if err != nil {
		logger.Fatalf("Error creating libp2p host: %v", err)
		os.Exit(1)
	}

	// Parse flags
	help := flag.Bool("h", false, "Display help")
	config, err := flags.ParseFlags(host.ID().String())
	if err != nil {
		logger.Fatalf("Error parsing flags: %v", err)
		os.Exit(1)
	}

	if *help {
		fmt.Println("Usage: go run idss_main.go [options]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	// Print a complete peer listening address for client to connect
	for _, addr := range host.Addrs() {
		completePeerAddr := addr.Encapsulate(multiaddr.StringCast("/p2p/" + host.ID().String()))
		logger.Info("Listening on peer Address: ", completePeerAddr, "\n")
	}

	/**********************

		Graph database operations using EliasDB. This must be affected in all the joining peers for efficient querying.
		Calling functions from idss_db_init.go to create the database nodes and edges. Note that, this process 
		involves generating random data using generate_data.py. This python script is used to generate 
		random data for the graph database. The data is stored in a JSON file which is then used to create 
		the nodes and edges in the graph database.

		Before generating the data, the graph database env must be created. This is done by creating a path, 
		storage instance and graph manager. This code already handles the process.

	************************/

	dbPath := fmt.Sprintf("%s/%s", common.DB_PATH, host.ID().String()) // Peer specific database path
	logger.Info("Database path: ", dbPath)

	graphDB, err := graphstorage.NewDiskGraphStorage(dbPath, false) // Create a new disk graph storage
	if err != nil {
		logger.Fatalf("Error creating graph database: %v", err)
		return
	}
	defer graphDB.Close()

	graphManager := graph.NewGraphManager(graphDB) // Create a new graph manager

	if graphManager == nil { // Check if the graph manager was created
		logger.Error("Graph manager not created")
		os.Exit(1)
	}

	// Load the sample data into the graph database
	logger.Info("Loading data from file: ", config.Filename)

	if err := broadcast.GenFakeDataAndInit(config.Filename, dbPath, graphDB, graphManager); err != nil {
		logger.Fatalf("Error initializing database: %v", err)
	}
	logger.Info("Data loaded into the graph database")

	// Initialise the query manager
	broadcast.QueryManager_init(graphManager)

	// Initialise the DHT and bootstrap the peer
	kadDHT := kaddht.InitialiseDHT(ctx, host, config) // Pass conf for protocol ID

	// A go routine to refresh the DHT and periodically find and connect to peers
	go kaddht.DiscoverAndConnectPeers(ctx, host, config, kadDHT)

	// Overlay Information
	/* overlayInfo := helpers.GetOverlayInfo(kadDHT)
	logger.Infof("Overlay Information: %s", overlayInfo) */

	// Gather overlay metrics
	/* overlayMetrics := GatherOverlayMetrics(host, kadDHT, 5) // Ping each peer 5 times
	logger.Infof("Overlay Metrics: %v", overlayMetrics) // TODO: It returns an empty slice. Investigate why */

	// Handle streams
	host.SetStreamHandler(protocol.ID(config.ProtocolID), func(stream network.Stream) { 
		atomic.AddInt64(&activeConnections, 1)
		defer atomic.AddInt64(&activeConnections, -1) // decrement when the handler exits
		go handleRequest(stream, stream.Conn().RemotePeer().String(), ctx, config, graphManager, kadDHT, host)
	})

	// Handle SIGTERM and interrupt signals
	go func() { handleInterrupts(host, graphDB, kadDHT) }()	

	// Keep the server running
	select {}
}

// A function to handle the interrupt signals and gracefully shut down the peer
func handleInterrupts(host host.Host, graphDB graphstorage.Storage, kadDHT *dht.IpfsDHT) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Monitor connection state
	host.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(net network.Network, conn network.Conn) {
			handlePeerDisconnection(conn.RemotePeer(), kadDHT)
		},
	})

	go func() {
		defer func ()  {
			cleanup(graphDB, host)
			// Clean up
			if err := host.Close(); err != nil {
				logger.Error("Error shutting down IDSS: ", err)
				os.Exit(1) // Different termination codes can be used
			}
			os.Exit(0) // normal termination
		}()


		sig := <-c
		logger.Infof("Received signal: %s. Peer exiting the network: %s", sig, host.ID().String())

		for atomic.LoadInt64(&activeConnections) > 0 {
			time.Sleep(100 * time.Millisecond) // Poll until all connections are closed
		}
	}()
}

// Function to clean up the graph database and the peer directory.
// This must be handled differently in production settings
func cleanup(graphDB graphstorage.Storage, host host.Host) {
	if err := graphDB.Close(); err != nil {
		logger.Error("Error closing graph database: ", err)
	}

	peerDir := filepath.Join(common.DB_PATH, host.ID().String())
	if err := os.RemoveAll(peerDir); err != nil { 
		logger.Error("Error deleting database: ", err)
	}
}

// Just remove the peer from the routing table
func handlePeerDisconnection(iD peer.ID, kadDHT *dht.IpfsDHT) {
	kadDHT.RoutingTable().RemovePeer(iD)
}

// A function to handle incoming requests from peers.
func handleRequest(conn network.Stream, remotePeerID string, ctx context.Context, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, host host.Host) {
	logger.Debug("Received incoming from %s", remotePeerID)
	defer conn.Close()

	// Read the incoming message
	for {
		msgBytes, err := helpers.ReadDelimitedMessage(conn, ctx)
		if err != nil {
			if err != io.EOF {
				logger.Debug("%v", err)
			}
			return
		}

		// Decode the incoming query message
		err = proto.Unmarshal(msgBytes, &msg)
		if err != nil {
			logger.Errorf("Error unmarshalling query message: %v", err)
			return
		}

		// This aims to handle only query messages. Other message types can be added and handled accordingly
		// The client will send a query message to the server
		if msg.Type == common.MessageType_QUERY{
			handleQuery(conn, &msg, remotePeerID, config, gm, kadDHT, host)
		}
	}
}

// Function to process the query message received from the client or intermediate peers
func handleQuery(conn network.Stream, msg *common.QueryMessage, remotePeerID string, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, host host.Host) {
	
	duplicateQuery, err := broadcast.CheckDuplicateQuery(msg.Uqid, gm) // with gm, we check queries in the graph database for this peer
	if err != nil {
		logger.Errorf("Error checking duplicate query: %v", err)
		return
	}

	logger.Debug("Query received:\nOn peer %s \nUQI: %s\nTTL: %f \nFrom: %s", kadDHT.Host().ID(), msg.Uqid, msg.Ttl, remotePeerID) // for debugging
	if len(duplicateQuery) > 0 {
		logger.Debug("Query IGNORED")
		return
	}
	logger.Infof("This is a new query on this peer")
	logger.Infof("\nReceiver %s \nUQI: %s\nTTL: %f \nFrom: %s", kadDHT.Host().ID(), msg.Uqid, msg.Ttl, remotePeerID) // for debugging
	msg.State = &common.QueryState{State: common.QueryState_QUEUED} // Set the query state to QUEUED
	broadcast.StoreQueryInfo(msg, gm, remotePeerID) // Store the query info in the graph database

	// Normalize the query for parsing
    queryLower := strings.ToLower(msg.Query)
    queryTrimmed := strings.TrimSpace(msg.Query)

	// Check if query is meant to be run locally or distributed
	if strings.Contains(queryLower, "-l") ||
		strings.Contains(queryLower, "-local") ||
		strings.Contains(queryLower, "add") ||
		strings.Contains(queryLower, "update") ||
		strings.Contains(queryLower, "delete"){
			logger.Info("Handling a local query")
			if (strings.HasPrefix(strings.ToLower(msg.Query), "add")){
				key, err := handleAddQuery(queryTrimmed, gm)
				if err != nil {
					logger.Errorf("Error handling add query: %v", err)
					helpers.SendErrorMessage(conn, peer.ID(remotePeerID), err.Error())
					return
				}
				successMsg := fmt.Sprintf("Node %s added successfully", key)
				logger.Infof(successMsg)
				sendSuccessMessage(conn, remotePeerID, successMsg, kadDHT)
				return // Do not broadcast
			}else if(strings.HasPrefix(queryTrimmed, "delete")){
				logger.Info("Handling a delete query")
				key, err := handleDeleteQuery(queryTrimmed, gm)
				if err != nil {
					logger.Errorf("Error handling delete query: %v", err)
					helpers.SendErrorMessage(conn, peer.ID(remotePeerID), err.Error())
					return
				}
				successMsg := fmt.Sprintf("Node %s deleted successfully", key)
				logger.Infof(successMsg)
				sendSuccessMessage(conn, remotePeerID, successMsg, kadDHT)
				return // Do not broadcast
			}else if(strings.HasPrefix(queryTrimmed, "update")){
				logger.Info("Handling an update query")

				key, err := handleUpdateQuery(queryTrimmed, gm)
				if err != nil {
					logger.Errorf("Error handling update query: %v", err)
					helpers.SendErrorMessage(conn, peer.ID(remotePeerID), err.Error())
					return
				}
				successMsg := fmt.Sprintf("Node %s updated successfully", key)
				logger.Infof(successMsg)
				sendSuccessMessage(conn, remotePeerID, successMsg, kadDHT)
				return // Do not broadcast
			}else{

				// Remove the -l or -local flag from the query
				msg.Query = strings.Replace(msg.Query, "-local", "", -1)
				msg.Query = strings.Replace(msg.Query, "-l", "", -1)
				msg.Query = strings.TrimSpace(msg.Query)
				
				
				// Execute the query locally
				results, header, err := broadcast.RunIDSSQuery(msg.Query, host.ID(), gm)
				if err != nil {
					logger.Errorf("Error executing local query: %v", err)
					// Send an error message back to the client
					helpers.SendErrorMessage(conn, peer.ID(remotePeerID), err.Error())
					return
				}

				// Send the results back to the client
				logger.Infof("Local query results - Header: %v, Rows: %d", header, len(results))
				
				helpers.SendMergedResult(conn, peer.ID(remotePeerID), results, header, kadDHT)
				logger.Infof("Local query executed successfully")
				return
			}
	}

	if (
		strings.Contains(msg.Query, "@avg(") ||
		strings.Contains(msg.Query, "@min(") ||
		strings.Contains(msg.Query, "@max(") ||
		strings.Contains(msg.Query, "@sum(")) &&
		!strings.Contains(msg.Query, "@count("){
		broadcast.HandleAggregateQuery(conn, msg, config, gm, kadDHT, helpers.ParseAggregates(msg.Query)[0])
		return
	}
	broadcast.ExecuteAndBroadcastQuery(conn, msg, config, gm, kadDHT)
}

// Function to send a success message back to the client
func sendSuccessMessage(conn network.Stream, remotePeerID string, message string, kadDHT *dht.IpfsDHT) {
    // Create a simple result with the success message
    results := [][]interface{}{
        {"Status"},
        {message},
    }
    header := []string{"Status"}
    helpers.SendMergedResult(conn, peer.ID(remotePeerID), results, header, kadDHT)
}

// Function to handle update nodes in the graph database
func handleUpdateQuery(query string, gm *graph.Manager) (string, error) {
    // Example query: "update Client 15 name='John Doe' power=300"
    query = strings.TrimSpace(strings.TrimPrefix(query, "update"))
    parts := strings.Fields(query)
    if len(parts) < 2 {
        return "", fmt.Errorf("invalid update query format: expected 'update <kind> <key> [attributes]'")
    }

    kind := parts[0]
    key := parts[1]
	attrStart := strings.Index(query, key) + len(key)
    attrString := strings.TrimSpace(query[attrStart:])
    attributes := extractAttributes(attrString)

	logger.Infof("Kind: %s, Key: %s, Attributes: %v", kind, key, attributes)

    graphNode := data.NewGraphNode()
    graphNode.SetAttr("key", key)
    graphNode.SetAttr("kind", kind)

    for k, v := range attributes {
        graphNode.SetAttr(k, v)
    }

	if err := gm.UpdateNode("main", graphNode); err != nil {
        return "", fmt.Errorf("failed to update node: %v", err)
    }

    return key, nil
}

// Function to handle deletion of nodes from the graph database
func handleDeleteQuery(query string, gm *graph.Manager) (string, error) {
    // Example query: "delete Client 15"
    query = strings.TrimSpace(strings.TrimPrefix(query, "delete"))
    parts := strings.Fields(query)
    if len(parts) < 2 {
        return "", fmt.Errorf("invalid delete query format: expected 'delete <kind> <key>'")
    }

    kind := parts[0]
    key := parts[1]
	logger.Infof("Attempting to delete node - Kind: %s, Key: %s", kind, key)

	// Verify node exists before deletion
    checkQuery := fmt.Sprintf("get %s where key = '%s'", kind, key)
    result, err := eql.RunQuery("checkNode", "main", checkQuery, gm)
    if err != nil || len(result.Rows()) == 0 {
        logger.Warnf("Node with kind %s and key %s not found", kind, key)
        return "", fmt.Errorf("node with key %s not found", key)
    }
	logger.Infof("Node found - Rows: %d", len(result.Rows()))

    trans := graph.NewGraphTrans(gm)
	if err := trans.RemoveNode("main", key, kind); err != nil {
		logger.Errorf("Error deleting node: %v", err)
        return "", fmt.Errorf("failed to delete node: %v", err)
    }

	if err := trans.Commit(); err != nil {
        logger.Errorf("Failed to commit deletion of key %s: %v", key, err)
        return "", fmt.Errorf("failed to commit deletion: %v", err)
    }

	logger.Infof("Node %s deleted successfully", key)
    return key, nil
}

// Function to handle addition of nodes to the graph database
func handleAddQuery(query string, gm *graph.Manager) (string, error) {
	// Example query: "add Client 15 name='John Mandili' contract_number=35435 power=255"
    query = strings.TrimSpace(strings.TrimPrefix(query, "add"))
    parts := strings.Fields(query)
    if len(parts) < 2 {
        return "", fmt.Errorf("invalid add query format: expected 'add <kind> <key> [attributes]'") 
    }

    kind := parts[0]
    key := parts[1]
	attrStart := strings.Index(query, key) + len(key)
    attrString := strings.TrimSpace(query[attrStart:])
    attributes := extractAttributes(attrString)

	logger.Infof("Kind: %s, Key: %s, Attributes: %v", kind, key, attributes)

    //trans := graph.NewGraphTrans(gm)
    graphNode := data.NewGraphNode()
    graphNode.SetAttr("key", key)
    graphNode.SetAttr("kind", kind)

    for k, v := range attributes {
        graphNode.SetAttr(k, v)
    }

	if err := gm.StoreNode("main", graphNode); err != nil {
        return "", fmt.Errorf("failed to store node: %v", err)
    }
    return key, nil
}

// Helper function to extract attributes from a query string
func extractAttributes(query string) map[string]string {
    attrRegex := regexp.MustCompile(`(\w[\w\s]*)\s*=\s*("[^"]*"|\S+)`)
    attrMatches := attrRegex.FindAllStringSubmatch(query, -1)
    attributes := make(map[string]string)

    for _, match := range attrMatches {
        attrKey := strings.TrimSpace(match[1])
        attrValue := strings.Trim(match[2], "\"")
        attributes[attrKey] = attrValue
    }
    return attributes
}

// Function to check for errors
func CheckError(err error, message ...string) {
	if err != nil {
		logger.Fatalf("Error during operation '%s': %v", message, err)
	}
}