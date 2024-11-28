/*
HOW TO RUN
1. Run the server(s) using the command: go run idss_main.go -f <filename> (or user script start_peers.sh <number>). <number> represents number of peers you want to simulate
2. Run the client using the command: go run idss_client.go -s <server_multiaddress>
3. Enter your query or 'exit' to quit.
4. The client will send the query to the server and print the response.
5. The client will continue to send queries until the user types 'exit'.

****************************************************************************************
SAMPLE QUERIES get and lookup
TRAVERSAL SYNTAX:
<source role>:<relationship kind>:<destination role>:<destination kind>

SAMPLE QUERIES
>> get Consumption traverse ::: where name = "Alice" (WORKING)
>> lookup Client '3' traverse ::: (WORKING)
>> get Client traverse owner:belongs_to:usage:Consumption (WORKING)


COUNT FUNCTION IN EQL
>> get Client where @count(owner:belongs_to:usage:Consumption) > 2 (WORKING)
>> get Client where @count(owner:belongs_to:usage:Consumption) > 4 (WORKING)
		NOTE: All clients that have more than 4 connections will be returned
				They have more 4 consumption nodes each
>> get Consumption where @count(:::) > 0 (WORKING)
		NOTE: Returns all the nodes in the graph who are connected to each other
		and its count is greater than 0, so any connected node will be returned

>> get Client where Power > 150 and @count(owner:belongs_to:usage:Consumption) > 5
		NOTE: Retrieve Clients with more than 5 Consumption Records with Power Greater than 150

>> get Client traverse owner:belongs_to:usage:Consumption where measurement >= 300 and measurement <= 800
		NOTE: Retrieve Clients with a Specific Consumption Measurement Range

>> get Client where contract_number >= 200 and contract_number <= 500 and @count(owner:belongs_to:usage:Consumption where measurement > 600) >= 3
		NOTE: Retrieve Clients with at Least 3 High-Power Consumptions and Specific Contract Numbers

>> get Client where @count(::belongs_to:usage:Consumption) > 1 and @count(owner:belongs_to::Client) > 1
		NOTE: Clients Who Have at Least 2 Shared Consumption Records with Other Clients

>> get Client where @count(owner:belongs_to:usage:Consumption where measurement > 700) > 0 and @count(owner:belongs_to:usage:Consumption where measurement < 300) > 0
		NOTE: Clients Who Have Mixed High and Low Power Consumption Records

>> get Client traverse owner:belongs_to:usage:Consumption where measurement >= 400 and measurement <= 900 traverse usage:belongs_to:owner:Client
		NOTE:  Find Clients with Specific Consumption Patterns and Shared Edges (This has alot of rows)

>>
****************************************************************************************
*/

package main

import (
	"bufio"
	"context"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"encoding/binary"
	"flag"
	"fmt"
	"idss/graphdb/common"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/ipfs/go-log/v2"
	"github.com/krotik/eliasdb/eql"
	eliasdb "github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"

	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

/*
*********
Constants
**********
*/
const (
	DB_PATH = "../server/idss_graph_db"
)

var (
	activeConnections int64
	logger            = log.Logger("IDSS")
	mu sync.Mutex
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

/*=====================
MAIN FUNCTION
=====================*/
func main() {
	log.SetLogLevel("IDSS", "info")

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
	config, err := ParseFlags(host.ID().String())
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

		Graph database operations using EliasDB. This must be affected in all the joining peers.
		Calling functions from idss_db_init.go to create the database nodes and edges. Note that, this process 
		involves generating random data using generate_data.py. This python script can be used to generate 
		random data for the graph database. The data is stored in a JSON file which is then used to create 
		the nodes and edges in the graph database.

		Before generating the data, the graph database env must be created. This is done by creating a path, 
		storage instance and graph manager.

	************************/

	dbPath := fmt.Sprintf("%s/%s", DB_PATH, host.ID().String()) // Peer specific database path

	graphDB, err := graphstorage.NewDiskGraphStorage(dbPath, false) // Create a new disk graph storage
	if err != nil {
		logger.Fatalf("Error creating graph database: %v", err)
		return
	}
	defer graphDB.Close()

	graphManager := eliasdb.NewGraphManager(graphDB) // Create a new graph manager

	if graphManager == nil { 
		logger.Error("Graph manager not created")
		os.Exit(1)
	}

	// Load the sample data into the graph database
	logger.Info("Loading data from file: ", config.Filename)

	if err := GenFakeDataAndInit(config.Filename, dbPath, graphDB, graphManager); err != nil {
		logger.Fatalf("Error initializing database: %v", err)
	}
	logger.Info("Data loaded into the graph database")

	QueryManager_init(graphManager)

	kadDHT := initialiseDHT(ctx, host, config) // Pass conf for protocol ID

	// A go routine to refresh the DHT and periodically find and connect to peers
	go discoverAndConnectPeers(ctx, host, config, kadDHT)

	// Handle streams
	host.SetStreamHandler(protocol.ID(config.ProtocolID), func(stream network.Stream) { 
		atomic.AddInt64(&activeConnections, 1)
		defer atomic.AddInt64(&activeConnections, -1) // decrement when the handler exits
		go handleRequest(host, stream, stream.Conn().RemotePeer().String(), ctx, config, graphManager, kadDHT)
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

func cleanup(graphDB graphstorage.Storage, host host.Host) {
	if err := graphDB.Close(); err != nil {
		logger.Error("Error closing graph database: ", err)
	}

	peerDir := filepath.Join(DB_PATH, host.ID().String())
	if err := os.RemoveAll(peerDir); err != nil {
		logger.Error("Error deleting database: ", err)
	}
}

// Just remove the peer from the routing table
func handlePeerDisconnection(iD peer.ID, kadDHT *dht.IpfsDHT) {
	kadDHT.RoutingTable().RemovePeer(iD)
}

// Function to discover and connect to peers
func discoverAndConnectPeers(ctx context.Context, host host.Host, config Config, kadDHT *dht.IpfsDHT) {
	startTime := time.Now()
	logger.Info("Starting peer discovery...")

	// Provide a value for the given key. Where a key in this case is the service name (config.IDSSString)
	routingDiscovery := drouting.NewRoutingDiscovery(kadDHT)
	dutil.Advertise(ctx, routingDiscovery, config.IDSSString)
	logger.Debug("Announced the IDSS service")

	// Look for other peers in the network who announced and connect to them
	anyConnected := false
	logger.Infoln("Searching for other peers...")
	fmt.Println("...")

	for !anyConnected {
		peerChan, err := routingDiscovery.FindPeers(ctx, config.IDSSString)
		if err != nil {
			logger.Errorf("Error finding peers: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		for peerInfo := range peerChan {
			if peerInfo.ID == host.ID() {
				continue // Skip self
			}
			if err := host.Connect(ctx, peerInfo); err != nil {
				continue
			} else {
				//logger.Infof("Connected to peer %s", peerInfo.ID)
				anyConnected = true
			}
		}
	}

	logger.Infof("Num of found peers in the Routing Table: %d", len(kadDHT.RoutingTable().GetPeerInfos()))
	logger.Info("Time taken to find peers: ", time.Since(startTime)) //for debugging
	logger.Info("Peer discovery completed") 
}

// Function to initialise the DHT and bootstrap the peer with default bootstrap nodes
func initialiseDHT(ctx context.Context, host host.Host, config Config) *dht.IpfsDHT {
	// Validate and prepare bootstrap peers
	var bootstrapPeers []peer.AddrInfo
	for _, addr := range config.BootstrapPeers {
		peerinfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			logger.Warnf("Invalid bootstrap peer address: %s. Skipping. Error: %v", addr.String(), err)
			continue
		}
		bootstrapPeers = append(bootstrapPeers, *peerinfo)
	}

	if len(config.BootstrapPeers) == 0 {
		logger.Warn("No local bootstrap peers provided. Trying public bootstrap peers.")
		for _, addr := range dht.DefaultBootstrapPeers {
			peerinfo, err := peer.AddrInfoFromP2pAddr(addr)
			if err != nil {
				logger.Warnf("Invalid default bootstrap peer address: %s. Skipping. Error: %v", addr.String(), err)
				continue
			}
			bootstrapPeers = append(bootstrapPeers, *peerinfo)
		}
	}

	kadDHT, err := dht.New(ctx, host, dht.BootstrapPeers(bootstrapPeers...))
	if err != nil {
		logger.Fatal("Error creating DHT", err)
	}

	if err := kadDHT.Bootstrap(ctx); err != nil {
		logger.Fatalf("Error bootstrapping the DHT: %v", err)
		os.Exit(1)
	}

	logger.Info("Bootstrapped the DHT")

	var wg sync.WaitGroup
	for _, peerInfo := range bootstrapPeers {
		//peerinfo, _ := peer.AddrInfoFromP2pAddr(addr)
		wg.Add(1)
		go func(p peer.AddrInfo) {
			defer wg.Done()
			if err := host.Connect(ctx, p); err != nil {
				logger.Warnf("Error connecting to bootstrap peer %s: %v", p.ID, err)
			}else{
				logger.Infof("Connected to bootstrap peer: %s", p.ID)
				return 
			}
			//logger.Info("Connected to bootstrap peer: ", p.ID)
		}(peerInfo)
	}
	wg.Wait() 
	return kadDHT
}

// A function to handle incoming requests from peers.
func handleRequest(host host.Host, conn network.Stream, remotePeerID string, ctx context.Context, config Config, gm *eliasdb.Manager, kadDHT *dht.IpfsDHT) {
	//logger.Infof("Received incoming from %s", remotePeerID)
	defer conn.Close()

	// For any connected peer, add to the routing table
	host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(net network.Network, conn network.Conn) {
			kadDHT.RoutingTable().PeerAdded(conn.RemotePeer()) // Add to routing table
		},
	})


	for {
		msgBytes, err := readDelimitedMessage(conn, ctx)
		if err != nil {
			if err != io.EOF {
				//logger.Errorf("%v", err)
			}
			return
		}

		// Decode the incoming query message
		err = proto.Unmarshal(msgBytes, &msg)
		if err != nil {
			logger.Errorf("Error unmarshalling query message: %v", err)
			return
		}

		if msg.Type == common.MessageType_QUERY{
			handleQuery(conn, &msg, remotePeerID, config, gm, kadDHT)
		}
	}
}

func handleQuery(conn network.Stream, msg *common.QueryMessage, remotePeerID string, config Config, gm *eliasdb.Manager, kadDHT *dht.IpfsDHT) {
	//logger.Infof("Query received:\nOn peer %s \nUQI: %s\nTTL: %f \nFrom: %s", kadDHT.Host().ID(), msg.Uqid, msg.Ttl, remotePeerID)

	duplicateQuery, err := checkDuplicateQuery(msg.Uqid, gm)
	if err != nil {
		logger.Errorf("Error checking duplicate query: %v", err)
		return
	}

	if len(duplicateQuery) > 0 {
		//logger.Warnf("Duplicate query received on Peer: %s %s (UQI: %s)", kadDHT.Host().ID(), msg.Query, msg.Uqid) 
		return
	}

	// Change the state of the query to QUEUED
	msg.State = &common.QueryState{State: common.QueryState_QUEUED}
	storeQueryInfo(msg, gm, remotePeerID)

	logger.Infof("This is a new query on this peer")

	executeAndBroadcastQuery(conn, msg, config, gm, kadDHT)
}

func executeAndBroadcastQuery(conn network.Stream, msg *common.QueryMessage, config Config, gm *eliasdb.Manager, kadDHT *dht.IpfsDHT) {
	//logger.Info("Entering executeAndBroadcastQuery...")
	var wg sync.WaitGroup
	var localResHolder [][]interface{}
	startTime := time.Now()

	// Run local query 
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, header, err := runQuery(msg.Query, kadDHT.Host().ID(), gm)
		if err != nil {
			logger.Errorf("Error executing local query: %v", err)
			return
		}

		// Handle nil header (for debugging or metadata) 
		if header != nil {
			labels := header.Labels()
			logger.Debug("Labels: ", labels)
		} else {
			labels := []string{}
			logger.Debug("Labels: ", labels)
		}

		localResHolder = result
		msg.State = &common.QueryState{State: common.QueryState_LOCALLY_EXECUTED}
		storeQueryInfo(msg, gm, conn.Conn().RemotePeer().String())
		//logger.Info("Local query executed successfully")
	}()

	wg.Wait() // proceed to convert results after local query is done
	localResults := covertResultToProtobufRows(localResHolder, nil)
	msg.Result = localResults

	// Run broacastquery in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Mow update sender address
		msg.Sender = kadDHT.Host().ID().String() // So that the overlay peers identify this as parent peer
		logger.Info("Sender address updated to: ", msg.Sender)
	
		broadcastQuery(msg, conn, config, localResHolder, gm, kadDHT)
	}()
	
	wg.Wait() // Ensure all operations are completed before closing the stream
	logger.Infof("FINISHED! Time taken to execute and broadcast query: %v", time.Since(startTime))
}

// Checks if the query is already in the graph database for the peer
func checkDuplicateQuery(uqi string, gm *eliasdb.Manager) ([][]interface{}, error) {
	statement := fmt.Sprintf("get Query where key = '%s'", uqi)

	checkQuery, err := eql.RunQuery("checkQuery", "main", statement, gm)
	if err != nil {
		return nil, err
	}
	return checkQuery.Rows(), nil
}

// Function to store query information in the graph database using msg contents
func storeQueryInfo(msg *common.QueryMessage, graphManager *eliasdb.Manager, remotePeerID string) {

	// Create a new transaction
	trans := eliasdb.NewGraphTrans(graphManager)

	// Create a new query node
	queryNode := data.NewGraphNode()
	queryNode.SetAttr("key", msg.Uqid)
	queryNode.SetAttr("kind", "Query")
	queryNode.SetAttr("name", "Query")
	queryNode.SetAttr("query_string", msg.Query)
	queryNode.SetAttr("arrival_time", msg.Timestamp)
	queryNode.SetAttr("ttl", msg.Ttl)
	queryNode.SetAttr("originator", msg.Originator)
	queryNode.SetAttr("sender_address", remotePeerID)
	queryNode.SetAttr("state", msg.State.State.String())

	// Store the query node in the graph database
	trans.StoreNode("main", queryNode)

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		logger.Errorf("Error committing transaction: %v", err)
		return
	}
}

// Function to broadcast the query to connected peers. This function also filters out the originating and parent peers because they are already queried
func broadcastQuery(msg *common.QueryMessage, parentStream network.Stream, config Config, localResults [][]interface{}, gm *eliasdb.Manager, kadDHT *dht.IpfsDHT) {
	logger.Infof("Broadcasting query in peer: %v, Parent peer %v ", kadDHT.Host().ID(), parentStream.Conn().RemotePeer())

	//routingDiscovery := drouting.NewRoutingDiscovery(kadDHT)
	var mergedResults [][]interface{} // To hold the merged results from all peers
	processedQueries := make(map[string]bool) // In-memory tracking of processed UQIs
	parentPeerID := parentStream.Conn().RemotePeer() // Parent peer ID

	// Check if this query has already been processed to avoid redundant work
    if processedQueries[msg.Uqid] {
        logger.Infof("Query %s has already been processed on this peer %s. Skipping broadcast.", msg.Uqid, kadDHT.Host().ID())
        return
    }
    processedQueries[msg.Uqid] = true // Mark this query as processed
	connectedPeers := kadDHT.Host().Network().Peers() // Connected peers are the peers in the routing table

	// Filter peers to exclude the originating peer and the parent stream peer
	var eligiblePeers []peer.ID
	for _, peerID := range connectedPeers {
		if peerID != kadDHT.Host().ID() && (parentStream == nil || parentStream.Conn().RemotePeer() != peerID) {
			// Ensure that we don't query self, the parent stream peer or the peer that sent the query
			// Check if there is an active connection to the peer
			eligiblePeers = append(eligiblePeers, peerID)
		}
	}

	if len(eligiblePeers) <= 0 {
        logger.Warn("No eligible peers for broadcast")
        return
    }

	// Decrease the TTL
	newTTL := msg.Ttl * 0.75
	logger.Infof("TTL in %v after decrement: %f", kadDHT.Host().ID(), newTTL)


	if newTTL < 0 { 
		// If the TTL has expired. Means this is the last peer in the overlay network, 
		// no merging needed, just send the local results back to the parent
		logger.Warn("TTL expired, not broadcasting query")
		msg.State = &common.QueryState{State: common.QueryState_SENT_BACK}
		storeQueryInfo(msg, gm, parentStream.Conn().RemotePeer().String())
		// Send just the local results because this is the last peer in the overlay network
		sendMergedResult(parentStream, parentPeerID, localResults, kadDHT) 
		return
	}

	// Else continue broadcasting the query, now with the new TTL
	msg.Ttl = newTTL

	// Prepare for concurrent broadcasting. We have to use concurrency to run the query 
	// on all eligible peers without waiting for one to complete
	var localWg sync.WaitGroup
	remoteResultsChan := make(chan [][]interface{}, len(eligiblePeers)) // enough to accomodate involve peers

	// Broadcast the query, and wait for results from the peers that have received a query from you
	for _, peerID := range eligiblePeers {
		localWg.Add(1)
		go func(p peer.ID){ // Separate goroutine for each peer
			defer localWg.Done()

			duration := time.Duration(newTTL * float32(time.Second)) // Duration follows the TTL
			responseTimeout := time.After(duration)
			streamCtx, streamCancel := context.WithTimeout(context.Background(), duration) // Stream life span is the TTL
			defer streamCancel()

			stream, err := kadDHT.Host().NewStream(streamCtx, p, protocol.ID(config.ProtocolID))
			if err != nil {
				return
			}
			defer stream.Close()

			// Change msg TYPE to QUERY
			msg.Type = common.MessageType_QUERY
			msgBytes, err := proto.Marshal(msg)
			if err != nil {
				logger.Errorf("Error marshalling query message: %v", err)
				return
			}

			// Send the query to the peer
			if err := writeDelimitedMessage(stream, msgBytes); err != nil {
				logger.Errorf("Error writing query to peer %s: %v", p, err)
				return
			}

			//logger.Infof("Query sent to peer %s, awaiting for response", p)

			select{
				case <-responseTimeout: // If the response times out
					//logger.Warnf("Response timeout from peer %s", p)
					return

				case resultsByte := <-func() chan []byte { // Wait for data t be received from the peer
					ch := make(chan []byte, 1)
					go func() { // Separate goroutine to read the response to avoid blocking
						data, err := readDelimitedMessage(stream, streamCtx)
						if err != nil {
							ch <- nil
						} else {
							ch <- data
						}
					}()
					return ch
				}():
					if resultsByte == nil {
						logger.Warnf("Error reading remote results from peer %s", p)
						return
					}

					//logger.Infof("Received %d records from peer %s", len(resultsByte), p)

					var remoteResults common.QueryMessage
					// Unmarshal the results
					err = proto.Unmarshal(resultsByte, &remoteResults) 
					if err != nil {
						logger.Errorf("Error unmarshalling remote results: %v", err)
						return
					}

					// Check if received message is of type RESULT
					if remoteResults.Type == common.MessageType_RESULT {
						remoteResult := convertProtobufRowsToResult(remoteResults.Result)
						remoteResultsChan <- remoteResult
					}
			}
		}(peerID)
	}

	// Wait for all goroutines to complete
	//logger.Infof("Peer %v waiting for overlay peers results...", kadDHT.Host().ID())

	go func(){
		// Waits for all goroutines (all eligible peers) to complete. 
		// To ensure that we do not bock indefinitely, TTL and timeout are used
		localWg.Wait() 
		close(remoteResultsChan)
	}()

	logger.Info("Overlay peers peer results collected, start merging...")

	// Collect al remote results
	for remoteResult := range remoteResultsChan {
		mergedResults = append(mergedResults, remoteResult...) 
	}
	mergedResults = mergeTwoResults(localResults, mergedResults) 
	logger.Infof("DONE merging local and remote results")

	// extract originator peer from the query message
	originatingPeerID, err := peer.Decode(msg.GetOriginator())
	if err != nil {
		logger.Errorf("Error decoding originating peer ID: %v", err)
		return
	}

	if kadDHT.Host().ID() == originatingPeerID { // It has to be an originating peer for state to be completed
		logger.Infof("This is the originating peer %s. Merging results locally", originatingPeerID)
		
		// Update the query state to completed
		msg.State = &common.QueryState{State: common.QueryState_COMPLETED}
		storeQueryInfo(msg, gm, originatingPeerID.String())

		// Send the results to the client
		clientPeerID, err := peer.Decode(msg.Sender)
		if err != nil {
			logger.Errorf("Error decoding client peer ID: %v", err)
			return
		}

		sendMergedResult(parentStream, clientPeerID, mergedResults, kadDHT)
	}else{

		parentPeerID := parentStream.Conn().RemotePeer() // Parent peer ID

		// Update the query state to sent back
		msg.State = &common.QueryState{State: common.QueryState_SENT_BACK}
		storeQueryInfo(msg, gm, parentPeerID.String())

		logger.Infof("This is an intermediate peer %s. Merging results to send to parent peer %s", kadDHT.Host().ID(), parentPeerID)
		// If time has elapsed and remote peer is the client then send the result to the client
		sendMergedResult(parentStream, parentPeerID, mergedResults, kadDHT) 
	}
}

// Function to send the merged result to the client
func sendMergedResult(parentStream network.Stream, parentPeerD peer.ID,   result [][]interface{}, kadDHT *dht.IpfsDHT) {
	var wg sync.WaitGroup
	defer parentStream.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Infof("Sending merged result to peer %s", parentStream.Conn().RemotePeer()) 
		recordCount := len(result)
		logger.Info("Record count: ", recordCount)
		resultRows := covertResultToProtobufRows(result, nil)

		resultMsg := &common.QueryMessage{
			Type:       common.MessageType_RESULT,
			Result:     resultRows,
			RecordCount: int32(recordCount),
		}

		// Marshal the QueryResult message to bytes using Protobuf
		resultBytes, err := proto.Marshal(resultMsg)
		if err != nil {
			logger.Errorf("Error marshalling result for peer %s: %v", kadDHT.Host().ID(), err)
			return
		}

		if err := writeDelimitedMessage(parentStream, resultBytes); err != nil {
			logger.Errorf("Error writing merged result to peer %s: %v", parentPeerD, err)
		} else {
			logger.Infof("Successfully sent merged result to peer %s", parentStream.Conn().RemotePeer()) 
		}
	}()

	// Ensure all operations are completed before closing the stream
	wg.Wait()
	if err := parentStream.Close(); err != nil {
		logger.Errorf("Error closing stream to peer %s: %v", parentPeerD, err)
	}
}

func mergeTwoResults(result1, result2 [][]interface{}) [][]interface{} {
	// Use a map to track unique entries
	estimatedSize := len(result1) + len(result2)
	uniqueRecords := make(map[string]bool, estimatedSize)
	mergedResults := make([][]interface{}, 0, estimatedSize)

	// Helper function to convert a record to a string key for uniqueness checking
    toKey := func(record []interface{}) string {
		var builder strings.Builder
        for _, value := range record {
			mu.Lock()
			builder.WriteString(fmt.Sprintf("%v", value)) // can be optimised further if values are strings or simpler types
			mu.Unlock()
		}
		return builder.String()
    }

	// Process result1
    for _, record := range result1 {
        key := toKey(record)
        if !uniqueRecords[key] {
            mergedResults = append(mergedResults, record)
            uniqueRecords[key] = true
        }
    }

	// Process result2
    for _, record := range result2 {
        key := toKey(record)
        if !uniqueRecords[key] {
            mergedResults = append(mergedResults, record)
            uniqueRecords[key] = true
        }
    }

	return mergedResults
}

func convertProtobufRowsToResult(rows []*common.Row) [][]interface{} {
	var result [][]interface{}
	for _, row := range rows {
		convertedData := make([]interface{}, len(row.Data))
		for i, value := range row.Data {
			convertedData[i] = value
		}
		result = append(result, convertedData)
	}
	return result
}

// Function to convert EliasDB query results to Protobuf rows
func covertResultToProtobufRows(result [][]interface{}, header []string) []*common.Row {
	var rows []*common.Row
	// Send headers first as a special row, if available
    if len(header) > 0 {
        rows = append(rows, &common.Row{Data: header})
    }


	for _, row := range result {
		var values []string
		for _, value := range row {
			values = append(values, fmt.Sprintf("%v", value))
		}
		rows = append(rows, &common.Row{Data: values})
	}
	return rows
}

// Function to read a delimited message from the stream
func readDelimitedMessage(r io.Reader, ctx context.Context) ([]byte, error) {
	bufReader := bufio.NewReader(r)

	// Set read timeout to avoing hanging over the stream
	deadlineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sizeChan := make(chan uint64, 1)
	errChan := make(chan error, 1)

	// Read the message size (varint encoded)
	go func(){
		size, err := binary.ReadUvarint(bufReader)
		if err != nil {
			errChan <- err
		}else{
			sizeChan <- size
		}
	}()

	select {
		case <-deadlineCtx.Done():
			return nil, fmt.Errorf("timeout reading message size")
		case err := <-errChan:
			return nil, err
		case size := <-sizeChan:
			// Read the message data
			buf := make([]byte, size)
			_, err := io.ReadFull(bufReader, buf)
			if err != nil {
				return nil, err
			}
			return buf, nil
	}
}

func writeDelimitedMessage(w io.Writer, data []byte) error {
	sizeBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(sizeBuf, uint64(len(data)))

	_, err := w.Write(sizeBuf[:n])
	if err != nil {
		return err
	}

	// Write the message data
	_, err = w.Write(data)
	return err
}

// IDSS Function to execute local query
func runQuery(command string, peer peer.ID, gm *eliasdb.Manager) ([][]interface{}, eql.SearchResultHeader, error) {
	logger.Infof("Executing %s locally in %s", command, peer)

	result, err := eql.RunQuery("myQuery", "main", command, gm)
	if err != nil {
		logger.Error("Error querying data: ", err)
		return nil, nil, err
	}
	return result.Rows(), result.Header(), err
}

// Function to check for errors
func CheckError(err error, message ...string) {
	if err != nil {
		logger.Fatalf("Error during operation '%s': %v", message, err)
	}
}