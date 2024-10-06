/*
HOW TO RUN
1. Run the server using the command: go run idss_main.go -f <filename> (go run . -f data1.json)
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
>> lookup client '3' traverse ::: (WORKING)
>> get Client traverse owner:belongs_to:usage:Consumption (WORKING)


COUNT FUNCTION IN EQL
>> get Client where @count(owner:belongs_to:usage:Consumption) > 2 (WORKING)
>> get Client where @count(owner:belongs_to:usage:Consumption) > 4 (WORKING)
		NOTE: Only Alice and Bob will be returned as they have more than 4 connections
				They have more 4 consumption nodes each
>> get Consumption where @count(:::) > 0 (WORKING)
		NOTE: Returns all the nodes in the graph who are connected to each other
		and its count is greater than 0, so any connected node will be returned
****************************************************************************************
*/

package main

import (
	"bufio"
	"context"
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
	PORT    = "8080" //optional
)

var (
	KADDHT *dht.IpfsDHT
	activeConnections int64
	logger            = log.Logger("IDSS")
)

/*=====================
MAIN FUNCTION
=====================*/

// Main function to start the server operations
func main() {
	// Set up logging
	//log.SetAllLoggers(log.LevelWarn) // Or log.LevelInfo, log.LevelDebug, etc.
	log.SetLogLevel("IDSS", "info")

	// Parse flags
	help := flag.Bool("h", false, "Display help")
	config, err := ParseFlags()
	if err != nil {
		logger.Fatalf("Error parsing flags: %v", err)
		os.Exit(1)
	}

	filename := config.Filename

	if *help {
		fmt.Println("Usage: go run idss_main.go [options]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	// Increase the buffer size
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{
		Cur: 4096,
		Max: 8192,
	})

	if err != nil {
		logger.Fatalf("Error setting file limit: %v", err)
		os.Exit(1)
	}

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
	)
	if err != nil {
		logger.Fatalf("Error creating libp2p host: %v", err)
		os.Exit(1)
	}

	// Print a complete peer listening address
	for _, addr := range host.Addrs() {
		completePeerAddr := addr.Encapsulate(multiaddr.StringCast("/p2p/" + host.ID().String()))
		logger.Info("Listening on peer Address:\n\n", completePeerAddr, "\n")
	}

	/**********************

		Graph database operations using EliasDB. This must be affected in all the connected peers.
		Calling functions from idss_db_init.go to create the database nodes and edges.

		When a node do not have any file, it will skip the database creation and will not create any
		nodes and edges in the graph database. In the event of data fetching it will fetch from
		other peers.

	************************/

	// Create a directory for the database specific to the peer
	dbPath := fmt.Sprintf("%s/%s", DB_PATH, host.ID().String())
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		os.MkdirAll(dbPath, 0755)
	}

	// Create the graph database storage
	graphDB, err := graphstorage.NewDiskGraphStorage(dbPath, false)
	if err != nil {
		logger.Fatalf("Error creating graph database: %v", err)
		return
	}
	defer graphDB.Close()

	// Create the graph manager
	graphManager := eliasdb.NewGraphManager(graphDB)

	// Check if the graph manager is created
	if graphManager == nil {
		logger.Error("Graph manager not created")
		os.Exit(1)
	}

	// Load the sample data into the graph database
	if filename != "" {
		logger.Info("Data sample provided...")
		Database_init(filename, dbPath, graphDB, graphManager)
	} else {
		Database_init("", dbPath, graphDB, graphManager)
	}

	// Initialise the Query Node
	QueryManager_init(graphManager)

	// Initialise the DHT
	KADDHT = initialiseDHT(ctx, host, config)

	// A go routine to refresh the routing table and periodically find and connect to peers
	go discoverAndConnectPeers(ctx, host, config)

	// Handle streams
	host.SetStreamHandler(protocol.ID(config.ProtocolID), func(stream network.Stream) {
		atomic.AddInt64(&activeConnections, 1)
		defer atomic.AddInt64(&activeConnections, -1) // decrement when the handler exits

		go handleRequest(stream, stream.Conn().RemotePeer().String(), ctx, config, graphManager)
	})

	// Handle SIGTERM and interrupt signals
	go func() { handleInterrupts(host, graphDB) }()	

	// Keep the server running
	select {}
}

// A function to handle the interrupt signals and gracefully shut down the peer
func handleInterrupts(host host.Host, graphDB graphstorage.Storage) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Monitor connection state
	host.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(net network.Network, conn network.Conn) {
			//logger.Infof("Disconnected from peer: %s", conn.RemotePeer())
			handlePeerDisconnection(conn.RemotePeer())
		},
	})

	go func() {
		sig := <-c
		logger.Infof("Received signal: %s. Peer exiting the network: %s", sig, host.ID().String())

		for atomic.LoadInt64(&activeConnections) > 0 {
			time.Sleep(100 * time.Millisecond) // Poll until all connections are closed
		}

		cleanup(graphDB, host)

		// Clean up
		if err := host.Close(); err != nil {
			logger.Error("Error shutting down IDSS: ", err)
			os.Exit(1) // Different termination codes can be used
		}

		os.Exit(0) // normal termination
	}()
}

func cleanup(graphDB graphstorage.Storage, host host.Host) {
	// Close the graph database
	if err := graphDB.Close(); err != nil {
		logger.Error("Error closing graph database: ", err)
	}

	// Delete the database
	peerDir := filepath.Join(DB_PATH, host.ID().String())
	if err := os.RemoveAll(peerDir); err != nil {
		logger.Error("Error deleting database: ", err)
	}
}

// Function to handle peer disconnection
func handlePeerDisconnection(iD peer.ID) {
	//logger.Infof("Preserving data for peer %s", iD, graphDB)
	// Delete the peer from the peerConnections map
	//peerConnections.Delete(iD)
}

// Function to discover and connect to peers
func discoverAndConnectPeers(ctx context.Context, host host.Host, config Config) {
	logger.Info("Starting peer discovery...")
	startTime := time.Now()

	logger.Info("Announcing the IDSS service...")
	routingDiscovery := drouting.NewRoutingDiscovery(KADDHT)
	dutil.Advertise(ctx, routingDiscovery, config.IDSSString)
	logger.Debug("Announced the IDSS service")

	// Look for other peers in the network who announced and connect to them
	anyConnected := false
	logger.Infoln("Searching for other peers...")
	fmt.Println("...")
	for !anyConnected {
		peerChan, err := routingDiscovery.FindPeers(ctx, config.IDSSString)
		if err != nil {
			logger.Error("Error finding peers, retrying in 2 seconds:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		for peer := range peerChan {
			if peer.ID == host.ID() {
				continue // Skip over self
			}

			logger.Debug("Found peer announced, connecting to it :", peer)

			err := host.Connect(ctx, peer)
			if err != nil {
				//logger.Error("Error connecting to peer:", err)
				continue
			}else{
				logger.Info("Connected to peer:", peer.ID)
				anyConnected = true
			}

			stream, err := host.NewStream(ctx, peer.ID, protocol.ID(config.ProtocolID))
			if err != nil {
				logger.Error("Connection failed:", err)
				continue
			}

			rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))

			go func() {
				defer stream.Close()
				writeData(rw)
			}()

			go func() {
				defer stream.Close()
				readData(rw)
			}()
		}
	}
	logger.Infof("Num of found peers in the DHT: %d", len(KADDHT.RoutingTable().GetPeerInfos()))

	// Print time taken to find peers
	logger.Info("Time taken to find peers: ", time.Since(startTime))
	logger.Info("Peer discovery completed")
}

func readData(rw *bufio.ReadWriter) {
	for {
		str, err := rw.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Warn("Connection closed by peer")
				return
			} else {
				logger.Error("Error reading from stream:", err)
			}
		}

		if str == "exit" {
			logger.Warn("Closing connection due to 'exit' command")
			return
		}

		if str == "" {
			logger.Warn("Closing connection due to empty string")
			return
		}

		if str != "\n" {
			// Green console colour: 	\x1b[32m
			// Reset console colour: 	\x1b[0m
			fmt.Printf("\x1b[32m%s\x1b[0m> ", str)
		}
	}
}

func writeData(rw *bufio.ReadWriter) {
	stdReader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		sendData, err := stdReader.ReadString('\n')
		if err != nil {
			logger.Error("Error reading from stdin:", err)
		}

		_, err = rw.WriteString(fmt.Sprintf("%s\n", sendData))
		if err != nil {
			logger.Error("Error writing to stream:", err)
		}

		err = rw.Flush()
		if err != nil {
			logger.Error("Error flushing stream:", err)
		}
	}
}

// Function to initialise the DHT and bootstrap the network with default nodes
func initialiseDHT(ctx context.Context, host host.Host, config Config) *dht.IpfsDHT {
	logger.Info("Initialising the DHT...")
	bootstrapPeers := make([]peer.AddrInfo, len(config.BootstrapPeers))

	if len(config.BootstrapPeers) == 0 {
		logger.Warn("No bootstrap peers provided. The network might have connectivity issues.")
	}

	kadDHT, err := dht.New(ctx, host, dht.BootstrapPeers(bootstrapPeers...))
	if err != nil {
		logger.Fatal("Error creating DHT", err)
	}

	logger.Debug("Bootstrapping the DHT")
	if err := kadDHT.Bootstrap(ctx); err != nil {
		logger.Fatalf("Error bootstrapping the DHT: %v", err)
		os.Exit(1)
	}

	logger.Info("Bootstrapped the DHT")

	var wg sync.WaitGroup
	for _, addr := range config.BootstrapPeers {
		peerinfo, _ := peer.AddrInfoFromP2pAddr(addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := host.Connect(ctx, *peerinfo); err != nil {
				logger.Warnf("Error connecting to bootstrap peer %s: %v", peerinfo.ID, err)
			}
		}()
		//bootstrapPeers[i] = *peerinfo
	}
	wg.Wait()

	return kadDHT
}

// A function to handle incoming requests from peers.
func handleRequest(conn network.Stream, REMOTE_ADDRESS string, ctx context.Context, config Config, gm *eliasdb.Manager) {
	logger.Infof("Handling request from %s", REMOTE_ADDRESS)
	defer conn.Close()

	for {
		// Read the incoming QueryMessage using protobuf
		msgBytes, err := readDelimitedMessage(conn, ctx)
		if err != nil {
			if err != io.EOF {
				logger.Errorf("Error reading from stream:", err)
			}
			return
		}

		// Decode the incoming query message
		var msg common.QueryMessage // Incoming query message
		err = proto.Unmarshal(msgBytes, &msg)
		if err != nil {
			logger.Errorf("Error unmarshalling query message: %v", err)
			return
		}

		// Handle message based on the message type
		switch msg.Type {
			case common.MessageType_QUERY:
				handleQuery(conn, &msg, REMOTE_ADDRESS, config, gm)
			case common.MessageType_RESULT:
				handleResult(conn, &msg, REMOTE_ADDRESS, gm)
			default:
				logger.Warn("Unknown message type received from %s", REMOTE_ADDRESS)
		}
	}
}

func handleQuery(conn network.Stream, msg *common.QueryMessage, REMOTE_ADDRESS string, config Config, gm *eliasdb.Manager) {
	logger.Infof("Query received on peer %s %s (UQI: %s)", KADDHT.Host().ID(), msg.Query, msg.Uqi)

	// Check query cache for duplicate queries
	duplicateQuery, err := checkDuplicateQuery(msg.Uqi, gm)
	if err != nil {
		logger.Errorf("Error checking duplicate query: %v", err)
		return
	}

	if len(duplicateQuery) > 0 {
		logger.Warnf("Duplicate query received on Peer: %s %s (UQI: %s)", KADDHT.Host().ID(), msg.Query, msg.Uqi)
		return
	}

	// Set query state to queued
	queryState := &common.QueryState{
		State: common.QueryState_QUEUED,
	}
	currentState := queryState.State.Enum()

	// Store query information in graph database
	storeQueryInfo(msg.Uqi, msg.Query, time.Now(), REMOTE_ADDRESS, msg.Ttl, currentState, gm)

	// print query state
	logger.Infof("Query state for query %s: %+v", msg.Query, currentState) // for debugging

	logger.Infof("This is a new query on Peer: %s %s (UQI: %s)", KADDHT.Host().ID(), msg.Query, msg.Uqi)

	executeAndBroadcastQuery(conn, msg, config, gm)
}

func handleResult(conn network.Stream, msg *common.QueryMessage, REMOTE_ADDRESS string, gm *eliasdb.Manager) {
	logger.Info("Result received on Peer: %s from Peer: %s", KADDHT.Host().ID(), REMOTE_ADDRESS)

	newResults := convertProtobufRowsToResult(msg.Result)

	originatingPeerID, err := peer.Decode(msg.GetOriginator())
	if err != nil {
		logger.Errorf("Error decoding originating peer ID: %v", err)
		return
	}

	// If this is originating peer, collect results locally
	if KADDHT.Host().ID() == originatingPeerID {
		logger.Infof("This is the originating peer %s. Collecting results locally", KADDHT.Host().ID())

		// Update query state to completed
		queryState := &common.QueryState{
			State:  common.QueryState_COMPLETED,
			Result: covertResultToProtobufRows(newResults),
		}

		updateQueryState(msg, gm, REMOTE_ADDRESS, queryState)

		logger.Infof("Query state for query %s: %+v", msg.Query, queryState.State)

		logger.Infof("Local result for peer %s (UQI: %s):  \n", KADDHT.Host().ID(), msg.Uqi)

		logger.Infof("%v\n", newResults)

		// Send the results to the client
		sendMergedResult(conn, peer.ID(REMOTE_ADDRESS), originatingPeerID, newResults)
	}else{
		logger.Infof("This is an intermediate peer %s. Sending results to parent peer %s", KADDHT.Host().ID(), originatingPeerID)
		sendMergedResult(conn, originatingPeerID, originatingPeerID, convertProtobufRowsToResult(msg.Result))
	}
}

func updateQueryState(msg *common.QueryMessage, gm *eliasdb.Manager, REMOTE_ADDRESS string, queryState *common.QueryState) {
	uqi := msg.Uqi
	// Update the query
	storeQueryInfo(uqi, "", time.Now(), REMOTE_ADDRESS, 0, queryState.State.Enum(), gm)
	logger.Infof("Query info updated for query (UQI: %s) state: %s", uqi, queryState.State)
}

func executeAndBroadcastQuery(conn network.Stream, msg *common.QueryMessage, config Config, gm *eliasdb.Manager) {
	var wg sync.WaitGroup
	localResultChan := make(chan [][]interface{}, 1) // Channel to receive local results
	//responseChannel := make(chan [][]interface{}, len(KADDHT.Host().Network().Peers())-1) // For remote results

	// Run local query 
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, header, err := executeLocalQuery(msg.Query, KADDHT.Host().ID(), gm)
		if err != nil {
			localResultChan <- nil
			logger.Errorf("Error executing local query: %v", err)
			return
		}

		// Handle nil header (for debugging or metadata) 
		if header != nil {
			labels := header.Labels()
			logger.Info("Labels: ", labels)
		} else {
			labels := []string{}
			logger.Info("Labels: ", labels)
		}

		// Update the query state to locally executed in this peer
		queryState := &common.QueryState{
			State:  common.QueryState_LOCALLY_EXECUTED,
		}

		updateQueryState(msg, gm, conn.Conn().RemotePeer().String(), queryState)

		logger.Infof("Query state for query %s: %+v", msg.Query, queryState.State.Enum())

		logger.Infof("Local result for peer %s (UQI: %s):  \n", KADDHT.Host().ID(), msg.Uqi)
		logger.Infof("%v\n", result)

		// Send the results to the channel
		localResultChan <- result
	}()

	holdHere := <-localResultChan

	// Convert results to protobuf rows
	localResults := covertResultToProtobufRows(holdHere)

	// Store the local results in the query message
	msg.Result = localResults

	// Run broacastquery in a goroutine
	if msg.Ttl > 0 {// if we still have time
		logger.Infof("Broadcasting query to connected peers...")
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Mow update sender address
			msg.Sender = KADDHT.Host().ID().String() // So that the overlay peers identify this as parent peer
			logger.Info("Sender address updated to: ", msg.Sender)
			
			//convertedLocalResults := convertProtobufRowsToResult(localResults)
			broadcastQuery(msg, conn, config, holdHere, gm)
		}()
	}

	// Wait for all goroutines to complete
	go func ()  {
		wg.Wait()
		close(localResultChan)
	}()
	
	//localResults = covertResultToProtobufRows(<-localResultChan)
}

// Checks if the query is already in the graph database
func checkDuplicateQuery(uqi string, gm *eliasdb.Manager) ([][]interface{}, error) {
	statement := fmt.Sprintf("get Query where key = '%s'", uqi)

	checkQuery, err := eql.RunQuery("checkQuery", "main", statement, gm)
	if err != nil {
		return nil, err
	}
	return checkQuery.Rows(), nil
}

// Function to store query information in the graph database
func storeQueryInfo(uqi, query string, arrival_time time.Time, REMOTE_ADDRESS string, ttl float32, queryState *common.QueryState_State, graphManager *eliasdb.Manager) {

	// Create a new transaction
	trans := eliasdb.NewGraphTrans(graphManager)

	// Create a new query node
	queryNode := data.NewGraphNode()
	queryNode.SetAttr("key", uqi)
	queryNode.SetAttr("kind", "Query")
	queryNode.SetAttr("name", "Query")
	queryNode.SetAttr("query_string", query)
	queryNode.SetAttr("arrival_time", arrival_time.Unix())
	queryNode.SetAttr("ttl", ttl)
	queryNode.SetAttr("sender_address", REMOTE_ADDRESS)
	queryNode.SetAttr("state", queryState.String())

	// Store the query node in the graph database
	trans.StoreNode("main", queryNode)

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		logger.Errorf("Error committing transaction: %v", err)
		return
	}
	logger.Infof("Query info stored for query %s (UQI: %s) state: %s, from %v", query, uqi, queryState, REMOTE_ADDRESS)
}

// Function to broadcast the query to connected peers. This function also filters out the originating and parent peers because it is already queried
func broadcastQuery(msg *common.QueryMessage, parentStream network.Stream, config Config, localResults [][]interface{}, gm *eliasdb.Manager) {
	logger.Info("Broadcasting query from peer: ", KADDHT.Host().ID())
	logger.Infof("Received this query from peer: %s", parentStream.Conn().RemotePeer())

	connectedPeers := KADDHT.Host().Network().Peers() //TODO: Return to routing table
	//peersInRoutingTable := KADDHT.RoutingTable().ListPeers()

	// Filter peers to exclude the originating peer and the parent stream peer
	var eligiblePeers []peer.ID
	for _, peerID := range connectedPeers {
		if peerID != KADDHT.Host().ID() && (parentStream == nil || parentStream.Conn().RemotePeer() != peerID) {
			// Ensure that we don't query self, the parent stream peer or the peer that sent the query
			// Check if there is an active connection to the peer
			eligiblePeers = append(eligiblePeers, peerID)
		}
	}

	if len(eligiblePeers) == 0 {
        logger.Warn("No eligible peers for broadcast")
        return
    }

	// Decrease the TTL
	newTTL := msg.Ttl * 0.75
	logger.Infof("TTL in %v after decrement: %f", KADDHT.Host().ID(), newTTL)
	if newTTL <= 0 {
		logger.Warn("TTL expired, not broadcasting query")

		// Update the query state to SENT_BACK and send the results to the parent peer
		queryState := &common.QueryState{
			State:  common.QueryState_SENT_BACK,
		}

		updateQueryState(msg, gm, "", queryState)

		// extract originator peer from the query message
		originatingPeerID, err := peer.Decode(msg.GetOriginator())
		if err != nil {
			logger.Errorf("Error decoding originating peer ID: %v", err)
			return
		}
		sendMergedResult(parentStream, parentStream.Conn().RemotePeer(), originatingPeerID, localResults)
	}


	msg.Ttl = newTTL

	// Prepare for concurrent broadcasting
	var localWg sync.WaitGroup
	localWg.Add(len(eligiblePeers))

	for _, peerID := range eligiblePeers {
		
		go func(p peer.ID){
			defer localWg.Done()

			// Create a new context and stream to the peer
			streamCtx, streamCancel := context.WithTimeout(context.Background(), time.Duration(newTTL)*time.Second) //TODO: Change the timeout
			defer streamCancel()

			stream, err := KADDHT.Host().NewStream(streamCtx, p, protocol.ID(config.ProtocolID))
			if err != nil {
				//logger.Errorf("Error opening stream to peer %s: %v", p, err)
				return
			}
			defer stream.Close()

			// Serialise the QueryMessage using Protobuf
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

			logger.Infof("Query sent to peer %s", p)

			logger.Infof("Waiting for response from peer %s", p)

			// Read results from the stream and send to the parent peer
			// Read remote results from stream, merge with local results and send to parent peer
			resultsByte, err := readDelimitedMessage(stream, context.Background()) 
			if err != nil {
				if err != io.EOF {
					logger.Infof("Connection closed by peer %s: %v", p, err)
				}else{
					logger.Errorf("Error reading remote results: %v", err)
				}
				return
			}

			logger.Info("Received results from peer: ", p)

			// Decode the incoming query message
			var remoteResults common.QueryMessage
			err = proto.Unmarshal(resultsByte, &remoteResults)
			if err != nil {
				logger.Errorf("Error unmarshalling remote results: %v", err)
				return
			}

			// Check if received message is of type RESULT
			if remoteResults.Type == common.MessageType_RESULT {
				remoteResult := convertProtobufRowsToResult(remoteResults.Result)
				logger.Infof("Received results from peer %s: %v", p, remoteResult)

				// Merge the local and remote results
				localResults = mergeTwoResults(localResults, remoteResult) // Here localResults holds the merged results from this peer and its children
				logger.Info("Merged results: ", localResults)
			}
		}(peerID)
	}

	// Wait for all goroutines to complete
	logger.Info("Waiting for overlay peers to respond...")
	localWg.Wait()
	logger.Info("Overlay peers have responded")

	// extract originator peer from the query message
	originatingPeerID, err := peer.Decode(msg.GetOriginator())
	if err != nil {
		logger.Errorf("Error decoding originating peer ID: %v", err)
		return
	}

	if KADDHT.Host().ID() == originatingPeerID {
		logger.Infof("This is the originating peer %s. Merging results locally", originatingPeerID)
		
		// Update the query state to completed
		queryState := &common.QueryState{
			State:  common.QueryState_COMPLETED,
		}

		updateQueryState(msg, gm, "", queryState)

		// Send the results to the client
		clientPeerID, err := peer.Decode(msg.Sender)
		if err != nil {
			logger.Errorf("Error decoding client peer ID: %v", err)
			return
		}
		sendMergedResult(parentStream, clientPeerID, originatingPeerID, localResults)
	}else{
		// Get the parent peer ID
		parentPeerID, err := peer.Decode(msg.Sender)
		if err != nil {
			logger.Errorf("Error decoding parent peer ID: %v", err)
			return
		}

		// Update the query state to sent back
		queryState := &common.QueryState{
			State:  common.QueryState_SENT_BACK,
		}

		updateQueryState(msg, gm, "", queryState)

		logger.Infof("This is an intermediate peer %s. Merging results to send to parent peer %s", KADDHT.Host().ID(), parentPeerID)
		// If time has elapsed and remote peer is the client then send the result to the client
		sendMergedResult(parentStream, parentPeerID, originatingPeerID, localResults)
	}
	
}

// Function to send the merged result to the client
func sendMergedResult(stream network.Stream, parentPeerD peer.ID, originatingPeerID peer.ID,  result [][]interface{}) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer stream.Close()

    	logger.Infof("Sending merged result to peer %s", parentPeerD)

		resultRows := covertResultToProtobufRows(result)
		var queryState *common.QueryState

		if KADDHT.Host().ID() == originatingPeerID {
			logger.Infof("This is the originating peer %s. Sending results to client %s", originatingPeerID, parentPeerD)
			queryState = &common.QueryState{
				State:  common.QueryState_COMPLETED,
			}
		}else{
			logger.Infof("This is an intermediate peer %s. Sending results to parent peer %s", KADDHT.Host().ID(), parentPeerD)
			queryState = &common.QueryState{
				State:  common.QueryState_SENT_BACK,
			}
		}

		resultMsg := &common.QueryMessage{
			Type:       common.MessageType_RESULT,
			Result:     resultRows,
			State: 		queryState,
		}

		// Marshal the QueryResult message to bytes using Protobuf
		resultBytes, err := proto.Marshal(resultMsg)
		if err != nil {
			logger.Errorf("Error marshalling result for peer %s: %v", KADDHT.Host().ID(), err)
			return
		}

		if err := writeDelimitedMessage(stream, resultBytes); err != nil {
			logger.Errorf("Error writing merged result to peer %s: %v", parentPeerD, err)
		} else {
			logger.Infof("Successfully sent merged result to peer %s", parentPeerD)
		}
	}()

	// Ensure all operations are completed before closing the stream
	wg.Wait()
	if err := stream.Close(); err != nil {
		logger.Errorf("Error closing stream to peer %s: %v", parentPeerD, err)
	}
}

func mergeTwoResults(result1, result2 [][]interface{}) [][]interface{} {
	logger.Infof("Merging two results: %v and %v in peer: %s", result1, result2, KADDHT.Host().ID())
	// If either result is empty, just return the other
    if len(result1) == 0 {
        return result2
    }
    if len(result2) == 0 {
        return result1
    }
	combinedResults := append(result1, result2...)
	return combinedResults
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
func covertResultToProtobufRows(result [][]interface{}) []*common.Row {
	var rows []*common.Row
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

	// Set read timeout to avoing hanging over
	//readTimeout := 200 * time.Second
	deadlineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sizeChan := make(chan uint64, 1)
	errChan := make(chan error, 1)

	// Read the message size (varint encoded)
	go func(){
		size, err := binary.ReadUvarint(bufReader)
		if err != nil {
			//logger.Errorf("Error reading message size: %v", err)
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
	// Write the message size (varint encoded)
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

// Function to execute local query
func executeLocalQuery(command string, peer peer.ID, gm *eliasdb.Manager) ([][]interface{}, eql.SearchResultHeader, error) {
	logger.Infof("Executing %s locally in %s", command, peer)

	// Acquire the mutex for the peer before querying
	logger.Infof("Acquiring mutex for peer %s", peer)

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
