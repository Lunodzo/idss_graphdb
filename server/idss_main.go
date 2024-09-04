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
	DB_PATH             = "../server/idss_graph_db" 
	PORT                = "8080"                    //optional
)

var (
	KADDHT            *dht.IpfsDHT
	GRAPH_MANAGER     *eliasdb.Manager
	activeConnections int64
	logger			= log.Logger("IDSS")
	queryCache        sync.Map // A concurrent map to store queries
)

/*
=====================
MAIN FUNCTION
=====================
*/

// Main function to start the server operations
func main() {
	// Set up logging to file
	log.SetAllLoggers(log.LevelWarn) // Or log.LevelInfo, log.LevelDebug, etc.
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
		logger.Info("Listening on peer Address:", completePeerAddr)
	}

	/**********************

		Graph database operations using EliasDB. This must be affected in all the connected peers.
		Calling functions from idss_db_init.go to create the database nodes and edges.

		When a node do not have any file, it will skip the database creation and will not create any
		nodes and edges in the graph database. In the event of data fetching it will fetch from
		other peers.

	************************/

	// Initialise the database instance
	dbPath := fmt.Sprintf("%s/%s", DB_PATH, host.ID().String()) 
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		os.MkdirAll(dbPath, 0755)
	}

	// Create a new graph database instance
	graphDB, err := graphstorage.NewDiskGraphStorage(dbPath, false)
	CheckError(err, "Error creating graph database")
	//defer graphDB.Close()
	GRAPH_MANAGER = eliasdb.NewGraphManager(graphDB)

	// Initialize the database only if the filename is provided
	if filename != "" {
		logger.Info("Creating nodes and edges in the graph database based on json data...")
		// Read the sample data
		Database_init(filename, dbPath, graphDB, GRAPH_MANAGER)
		logger.Info("Graph database nodes and edges created successfully")
	} else {
		logger.Info("No file provided. Skipping database creation...")
	}

	// Initialise the DHT
	KADDHT = initialiseDHT(ctx, host, config)

	// A go routine to refresh the routing table and periodically find and connect to peers
	go discoverAndConnectPeers(ctx, host, config)

	// Handle streams
	host.SetStreamHandler(protocol.ID(config.ProtocolID), func(stream network.Stream) {
		atomic.AddInt64(&activeConnections, 1)
		defer atomic.AddInt64(&activeConnections, -1) // decrement when the handler exits
		go handleRequest(stream, stream.Conn().RemotePeer().String(), ctx, config)
	})

	// Handle SIGTERM and interrupt signals
	HandleInterrupts(host, peer.AddrInfo{}, graphDB)

	// Keep the server running
	select {}
}

//TODO: Add condition to handle peer disconnection. Data should be reserved in the DHT
func HandleInterrupts(host host.Host, peer peer.AddrInfo, graphDB graphstorage.Storage) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Monitor connection state
	host.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(net network.Network, conn network.Conn) {
			//logger.Infof("Disconnected from peer: %s", conn.RemotePeer())
			handlePeerDisconnection(conn.RemotePeer(), graphDB)
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
func handlePeerDisconnection(iD peer.ID, graphDB graphstorage.Storage) {
	// preserve the data in the DHT TODO: There is a task to be done here
	//logger.Infof("Preserving data for peer %s", iD, graphDB)
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
	logger.Infoln("Searching for other peers...") //3YPK4CV 
	fmt.Println("...")
	for !anyConnected {
		peerChan, err := routingDiscovery.FindPeers(ctx, config.IDSSString)
		if err != nil {
			logger.Error("Error finding peers:", err)
		}

		for peer := range peerChan {
			if peer.ID == host.ID() {
				continue // Skip over self
			}

			logger.Debug("Found peer announced, connecting to it :", peer)

			err := host.Connect(ctx, peer) 
			if err != nil {
				continue
			}

			// Advertise the protocol immediately after connecting to the peer
			routingDiscovery := drouting.NewRoutingDiscovery(KADDHT)
			dutil.Advertise(ctx, routingDiscovery, config.IDSSString)

			stream, err := host.NewStream(ctx, peer.ID, protocol.ID(config.ProtocolID))
			if err != nil {
				logger.Error("Connection failed:", err)
				continue
			}else{
				logger.Info("Connected to peer:", peer.ID)
				anyConnected = true

				rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))

				go writeData(rw)
				go readData(rw)
			}
		}
	}

	logger.Info("Finished peer discovery")
	logger.Infof("Num of found peers in the DHT: %d", len(KADDHT.RoutingTable().GetPeerInfos()))

	// Print time taken to find peers
	logger.Info("Time taken to find peers: ", time.Since(startTime))
	logger.Info("Peer discovery completed")
}

func readData(rw *bufio.ReadWriter) {
	for{
		str, err := rw.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Warn("Connection closed by peer")
				return
			}else{
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

	for i, addr := range config.BootstrapPeers {
		peerinfo, _ := peer.AddrInfoFromP2pAddr(addr)
		bootstrapPeers[i] = *peerinfo
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

	return kadDHT
}

// A function to handle incoming requests from clients
func handleRequest(conn network.Stream, REMOTE_ADDRESS string, ctx context.Context, config Config) {
	logger.Infof("Handling request from %s", REMOTE_ADDRESS)
	defer conn.Close()

	for {
		// Read the incoming QueryMessage using protobuf
		msgBytes, err := readDelimitedMessage(conn, ctx)
		if err != nil {
			if err != io.EOF {
				logger.Errorf("Error reading from stream:", err)
			}
			break
		}

		// Decode the incoming query message
		var msg common.QueryMessage 
		err = proto.Unmarshal(msgBytes, &msg)
        if err != nil {
			logger.Errorf("Error unmarshalling query message: %v", err)
			continue
        }

		query := msg.Query
		uqi := msg.Uqi
		ttl := msg.Ttl

		peerQueryCacheKey := fmt.Sprintf("%s-%s", KADDHT.Host().ID(), uqi)

		// Initialise the query state
		queryState := &common.QueryState{}

		// Check query cache for duplicate queries
		if _, loaded := queryCache.LoadOrStore(peerQueryCacheKey, struct{}{}); loaded {
			logger.Warnf("Duplicate query received on this peer: Peer: %s %s (UQI: %s)", KADDHT.Host().ID(), query, uqi)
			continue // Skip duplicate query
		}else{
			logger.Infof("New query received on this peer: Peer: %s %s (UQI: %s)", KADDHT.Host().ID(), query, uqi)
		}

		// Create context with deadline based on the TTL
		queryCtx, cancel := context.WithTimeout(ctx, time.Duration(ttl) * time.Second)
		defer cancel()

		localResult, header, err := executeLocalQuery(query, KADDHT.Host().ID())
		if err != nil {
			logger.Errorf("Error executing local query in %s: %v", KADDHT.Host().ID(), err)

			//TODO: if the peer does not have database, forward the query to other peers
			
			// Send the error back to the client
			queryState = &common.QueryState{
				State: common.QueryState_FAILED,
			}

			// Create the error message
			errorMsg := &common.QueryResult{
				PeerId: KADDHT.Host().ID().String(),
				QueryState: queryState,
				Error: err.Error(),
			}

			// Marshal the error message
			errorBytes, err := proto.Marshal(errorMsg)
			if err != nil {
				logger.Errorf("Error marshalling error message: %v", err)
				continue
			}

			// Write the error message to the client
			err = writeDelimitedMessage(conn, errorBytes)
			if err != nil {
				logger.Errorf("Error writing error message to client %s: %v", REMOTE_ADDRESS, err)
				break
			}
			continue //TODO: Should we break here?
		}

		// Handle nil header
		if header != nil {
			logger.Info("Labels: ", header.Labels())
			logger.Info("Peer: ", KADDHT.Host().ID())
		}else{
			logger.Warn("Header is nil")
		}

		logger.Infof("Local result for peer %s (UQI: %s): %v", KADDHT.Host().ID(), uqi, localResult)

		var wg sync.WaitGroup

		if ttl > 0 {
			broadcastQuery(queryCtx, &msg, conn, config, &wg)
		}

		// Wait for all goroutines to finish
		wg.Wait()

		logger.Infof("Now merging results for query %s (UQI: %s)", query, uqi)

		// Wait for and merge results from all peers
		mergedResult, err := receiveMergeResults(queryCtx, conn, localResult) 
		if err != nil {
			if err == context.Canceled {
				// Context cancelled, return partial results anyway
				logger.Warnf("Context cancelled while merging results for query %s (UQI: %s)", query, uqi)

				// Send the merged result back to the client
				finalResult := &common.QueryResult{
					PeerId: KADDHT.Host().ID().String(),
					Result: covertResultToProtobufRows(mergedResult),
					QueryState: queryState,
				}

				// Convert the merged result to a string
				formattedResult, err := proto.Marshal(finalResult)
				if err != nil {
					logger.Errorf("Error marshalling merged result: %v", err)
					continue
				}

				// Send the merged result back to the client
				err = writeDelimitedMessage(conn, formattedResult)
				if err != nil {
					logger.Errorf("Error writing merged result to client %s: %v", REMOTE_ADDRESS, err)
					break
				}
				return 
			}
			logger.Errorf("Error merging results for query %s (UQI: %s): %v", query, uqi, err)
			continue
		}

		// Update the query state to completed
		queryState = &common.QueryState{
			State: common.QueryState_COMPLETED,
			Result: covertResultToProtobufRows(mergedResult),
		}

		// log the merged result
		logger.Infof("Merged results for query %s (UQI: %s): %v", query, uqi, mergedResult)

		if len(mergedResult) == 0 {
			logger.Warnf("No results found for query %s (UQI: %s)", query, uqi)
			continue
		}

		if msg.GetTtl() == 0 {
			logger.Warnf("TTL expired for query %s (UQI: %s)", query, uqi)

			// Convert the merged result to protobuf
			finalResult := &common.QueryResult{
				PeerId: KADDHT.Host().ID().String(),
				Result: covertResultToProtobufRows(mergedResult),
				QueryState: queryState,
			}

			// Convert the merged result to a string
			formattedResult, err := proto.Marshal(finalResult)
			if err != nil {
				logger.Errorf("Error marshalling merged result: %v", err)
				continue
			}

			// Send the merged result back to the client
			err = writeDelimitedMessage(conn, formattedResult)
			if err != nil {
				logger.Errorf("Error writing merged result to client %s: %v", REMOTE_ADDRESS, err)
				break
			}
		}
		queryCache.Delete(peerQueryCacheKey)
	}
}

// Function to broadcast the query to connected peers. This function also filters out the originating peer because it is already queried
func broadcastQuery(ctx context.Context, msg *common.QueryMessage, parentStream network.Stream, config Config, wg *sync.WaitGroup) {
	logger.Info("Broadcasting query from peer: ", KADDHT.Host().ID())

	connectedPeers := KADDHT.Host().Network().Peers()

	// Filter peers to exclude the originating peer and the parent stream peer
	var eligiblePeers []peer.ID
	for _, peerID := range connectedPeers {
		if peerID != KADDHT.Host().ID() && (parentStream == nil || parentStream.Conn().RemotePeer() != peerID) { // Skip self
			eligiblePeers = append(eligiblePeers, peerID)
		}
	}

	if parentStream == nil {
		logger.Error("parentStream is nil in broadcastQuery") // we cant proceed without the parent stream, TODO: right?
		return
	}

	if len(eligiblePeers) == 0 {
		logger.Warn("No eligible peer to broadcast query to")
		return
	}else{
		logger.Infof("Broadcasting query to %d peers", len(eligiblePeers)) // For debugging
	}

	for _, peerID := range eligiblePeers {
		wg.Add(1)
		go func (p peer.ID)  {
			// p has to a local peer not the parent stream peer
			defer wg.Done()
			// Execute the query on the peer and send the result back to the channel
			// Do not query to the kademlia peer
			if p == KADDHT.Host().ID() {
				logger.Infof("Skipping query to self: %s", p)
				return
			}
			queryPeer(ctx, p, msg, parentStream, config, wg) 
		}(peerID)
	}
}

// Function to query a single peer
func queryPeer(ctx context.Context, peerChild peer.ID, msg *common.QueryMessage, parentStream network.Stream, config Config, wg *sync.WaitGroup) {
	logger.Infof("Entering queryPeer for peer %s", peerChild)
	// Are the DHT and host are initialised?
	if KADDHT == nil || KADDHT.Host() == nil {
		logger.Errorf("DHT or its host is not initialised")
	}

	if parentStream == nil {
		logger.Error("parentStream is nil in broadcastQuery")
		logger.Errorf("parentStream is nil")
	}

	// Print the ttl
	logger.Infof("TTL in %v before decrement: %d", peerChild, msg.Ttl)

	// Decrease the TTL
	msg.Ttl--
	var stream network.Stream
    var err error

	// Update context with new deadline
	deadline := time.Now().Add(time.Duration(msg.GetTtl()) * time.Second)
	queryCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// Create new stream to the peer
	stream, err = KADDHT.Host().NewStream(queryCtx, peerChild, protocol.ID(config.ProtocolID))
	if err != nil {
		// skip to the next peer
		logger.Errorf("error opening stream to peer: %v", err)
	}

	logger.Info("Stream opened to peer: ", peerChild)

	// Control duplicate queries (per-peer query cache)
	peerQueryCacheKey := fmt.Sprintf("%s-%s", peerChild, msg.Uqi)
	if _, loaded := queryCache.LoadOrStore(peerQueryCacheKey, struct{}{}); loaded {
		logger.Infof("Duplicate query received: Peer: %s %s (UQI: %s)", peerChild, msg.Query, msg.Uqi)
		logger.Errorf("duplicate query received: %s", msg.Query)
	}

	// Run the query on the peer
	localResults, header, err := executeLocalQuery(msg.Query, peerChild) 
	if err != nil {
		logger.Errorf("error executing local query: %v", err)
	}

	// Handle nil header
	//var labels []string
	if header != nil {
		logger.Info("Labels: ", header.Labels())
		//labels := header.Labels()
	}else{
		logger.Warn("Header is nil")
		//labels = []string{}
	}

	logger.Infof("Local result for peer %s: %v", peerChild, localResults)

	// Initiating query state for the peer
	queryState := &common.QueryState{
		State: common.QueryState_LOCALLY_EXECUTED,
		Result: covertResultToProtobufRows(localResults),
	}

	// Print ttl and query state
	logger.Infof("TTL: %d, Peer: %v", msg.GetTtl(), peerChild)

	// Broadcast the query to other peers if TTL is greater than 0
	if msg.GetTtl() > 0 {
		logger.Infof("Peer %v broadcasting to its space", peerChild)
		broadcastQuery(queryCtx, msg, stream, config, wg)
	}

	// Wait for TTL to reach 0 or context to be done
	<-queryCtx.Done()

	mergedChildResults, err := receiveMergeResults(queryCtx, stream, localResults)
	if err != nil {
		if err == context.DeadlineExceeded{
			logger.Info("Timeout reading response from peer : %v", peerChild)

			// Update state
			queryState.State = common.QueryState_COMPLETED
			queryState.Result = covertResultToProtobufRows(mergedChildResults)

			// Create results message
			resultMsg := &common.QueryResult{
				PeerId: peerChild.String(),
				Result: covertResultToProtobufRows(mergedChildResults),
				Labels: header.Labels(),
				QueryState: queryState,
			}

			// Marshal the QueryResult message
			resultBytes, err := proto.Marshal(resultMsg)
			if err != nil {
				logger.Errorf("Error marshalling result: %v", err)
			}

			// Write the result to the parent stream
			err = writeDelimitedMessage(parentStream, resultBytes)
			if err != nil {
				logger.Errorf("Error writing remote result to parent peer %s: %v", parentStream.Conn().RemotePeer(), err)
			}

			// Change the state to SENT_BACK
			queryState.State = common.QueryState_SENT_BACK

			// Log the merged result and query state
			logger.Infof("Merged results for peer %s: %v", peerChild, mergedChildResults)
			logger.Infof("Query state for peer %s: %v", peerChild, queryState)

			// Close the stream if its its not the parent stream and if TTL has reached 0 and the query has been sent back
			if parentStream.Conn().RemotePeer() != peerChild && queryState.State == common.QueryState_SENT_BACK && msg.GetTtl() == 0 {
				stream.Close()
			}
			
		}else{
			logger.Errorf("Error merging results from peer %s: %v", peerChild, err)
			queryState.State = common.QueryState_FAILED
			
			// proceed to the next peer
			return
		}
	}

	// update state
	queryState.State = common.QueryState_SENT_BACK

	// Create results message
	resultMsg := &common.QueryResult{
		PeerId: peerChild.String(),
		Result: covertResultToProtobufRows(mergedChildResults),
		Labels: header.Labels(),
		QueryState: queryState,
	}

	// Marshal the QueryResult message
	resultBytes, err := proto.Marshal(resultMsg)
	if err != nil {
		logger.Errorf("Error marshalling result: %v", err)
	}

	// Write the result to the parent stream
	err = writeDelimitedMessage(parentStream, resultBytes)
	if err != nil {
		logger.Errorf("Error writing remote result to parent peer %s: %v", parentStream.Conn().RemotePeer(), err)
	}

	// Close the stream if its its not the parent stream
	if parentStream.Conn().RemotePeer() != peerChild && queryState.State == common.QueryState_SENT_BACK && msg.GetTtl() == 0 {
		stream.Close()
	}
	//return mergedChildResults, nil
}


func receiveMergeResults(ctx context.Context, stream network.Stream, localResults [][]interface{}) ([][]interface{}, error) {
	mergedResults := localResults
	for{
		select{
			case <-ctx.Done():
				return mergedResults, ctx.Err()
			default:
				resBytes, err := readDelimitedMessageWithContext(stream, ctx)
				if err != nil {
					if err == io.EOF {
						logger.Warn("Connection closed by peer")
						return mergedResults, nil
					}
					return nil, err
				}

				var res common.QueryResult
				err = proto.Unmarshal(resBytes, &res)
				if err != nil {
					return nil, fmt.Errorf("error unmarshalling remote result: %v", err)
				}

				// Merge received results with local results
				receivedResults := convertProtobufRowsToResult(res.Result)
				mergedResults = mergeTwoResults(mergedResults, receivedResults)

				// Log the merged results
				logger.Infof("Merged results from peer %s: %v", res.PeerId, mergedResults)
				return mergedResults, nil
		}
	}
}

func mergeTwoResults(result1, result2 [][]interface{}) [][]interface{} {
	combinedResults := append(result1, result2...)
    return combinedResults
}

func readDelimitedMessageWithContext(r io.Reader, queryCtx context.Context) ([]byte, error) {
	// Read the message size (varint encoded)
	logger.Info("Reading message size...", queryCtx)
    size, err := binary.ReadUvarint(bufio.NewReader(r))
    if err != nil {
        return nil, err
    }

	// Check if size is zero-length
	if size == 0 {
		return nil, fmt.Errorf("zero-length message received")
	}

    // Read the message data
	buf := make([]byte, size)
    done := make(chan struct{})
    go func() {
        _, err = io.ReadFull(r, buf)
        close(done)
    }()

	select {
	case <-done:

	case <-queryCtx.Done():
		return nil, queryCtx.Err()
	}

	if err != nil {
		return nil, err
	}

    return buf, nil
}

func convertProtobufRowsToResult(rows []*common.Row) [][]interface{}{
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
func readDelimitedMessage(r io.Reader, _ context.Context) ([]byte, error) {
	// Read the message size (varint encoded)
	size, err := binary.ReadUvarint(bufio.NewReader(r))
    if err != nil {
        return nil, err
    }

    // Read the message data
    buf := make([]byte, size)
    _, err = io.ReadFull(r, buf)
    if err != nil {
        return nil, err
    }

    return buf, nil
}

func writeDelimitedMessage(w io.Writer, data []byte)error{
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
func executeLocalQuery(command string, peer peer.ID) ([][]interface{}, eql.SearchResultHeader, error) {
	logger.Infof("Executing %s locally in %s", command, peer)
	result, err := eql.RunQuery("myQuery", "main", command, GRAPH_MANAGER)
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