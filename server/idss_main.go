/*
HOW TO RUN
1. Run the server using the command: go run idss_main.go -f <filename> (go run . -f data1.json)
2. Run the client using the command: go run idss_client.go -s <server_multiaddress>
3. Enter your query or 'exit' to quit.
4. The client will send the query to the server and print the response.
5. The client will continue to send queries until the user types 'exit'.
*/

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/krotik/eliasdb/eql"
	eliasdb "github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/graphstorage"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"

	//"google.golang.org/protobuf/proto"
	//"google.golang.org/protobuf/types/known/timestamppb"
	logrotate "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
)

/*
*********
Constants
**********
*/
const (
	DB_PATH             = "../server/idss_graph_db" 
	PORT                = "8080"                    //optional
	discoveryServiceTag = "kadProtocol"             // The tag used to advertise and discover the IDSS service
	IDSS_PROTOCOL       = protocol.ID("/idss/1.0.0")
	MAX_HOPS = 5
)

var (
	KADDHT            *dht.IpfsDHT
	GRAPH_MANAGER     *eliasdb.Manager
	activeConnections int64
	log               = logrus.New()
	queryCache        sync.Map // A concurrent map to store queries
	logWithFile       *logrus.Logger
)

// Channel to collect query results from peers
type QueryResult struct {
	PeerID peer.ID
	Result string
	Error  error
}

type QueryMessage struct {
	Query string
	UQI   string
	Hop   int // Number of hops the query has made
}

/*
=====================
MAIN FUNCTION
=====================
*/
func main() {
	// Set up logging to file
	logFile, err := logrotate.New(
		"idss_server.log.%Y%m%d", // Log rotation by day (change pattern as needed)
		logrotate.WithLinkName("idss_server.log"),
		logrotate.WithMaxAge(24*time.Hour*7),     // Keep logs for a week (optional)
		logrotate.WithRotationTime(24*time.Hour), // Rotate at midnight every day (optional)
	)
	if err != nil {
		log.Fatalf("Failed to create log file: %v", err)
	}
	defer logFile.Close()

	// Create a new logger with the rotated log file as output
	logWithFile = logrus.New()
	logWithFile.SetFormatter(&logrus.TextFormatter{})
	logWithFile.SetOutput(logFile)

	// String flag to specify the database file
	filename := flag.String("f", "", "JSON file containing the graph data")

	// Parse command line arguments
	flag.Parse()

	// Increase the buffer size
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{
		Cur: 4096,
		Max: 8192,
	})
	if err != nil {
		log.Fatal("Error setting rlimit:", err)
	}

	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Generate key pair for the server/host
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		log.Fatal("Error generating RSA key pair:", err)
	}

	// Dynamic port allocation
	listenAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		log.Fatal("Error creating listening address:", err)
	}

	// Create libp2p host
	host, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrs(listenAddr),
	)
	if err != nil {
		panic(err)
	}

	// Print a complete peer listening address
	for _, addr := range host.Addrs() {
		completePeerAddr := addr.Encapsulate(multiaddr.StringCast("/p2p/" + host.ID().String()))
		log.Info("Listening on peer Address:", completePeerAddr)
	}

	/**********************

	Graph database operations using EliasDB. This must be affected in all the connected peers.
	Calling functions from idss_db_init.go to create the database nodes and edges.

	When a node do not have any file, it will skip the database creation and will not create any
	nodes and edges in the graph database. In the event of data fetching it will fetch from
	other peers.

	************************/

	// Initialise the database instance
	initDBIntance(host.Peerstore().PeerInfo(host.ID()), *filename)

	// Check if the filename is provided
	if *filename != "" {
		log.Info("Creating nodes and edges in the graph database based on json data...")
		// Read the sample data
		Database_init(*filename, string(host.ID()))
		logWithFile.Info("Graph database nodes and edges created successfully")
	} else {
		log.Info("No file provided. Skipping database creation...")
	}

	// Initialise the DHT
	KADDHT = initialiseDHT(ctx, host)

	// A go routine to refresh the routing table and periodically find and connect to peers
	go discoverAndConnectPeers(ctx, host)

	// Handle streams
	host.SetStreamHandler(IDSS_PROTOCOL, func(stream network.Stream) {
		atomic.AddInt64(&activeConnections, 1)
		defer atomic.AddInt64(&activeConnections, -1) // decrement when the handler exits
		go handleRequest(stream, stream.Conn().RemotePeer().String(), ctx)
	})

	// Handle SIGTERM and interrupt signals
	HandleInterrupts(host, peer.AddrInfo{})

	// Keep the server running
	select {}
}

func HandleInterrupts(host host.Host, peer peer.AddrInfo) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-c
		log.Printf("Received signal: %s. Peer exiting the network: %s", sig, host.ID().String())

		for atomic.LoadInt64(&activeConnections) > 0 {
			time.Sleep(100 * time.Millisecond) // Poll until all connections are closed
		}

		// Clean up
		if err := host.Close(); err != nil {
			log.Error("Error shutting down IDSS: ", err)
			logWithFile.Error("Error shutting down IDSS: ", err)
			os.Exit(1) // Different termination codes can be used
		}

		// Delete the database
		DB_PATH_PEER := DB_PATH + "/" + peer.ID.String()
		if err := os.RemoveAll(DB_PATH_PEER); err != nil {
			log.Error("Error deleting database: ", err)
			logWithFile.Error("Error deleting database: ", err)
		}
		os.Exit(0) // normal termination
	}()
}

// Function to discover and connect to peers
func discoverAndConnectPeers(ctx context.Context, host host.Host) {
	log.Info("Starting peer discovery...")
	logWithFile.Info("Starting peer discovery...")
	startTime := time.Now()

	routingDiscovery := drouting.NewRoutingDiscovery(KADDHT)
	dutil.Advertise(ctx, routingDiscovery, discoveryServiceTag)

	// Look for other peers in the network who announced and connect to them
	anyConnected := false
	log.Info("Searching for peers...")
	for !anyConnected {
		// Print a counter to show progress
		fmt.Print(".")

		peerChan, err := routingDiscovery.FindPeers(ctx, discoveryServiceTag)
		if err != nil {
			log.Error("Error finding peers:", err)
			logWithFile.Error("Error finding peers:", err)
		}

		for peer := range peerChan {
			if peer.ID == host.ID() {
				continue // Skip over self
			}

			err := host.Connect(ctx, peer)
			if err != nil {
				//log.Error("Error connecting to peer:", err)
			} else {
				log.Info("Connected to peer:", peer.ID)
				logWithFile.Info("Connected to peer:", peer.ID)
				anyConnected = true
			}
		}
	}
	log.Info("Finished peer discovery")
	log.WithFields(logrus.Fields{
		// Close peers
		"numPeersFound": len(KADDHT.RoutingTable().GetPeerInfos()),
	}).Info("Found peers in the DHT")

	// Print time taken to find peers
	log.Info("Time taken to find peers: ", time.Since(startTime))
	logWithFile.Info("Peer discovery completed")
}

// Function to initialise the DHT and bootstrap the network with default nodes
func initialiseDHT(ctx context.Context, host host.Host) *dht.IpfsDHT {
	logWithFile.Info("Initialising the DHT...")
	kadDHT, err := dht.New(ctx, host)
	if err != nil {
		log.Fatal("Error creating DHT", err)
		logWithFile.Fatal("Error creating DHT", err)
	}

	if err := kadDHT.Bootstrap(ctx); err != nil {
		log.Fatal("Error bootstrapping DHT", err)
		logWithFile.Fatal("Error bootstrapping DHT", err)
	}

	// Connect to the default bootstrap nodes
	log.Info("Bootstrapping the DHT")

	var wg sync.WaitGroup
	for _, peerAddr := range dht.DefaultBootstrapPeers {
		peerinfo, _ := peer.AddrInfoFromP2pAddr(peerAddr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := host.Connect(ctx, *peerinfo); err != nil {
				log.Error("Error connecting to bootstrap node:", err)
				logWithFile.Error("Error connecting to bootstrap node:", err)
			}
		}()
	}
	// Wait for all connections to finish
	log.Info("Waiting for connections to bootstrap nodes to finish")
	wg.Wait()

	// Log the progress
	log.Info("Bootstrapped the DHT")
	logWithFile.Info("DHT initialised successfully")

	return kadDHT
}

// A function to handle incoming requests from clients
func handleRequest(conn network.Stream, REMOTE_ADDRESS string, ctx context.Context) {
	logWithFile.Infof("Handling request from %s", REMOTE_ADDRESS)
	defer conn.Close()

	// Create a Reader to efficiently handle input
	reader := bufio.NewReader(conn)

	for {
		// Read until newline to get complete command
		command, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading from stream:", err)
				logWithFile.Error("Error reading from stream:", err)
			}
			break
		}

		// Trim the command to remove leading/trailing whitespace and newline
		query := strings.TrimSpace(command)

		if query == "exit" {
			log.Warn("Closing connection due to 'exit' command")
			return // Exit the handleRequest function
		}

		// Define UQI
		uqi := fmt.Sprintf("%s-%d", KADDHT.Host().ID(), time.Now().UnixNano())

		// Check query cache for duplicate queries
		if _, loaded := queryCache.LoadOrStore(uqi, struct{}{}); loaded {
			log.Infof("Duplicate query received: %s (UQI: %s)", query, uqi)
			logWithFile.Infof("Duplicate query received: %s (UQI: %s)", query, uqi)
			continue // Skip duplicate query
		}

		log.Infof("Received query: %s (UQI: %s)", query, uqi)
		logWithFile.Infof("Received query: %s (UQI: %s)", query, uqi)

		// Channel to collect results from peers
		// TODO: Change size to count of peers with the IDSS protocol
		//channelSize := len(KADDHT.RoutingTable().ListPeers())
		idssPeers := 0
		for _, peer := range KADDHT.RoutingTable().ListPeers() {
			if supportsIDSSProtocol(peer) {
				idssPeers++
			}
		}
		
		channelSize := idssPeers
		resultChan := make(chan QueryResult, channelSize)

		// Execute the query locally and send the result to the channel
		go func() {
			localResult := executeLocalQuery(query, KADDHT.Host().ID())
			resultJSON, _ := json.Marshal(localResult.Rows())
			resultChan <- QueryResult{
				PeerID: KADDHT.Host().ID(),
				Result: string(resultJSON)}
			log.Infof("Local result for query %s (UQI: %s): %s", query, uqi, localResult) // for logging
			logWithFile.Infof("Local result for query %s (UQI: %s): %s", query, uqi, localResult)
		}()

		// Broadcast the query to other peers
		//broadcastQuery(ctx, query, uqi, resultChan)
		broadcastQuery(ctx, query, uqi, resultChan)

		// Wait for and merge results from all peers
		mergedResult := mergeResults(resultChan)

		// Convert the merged result to a string
		formattedResult, err := json.Marshal(mergedResult)
		if err != nil {
			log.Errorf("Error marshalling merged result: %v", err)
			continue
		}

		// Send the merged result back to the client
		if _, err := conn.Write(append([]byte(formattedResult), []byte("\n")...)); err != nil {
			log.Errorf("Error writing to client stream: %s", REMOTE_ADDRESS)
			break
		}

		// Remove query from the cache
		queryCache.Delete(uqi)

		// Process the command (remove leading/trailing whitespace and newline)
		//response := processCommand(context.Background(), strings.TrimSpace(command))
	}
}

func supportsIDSSProtocol(peer peer.ID) bool {
	supportedProtocols, err := KADDHT.Host().Peerstore().SupportsProtocols(peer, IDSS_PROTOCOL)
	if err != nil {
		log.Errorf("Error checking supported protocols for peer %v: %v", peer, err)
		return false
	}

	// Check if the peer supports the IDSS protocol
	for _, proto := range supportedProtocols {
		if proto == IDSS_PROTOCOL {
			return true
		}
	}
	return false
}

func mergeResults(resultChan <-chan QueryResult) [][]interface{} {
	logWithFile.Info("Merging results from peers...")
	var mergedResult [][]interface{} // Store rows directly

	// Keep track of unique rows seen across all results
	uniqueRows := make(map[string]bool)

	// Iterate over the results
	//TODO: Make this work with peers of the same protocol
	for result := range resultChan {
		if result.Error != nil {
			//log.Errorf("Error fetching result from peer %s: %v", result.PeerID, result.Error)
			logWithFile.Errorf("Error fetching result from peer %s: %v", result.PeerID, result.Error)
			continue
		}

		// Unmarshal the result (which is now a JSON string) into a slice of interfaces
		var rows []interface{}
		err:= json.Unmarshal([]byte(result.Result), &rows)
		if err != nil {
			log.Error("Error unmarshalling result row: ", err)
			continue
		}


		// Iterate over the rows and add unique ones to mergedResult
		for _, row := range rows {
			rawJSON, _ := json.Marshal(row)
			if _, exists := uniqueRows[string(rawJSON)]; !exists {
				uniqueRows[string(rawJSON)] = true
				mergedResult = append(mergedResult, row.([]interface{}))
			}
		}
	}

	// Convert the merged result to a string
	//return formatResult(mergedResult, KADDHT.Host().ID())
	return mergedResult
}

// Function to broadcast the query to peers in an overlay network
func broadcastQuery(ctx context.Context, query string, uqi string, resultChan chan QueryResult) {
	log.Info("Broadcasting query to peers...")
	logWithFile.Info("Broadcasting query to peers...")
	for _, peerID := range KADDHT.RoutingTable().ListPeers() {
		if peerID != KADDHT.Host().ID() && supportsIDSSProtocol(peerID) { // Skip self and filter by protocol
			go func(p peer.ID) {
				defer func() {
					if r := recover(); r != nil {
						log.Errorf("Recovered from panic: %v", r)
					}
				}()

				select{
				case <-ctx.Done():
					resultChan <- QueryResult{
						PeerID: p,
						Result: "",
						Error: ctx.Err(),
					}
				default:
					result, err := queryPeer(ctx, p, QueryMessage{Query: query, UQI: uqi, Hop: 0}, nil)
					resultChan <- QueryResult{
						PeerID: p,
						Result: result,
						Error:  err,
					}
				}
			}(peerID)
		}
	}
}

// Function to process the received command
/* func processCommand(ctx context.Context, command string, uqi string) string {
	log.Println("Processing command ")

	// Fetch results from all connected peers
	results, err := fetchResultsFromPeers(ctx, command, uqi)
	if err != nil {
		log.Errorf("Error fetching results from peers: %v", err)
	}
	// Print the results for logging
	log.Infof("Results fetched from peers: %v", results)

	// Aggregate and format the results
	resultStr := aggregateResults(results)
	return fmt.Sprintf("Command processed successfully:\n%s", resultStr)
} */

// Function to fetch results from all connected peers
/* func fetchResultsFromPeers(ctx context.Context, command string, uqi string) ([]string, []error) {
	log.Info("Fetching results from all connected peers...")
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []string
	var errors []error

	// Get connected peers
	connectedPeers := KADDHT.Host().Network().Peers()
	queryChan := make(chan peer.ID, 5) // Limit to 5 concurrent queries

	// VErify that the routing table is not nil
	number := len(KADDHT.RoutingTable().ListPeers())
	log.Info("Number of peers in the routing table: ", number)
	if KADDHT.RoutingTable() == nil {
		log.Error("Routing table is nil")
		return results, []error{fmt.Errorf("routing table is nil")}
	}

	// Iterate over all connected peers in the network
	for _, peerID := range connectedPeers {
		if !isBootstrapPeer(peerID) {// Skip bootstrap peers
			supportedProtocols, err :=  KADDHT.Host().Peerstore().SupportsProtocols(peerID, IDSS_PROTOCOL)
			if err != nil {
				log.Errorf("Error checking supported protocols for peer %v: %v", peerID, err)
				continue
			}

			// Check if the peer supports the IDSS protocol
			hasIDSSProtocol := false
			for _, proto := range supportedProtocols {
				if proto == IDSS_PROTOCOL {
					hasIDSSProtocol = true
					break
				}
			}

			if hasIDSSProtocol{
				//TODO: Check for appropiate implementation btn goroutine and other
				// Increment the wait group
				wg.Add(1)
				go func(p peer.ID) {
					defer wg.Done()
					queryChan <- p // Add the peer to the query channel


					resp, err := queryPeer(ctx, p, command, uqi)
					mu.Lock()
					if err != nil {
						log.Errorf("Error querying peer %v: %v", p, err)
						errors = append(errors, err) // Add the error
					}
					results = append(results, resp)
					log.Info("Results fetched: ", results)
					mu.Unlock()
				}(peerID)
			}
		}

	}
	go func() {
        wg.Wait()
        close(queryChan)
    }()

	// Process queries sequentially
    for peerID := range queryChan { // Receive peer IDs from the channel
        resp, err := queryPeer(ctx, peerID, command, uqi)
        mu.Lock()
        if err != nil {
            log.Errorf("Error querying peer %v: %v", peerID, err)
            errors = append(errors, err)
        }
        results = append(results, resp)
        log.Info("Results fetched: ", results)
        mu.Unlock()
    }

	// Print results for debugging
	log.Info("Results fetched from peers: ", results)
	return results, errors
} */

// Helper function to check if peer is a bootstrap peer
/* func isBootstrapPeer(peerID peer.ID) bool {
	for _, peerAddr := range dht.DefaultBootstrapPeers{
		peerInfo, _ := peer.AddrInfoFromP2pAddr(peerAddr)
		if peerInfo.ID == peerID{
			return true
		}
	}
	return false
} */

// Function to query a single peer
func queryPeer(ctx context.Context, peer peer.ID, msg QueryMessage, parentStream network.Stream) (string, error) {
	//log.Info("Querying peer: ", peer)
	logWithFile.Info("Querying peer: ", peer)
	// Check if KADDHT is initialised and has a host
	if KADDHT == nil || KADDHT.Host() == nil {
		// stop the execution
		return "", fmt.Errorf("DHT or its host is not initialised")
	}

	// Increment the hop count
	msg.Hop++

	// Check if the hop count exceeds the maximum
	if msg.Hop >= MAX_HOPS {
		localRes := executeLocalQuery(msg.Query, peer)
		resultJSON, _ := json.Marshal(localRes.Rows())
		return string(resultJSON), nil
	}

	// Create new stream to the peer
	stream, err := KADDHT.Host().NewStream(ctx, peer, IDSS_PROTOCOL)
	if err != nil {
		// skip to the next peer
		return "", fmt.Errorf("error opening stream to peer: %v", err)
	}
	defer stream.Close() // Always close the stream

	log.Info("Stream opened to peer: ", peer)         // For logging
	logWithFile.Info("Stream opened to peer: ", peer) // For logging

	_, err = stream.Write([]byte(fmt.Sprintf("%s:%s", msg.Query, msg.UQI))) // Send the query and UQI
	if err != nil {
		log.Errorf("Error writing to stream of %v: %v", peer, err)
		logWithFile.Errorf("Error writing to stream of %v: %v", peer, err)
		return "", err
	}

	// Check if query has been executed in this peer
	if _, loaded := queryCache.LoadOrStore(msg.UQI, struct{}{}); loaded {
		log.Infof("Duplicate query received: %s (UQI: %s)", msg.Query, msg.UQI)
		return "", fmt.Errorf("duplicate query received: %s", msg.Query)
	}

	// Run the query on the peer
	localRes := executeLocalQuery(msg.Query, peer)
	resultJSON, _ := json.Marshal(localRes.Rows())
	log.Print("Local result: ", localRes) // For logging

	// Send the local result to the peer as JSON
	err = writeQueryMessage(stream, msg, resultJSON)
	//resultJSON, err := json.Marshal(localRes.Rows()) // Marshal only the rows
	if err != nil {
		log.Errorf("Error marshalling local result: %v", err)
		return "", err
	}
	

	// Read the response as string
	reader := bufio.NewReader(stream)
	remoteResultStr, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		log.Errorf("Error reading response from %v: %v", peer, err)
		return "", err
	}

	// Send the remote result back up the chain if this is not the originating peer
	if parentStream != nil {
		if _, err = parentStream.Write([]byte(remoteResultStr)); err != nil {
			log.Errorf("Error sending remote result back to parent peer %s: %v", peer, err)
		}
	}

	logWithFile.Info("Response from peer: ", remoteResultStr)
	// Return the response
	return remoteResultStr, nil
}

// Function to write the query message to the stream
func writeQueryMessage(stream network.Stream, msg QueryMessage, localResults []byte) error {
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = stream.Write(msgJSON)
	if err != nil {
		return err
	}

	_, err = stream.Write([]byte("\n"))
	if err != nil {
		return err
	}

	_, err = stream.Write(localResults)
	if err != nil {
		return err
	}

	_, err = stream.Write([]byte("\n"))
	return err
}


// Function to aggregate results from all peers
/* func aggregateResults(results []string) string {
	log.Info("Aggregating results...")
	var sb strings.Builder
	for _, result := range results {
		sb.WriteString(fmt.Sprintf("%s\n", result))
	}

	// Print something for logging
	log.Info("Aggregated results: ", sb.String())
	// Return the aggregated results
	return sb.String()
} */

// Function to execute local query
func executeLocalQuery(command string, peer peer.ID) eql.SearchResult {
	logWithFile.Info("Executing query locally in ", peer)
	log.Printf("Executing %v in %v: ", command, peer)
	result, err := eql.RunQuery("myQuery", "main", command, GRAPH_MANAGER)
	if err != nil {
		log.Error("Error querying data: ", err)
		logWithFile.Error("Error querying data: ", err)
	} else {
		log.Info("Query processed locally in ", peer)
		logWithFile.Info("Query processed locally in ", peer)
	}
	return result
}

// Function to initialise the database instance
func initDBIntance(peer peer.AddrInfo, filename string) {
	log.Info("Initialising the database instance...")
	// Create a peer-specific database directory
	dbPath := fmt.Sprintf("%s/%s", DB_PATH, peer.ID.String())
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		os.MkdirAll(dbPath, 0755)
	}

	graphDB, err := graphstorage.NewDiskGraphStorage(dbPath, false)
	CheckError(err, "Error creating graph database")
	defer graphDB.Close()
	GRAPH_MANAGER = eliasdb.NewGraphManager(graphDB)

	// Initialize the database only if the filename is provided
	if filename != "" {
		Database_init(filename, dbPath)
	}
	log.Printf("Graph DB instance created at peer: %s (path: %s)", peer.ID, dbPath)
	logWithFile.Info("Graph DB instance created successfully")
}

// Function to format the result in a string format in each peer
/* func formatResult(result eql.SearchResult, peer peer.ID) string {
	log.Info("Formatting query result in ", peer)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Results from peer %s:\n", peer))

	// Check if the result set is empty
    if len(result.Rows()) == 0 {
        sb.WriteString("No results found.\n")
        return sb.String()
    }


	// Iterate over each row and format it
	for _, row := range result.Rows() {
		formattedRow := make([]string, len(row))
        for i, value := range row {
            formattedRow[i] = fmt.Sprintf("%v", value) // Basic formatting, adjust as needed
        }
        sb.WriteString(strings.Join(formattedRow, "\t") + "\n") // Join values with tabs
	}
	return sb.String()
} */

/* {
// Add support for Graph Database using EliasDB
	//TODO: Cant all these be defined with Database_init()?
	GRAPH_DB, err := graphstorage.NewDiskGraphStorage(DB_PATH, false)
	CheckError(err, "Error creating graph database")
	defer GRAPH_DB.Close()
	GRAPH_MANAGER := graph.NewGraphManager(GRAPH_DB)
	log.Printf("Processing command: %s", command)

	// Trim client address to get only the port number

	//clientAddress := conn.RemoteAddr().String()
	clientAddress := conn.Conn().RemoteMultiaddr().String()
	addressParts := strings.Split(clientAddress, ":")
	clientPort := addressParts[len(addressParts)-1]

	timestamp := time.Now().UnixNano()
	unid := fmt.Sprintf("%d%s", timestamp, clientPort)
	log.Println("Unique query ID: ", unid)

	/********************************

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

	**********************************/
/*
	result, err := eql.RunQuery("myQuery", "main", command, GRAPH_MANAGER)
	if err != nil {
		log.Println("Error querying data: ", err)
		// This error will be printed on the client side sometimes because of quoting issues in the query.
		// It is recommended to use double quotes for the query
		errMsg := "Error querying data: " + err.Error()
		_, writeErr := conn.Write([]byte(errMsg))
		CheckError(writeErr)
		conn.Close()
		return
	}

	query_mng := &QueryManager{
		Uqid: unid, // Generated unique ID based on the client port number and the current time
		Query: command,
		Ttl: 100,
		ArrivedAt: timestamppb.New(time.Now()),
		SenderId: clientAddress, // Client address coming from the established connection
		LocalExecution: true, //TODO: It should be true at this stage
		Completed: true,
		SentBack: false, //TODO: To be updated after sending the result back to other peer (After libp2p implementation)
		Failed: false,
	}

	// Marshal the query manager data and store it
	query_data, err := proto.Marshal(query_mng)
	CheckError(err)

	// Unmarshall and Print query manager details
	query_mng2 := &QueryManager{}
	err = proto.Unmarshal(query_data, query_mng2)
	CheckError(err)
	log.Println("Unmarshalled query manager data: ", query_mng2)

	// Check if the result is empty
	if len(result.Rows()) == 0 {
		_, err := conn.Write([]byte("No result found\n"))
		if err != nil {
			log.Println("Error sending data to client: ", err)
		}
		conn.Close()
		return
	}

	// Print the result
	// Results can be printed in any format as per the requirement
	log.Printf("Query result: %v", result)
	_, err = conn.Write([]byte("Command processed successfully\n"))
	CheckError(err)
} */

// Function to check for errors
func CheckError(err error, message ...string) {
	if err != nil {
		log.Fatalf("Error during operation '%s': %v", message, err)
	}
}
