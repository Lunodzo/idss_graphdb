/*
@2023, University of Salento, Italy

*/

package main

import (
	"bufio"
	"context"
	"net/http"
	_ "net/http/pprof"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"encoding/binary"
	"encoding/json"
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
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/util"
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
	DB_PATH = "../server/idss_graph_db" // Path to the graph database
)

var (
	activeConnections int64
	logger            = log.Logger("IDSS")
	msg common.QueryMessage 
)

type AggregateInfo struct {
    Function    string
    Traversal   string
    Filter      string
    Comparison  string
    Attribute   string
    Threshold   float64
}

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

		Graph database operations using EliasDB. This must be affected in all the joining peers for efficient querying.
		Calling functions from idss_db_init.go to create the database nodes and edges. Note that, this process 
		involves generating random data using generate_data.py. This python script is used to generate 
		random data for the graph database. The data is stored in a JSON file which is then used to create 
		the nodes and edges in the graph database.

		Before generating the data, the graph database env must be created. This is done by creating a path, 
		storage instance and graph manager. This code already handles the process.

	************************/

	dbPath := fmt.Sprintf("%s/%s", DB_PATH, host.ID().String()) // Peer specific database path

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

	if err := GenFakeDataAndInit(config.Filename, dbPath, graphDB, graphManager); err != nil {
		logger.Fatalf("Error initializing database: %v", err)
	}
	logger.Info("Data loaded into the graph database")

	// Initialise the query manager
	QueryManager_init(graphManager)

	// Initialise the DHT and bootstrap the peer
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

// Function to clean up the graph database and the peer directory.
// This must be handled differently in production settings
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
	startTime := time.Now() //for debugging
	logger.Info("Starting peer discovery...")

	// Provide a value for the given key. Where a key in this case is the service name (config.IDSSString)
	routingDiscovery := routing.NewRoutingDiscovery(kadDHT)
	util.Advertise(ctx, routingDiscovery, config.IDSSString)
	logger.Debug("Announced the IDSS service")

	// Look for other peers in the network who announced and connect to them
	anyConnected := false
	logger.Infoln("Searching for other peers...")
	fmt.Println("...")

	for !anyConnected {
		peerChan, err := routingDiscovery.FindPeers(ctx, config.IDSSString)
		if err != nil {
			logger.Errorf("Error finding peers: %v", err)
			time.Sleep(2 * time.Second) // Retry after 2 seconds
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
			//logger.Info("Connected to bootstrap peer: ", p.ID) // For debugging
		}(peerInfo)
	}
	wg.Wait() 
	return kadDHT
}

// A function to handle incoming requests from peers.
func handleRequest(host host.Host, conn network.Stream, remotePeerID string, ctx context.Context, config Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) {
	//logger.Infof("Received incoming from %s", remotePeerID)
	defer conn.Close()

	// For any connected peer, add to the routing table
	host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(net network.Network, conn network.Conn) {
			kadDHT.RoutingTable().PeerAdded(conn.RemotePeer()) // Add to routing table
		},
	})

	// Read the incoming message
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

		// This aims to handle only query messages. Other message types can be added and handled accordingly
		// The client will send a query message to the server
		if msg.Type == common.MessageType_QUERY{
			handleQuery(conn, &msg, remotePeerID, config, gm, kadDHT)
		}
	}
}

// Function to process the query message received from the client or intermediate peers
func handleQuery(conn network.Stream, msg *common.QueryMessage, remotePeerID string, config Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) {
	//logger.Infof("Query received:\nOn peer %s \nUQI: %s\nTTL: %f \nFrom: %s", kadDHT.Host().ID(), msg.Uqid, msg.Ttl, remotePeerID) // for debugging

	duplicateQuery, err := checkDuplicateQuery(msg.Uqid, gm) // with gm, we check queries in the graph database for this peer
	if err != nil {
		logger.Errorf("Error checking duplicate query: %v", err)
		return
	}

	if len(duplicateQuery) > 0 {
		return
	}
	logger.Infof("This is a new query on this peer")
	logger.Infof("\nReceiver %s \nUQI: %s\nTTL: %f \nFrom: %s", kadDHT.Host().ID(), msg.Uqid, msg.Ttl, remotePeerID) // for debugging
	msg.State = &common.QueryState{State: common.QueryState_QUEUED} // Set the query state to QUEUED
	storeQueryInfo(msg, gm, remotePeerID) // Store the query info in the graph database

	// Parse the query string to extract the aggregates
	aggregates := parseAggregates(msg.Query)
    if len(aggregates) > 0 {
        handleAggregateQuery(conn, msg, config, gm, kadDHT, aggregates[0])
        return
    }

	// Execute and broadcast the query
	executeAndBroadcastQuery(conn, msg, config, gm, kadDHT)
}

// Function to parse the query string and extract the aggregates
func parseAggregates(query string) []AggregateInfo {
    var aggregates []AggregateInfo
    re := regexp.MustCompile(
        `@(sum|avg|max|min)\(([^\)]+)\)\s*(>|<|>=|<=|==|!=)\s*([\d\.]+)`,
    )

    matches := re.FindAllStringSubmatch(query, -1)
    for _, m := range matches {
        if len(m) < 5 {
            continue
        }

        parts := strings.SplitN(m[2], " where ", 2)
        traversal := parts[0]
        filter := ""
        if len(parts) > 1 {
            filter = parts[1]
        }

        threshold, _ := strconv.ParseFloat(m[4], 64)
        aggregates = append(aggregates, AggregateInfo{
            Function:   m[1],
            Traversal:  traversal,
            Filter:     filter,
            Comparison: m[3],
            Attribute:  "measurement", // Adjust based on your schema
            Threshold:  threshold,
        })
    }
    return aggregates
}

// Function to handle aggregate queries
func handleAggregateQuery(conn network.Stream, msg *common.QueryMessage, config Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, agg AggregateInfo) {
    // Execute local aggregate
	localSum, localCount, err := runAggregatedQuery(agg, gm, kadDHT)
    if err != nil {
        logger.Errorf("Aggregate query failed: %v", err)
        return
    }

    // Broadcast and collect remote results
    remoteResults := broadcastAggregateQuery(msg, conn, config, gm, kadDHT)
    
    // Merge results
    finalValue := mergeAggregateResults(agg, localSum, localCount, remoteResults)
    
    // Apply comparison
    if !applyAggregateCondition(finalValue, agg.Comparison, agg.Threshold) {
        logger.Info("Aggregate condition not met")
        return
    }

    // Prepare and send final result
    finalRows := [][]interface{}{{finalValue}}
    sendMergedResult(conn, conn.Conn().RemotePeer(), finalRows, kadDHT)
}

func runAggregatedQuery(agg AggregateInfo, gm *graph.Manager, kadDHT *dht.IpfsDHT) (sum float64, count float64, err error) {
    // Transform query based on aggregate type
    baseQuery := fmt.Sprintf("get %s traverse %s where %s",
        strings.Split(agg.Traversal, ":")[0],
        agg.Traversal,
        agg.Filter,
    )

    switch agg.Function {
    case "sum":
		res, _, err := runQuery(baseQuery+" show sum("+agg.Attribute+")", kadDHT.Host().ID(), gm)
        if err != nil || len(res) == 0 {
            return 0, 0, err
        }
        return res[0][0].(float64), 0, nil

    case "avg":
        sumRes, _, err := runQuery(baseQuery+" show sum("+agg.Attribute+")", kadDHT.Host().ID(), gm)
        if err != nil || len(sumRes) == 0 {
            return 0, 0, err
        }
        countRes, _, err := runQuery(baseQuery+" show count()", kadDHT.Host().ID(), gm)
        if err != nil || len(countRes) == 0 {
            return 0, 0, err
        }
        return sumRes[0][0].(float64), countRes[0][0].(float64), nil

    case "max":
        res, _, err := runQuery(baseQuery+" show max("+agg.Attribute+")", kadDHT.Host().ID(), gm)
        if err != nil || len(res) == 0 {
            return 0, 0, err
        }
        return res[0][0].(float64), 0, nil

    case "min":
        res, _, err := runQuery(baseQuery+" show min("+agg.Attribute+")", kadDHT.Host().ID(), gm)
        if err != nil || len(res) == 0 {
            return 0, 0, err
        }
        return res[0][0].(float64), 0, nil

    default:
        return 0, 0, fmt.Errorf("unsupported aggregate function")
    }
}

// Function to broadcast the aggregate query to connected peers
func broadcastAggregateQuery(msg *common.QueryMessage, parentStream network.Stream, config Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) [][2]float64 {
    var remoteResults [][2]float64
    var mu sync.Mutex
    var wg sync.WaitGroup

    eligiblePeers := getEligiblePeers(kadDHT, parentStream)
    
    for _, peerID := range eligiblePeers {
        wg.Add(1)
        go func(p peer.ID) {
            defer wg.Done()
            
            // Stream creation
			streamCtx, streamCancel := context.WithTimeout(context.Background(), time.Duration(msg.Ttl)) // Stream life span is the TTL
			defer streamCancel()

			stream, err := kadDHT.Host().NewStream(streamCtx, p, protocol.ID(config.ProtocolID))
			if err != nil {
				return
			}
			defer stream.Close()

			// Update TTL in the graph database and the message
			if err := updateTTL(msg, gm); err != nil {
				logger.Errorf("Error updating TTL: %v", err)
			}
			logger.Infof("Broadcasting query with TTL: %f", msg.Ttl)
            
            // Send query and receive response
            sum, count, err := receiveAggregateResponse(stream)
            if err != nil {
                return
            }

            mu.Lock()
            remoteResults = append(remoteResults, [2]float64{sum, count})
            mu.Unlock()
        }(peerID)
    }
    
    wg.Wait()
    return remoteResults
}

func receiveAggregateResponse(stream network.Stream) (float64, float64, error) {
    defer stream.Close()

    // Read response message
    msgBytes, err := readDelimitedMessage(stream, context.Background())
    if err != nil {
        return 0, 0, fmt.Errorf("error reading response: %v", err)
    }

    // Unmarshal response into QueryMessage
    var response common.QueryMessage
    err = proto.Unmarshal(msgBytes, &response)
    if err != nil {
        return 0, 0, fmt.Errorf("error unmarshalling response: %v", err)
    }

    // Ensure the received message is of type RESULT
    if response.Type != common.MessageType_RESULT {
        return 0, 0, fmt.Errorf("unexpected message type: %v", response.Type)
    }

    // Extract sum and count values from the response
    if len(response.Result) < 1 || len(response.Result[0].Data) < 1 {
        return 0, 0, fmt.Errorf("received empty aggregate result")
    }

    var sum, count float64
    sum, err = strconv.ParseFloat(response.Result[0].Data[0], 64)
    if err != nil {
        return 0, 0, fmt.Errorf("error parsing sum: %v", err)
    }

    // Check if the response includes a count value (needed for AVG queries)
    if len(response.Result[0].Data) > 1 {
        count, err = strconv.ParseFloat(response.Result[0].Data[1], 64)
        if err != nil {
            return 0, 0, fmt.Errorf("error parsing count: %v", err)
        }
    }

    return sum, count, nil
}


func getEligiblePeers(kadDHT *dht.IpfsDHT, parentStream network.Stream) []peer.ID {
    var eligible []peer.ID
    for _, p := range kadDHT.RoutingTable().ListPeers() {
        if p != kadDHT.Host().ID() && 
            (parentStream == nil || p != parentStream.Conn().RemotePeer()) {
            eligible = append(eligible, p)
        }
    }
    return eligible
}

// Function to execute the query and broadcast it to connected peers (peers in an overlay network)
func executeAndBroadcastQuery(conn network.Stream, msg *common.QueryMessage, config Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) {
	// Get query details from the graph database
	queryDetails, err := fetchQueryDetails(msg.Uqid, gm)
	if err != nil {
		logger.Errorf("Error fetching query details: %v", err)
		return
	}

	// Use the details
	logger.Infof("Query details from graph database: %+v\n", queryDetails)

	if query_string, ok := queryDetails["Query String"].(string); ok {
		msg.Query = query_string
	} else {
		if v, exists := queryDetails["Query String"]; exists { // verify if we are fetching the right attribute
			logger.Warnf("Query string for UQI %s is not a string: %v, Type: %T", msg.Uqid, v, v)
		} else {
			logger.Warn("Query not found or it is not a string ", err)
		}
		return
	}

	if originator, ok := queryDetails["Originator"]; ok {
		msg.Originator = originator.(string)
	} else {
		logger.Warn("Originator not found or it is not a string")
		return
	}

	if senderAddress, ok := queryDetails["Sender Address"]; ok {
		msg.Sender = senderAddress.(string)
	} else {
		logger.Warn("Sender address not found or it is not a string")
		msg.Sender = kadDHT.Host().ID().String() // Set the sender address to the current peer
	}

	if arrivalTime, ok := queryDetails["Arrival Time"]; ok {
		msg.Timestamp = arrivalTime.(string)
	} else {
		logger.Warn("Arrival time not found or it is not a string")
		return
	}
	
	msg.Result = nil // Clear the result
	msg.Uqid = queryDetails["Query Key"].(string)


	logger.Infof("Query UQI: %s, TTL: %f", msg.Uqid, msg.Ttl) // for debugging
	var wg sync.WaitGroup // Wait group to ensure all operations are completed before closing the stream
	var localResHolder [][]interface{} // To hold the local results
	startTime := time.Now() // for debugging duration of query execution

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
		//msg.State = &common.QueryState{State: common.QueryState_LOCALLY_EXECUTED}
		updateQueryState(msg, common.QueryState_LOCALLY_EXECUTED, gm)
		//logger.Info("Local query executed successfully")
	}()

	wg.Wait() // proceed to convert results after local query is done
	localResults := covertResultToProtobufRows(localResHolder, nil)
	//msg.Result = localResults

	// Store the local results in the graph database
	storeResults(msg, localResHolder, gm)

	// Incase the TTL has expired, send available results to the parent peer 
	if msg.Ttl <= 0 {
		logger.Warn("TTL expired, not broadcasting query")
		updateQueryState(msg, common.QueryState_SENT_BACK, gm)
		sendMergedResult(conn, conn.Conn().RemotePeer(), convertProtobufRowsToResult(localResults), kadDHT)
		return
	}

	// Now update sender address to the current peer graph database
	err = updateQuerySenderAddress(msg, kadDHT.Host().ID().String(), gm)
	if err != nil {
		logger.Errorf("Error updating sender address: %v", err)
	}
	msg.Sender = kadDHT.Host().ID().String() // So that the overlay peers identify this as parent peer

	// Run broadcastquery in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		broadcastQuery(msg, conn, config, localResHolder, gm, kadDHT)
	}()

	// Wait for all goroutines to complete
	go func(){
		wg.Wait()
		logger.Info("All remote results collected in originating peer")
		logger.Infof("Time taken to execute query: %v", time.Since(startTime)) // for debugging
		// close the stream
		if err := conn.Close(); err != nil {
			logger.Errorf("Error closing stream: %v", err)
		}
	}()
}

// Support for WITH operations
func parseWithClauses(query string) []string {
    re := regexp.MustCompile(`(?i)\bwith\s+([^;]*)`)
    matches := re.FindStringSubmatch(query)
    if len(matches) < 2 {
        return nil
    }
    return strings.Split(matches[1], ",")
}


func applyWithClauses(results [][]interface{}, clauses []string) [][]interface{} {
    for _, clause := range clauses {
        clause = strings.TrimSpace(clause)
        if strings.HasPrefix(clause, "ordering(") {
            applyOrdering(results, clause)
        }
    }
    return results
}

func applyOrdering(results [][]interface{}, clause string) {
    if len(results) < 2 {
        return
    }

    re := regexp.MustCompile(`ordering\((ascending|descending)\s+([^)]+)`)
    matches := re.FindStringSubmatch(clause)
    if len(matches) < 3 {
        return
    }

	orderType := matches[1]
    colName := matches[2]
    //header := results[0]
    var colIndex int = -1

	// Assume the first row is the header
	headers := results[0]
	for i, h := range headers {
		if fmt.Sprintf("%v", h) == colName {
			colIndex = i
			break
		}
	}
	
    
    // If the header is missing assume column index is provided in the query
    if colIndex == -1 {
        logger.Warnf("Column %s not found, defaulting to index 2", colName)
        colIndex = 2 // Default to Name column. For debugging, this can be changed to 0
    }

	// Sort the results based on the identified column 
    sort.Slice(results[1:], func(i, j int) bool {
		a, okA := results[i+1][colIndex].(string)
        b, okB := results[j+1][colIndex].(string)

        if !okA || !okB {
            return false
        }

        if orderType == "ascending" {
            return a < b
        }
        return a > b
    })
}

func updateQuerySenderAddress(s1 *common.QueryMessage, s2 string, gm *graph.Manager) error {
	trans := graph.NewGraphTrans(gm)
	queryNode := data.NewGraphNode()
	queryNode.SetAttr("key", s1.Uqid)
	queryNode.SetAttr("kind", "Query")
	queryNode.SetAttr("sender_address", s2)

	// Update only the sender address attribute of existing query node
	if err := trans.UpdateNode("main", queryNode); err != nil { // This is ECAL in EliasDB
		logger.Errorf("Error updating sender address: %v", err)
		return err
	}

	if err := trans.Commit(); err != nil {
		logger.Errorf("Error committing transaction: %v", err)
		return err
	}

	logger.Infof("Sender address updated to: %s", s2)
	return nil
}

func fetchQueryDetails(s string, gm *graph.Manager) (map[string]interface{}, error) {
	queryStatement := fmt.Sprintf("get Query where key = '%s'", s)
	results, err := eql.RunQuery("fetchQueryDetails", "main", queryStatement, gm)
	if err != nil {
		logger.Errorf("Error fetching query details: %v", err)
		return nil, err
	}

	// Check if the query details are not empty
	if len(results.Rows()) == 0 {
		logger.Warn("Query details not found")
		return nil, fmt.Errorf("no query details found for UQI: %s", s)
	}

	queryDetails := make(map[string]interface{})
	for i, v := range results.Rows()[0] {
		attributeName := results.Header().Labels()[i]
		queryDetails[attributeName] = v
	}

	return queryDetails, nil
}

// Checks if the query is already in the graph database for the peer
func checkDuplicateQuery(uqi string, gm *graph.Manager) ([][]interface{}, error) {
	statement := fmt.Sprintf("get Query where key = '%s'", uqi)

	checkQuery, err := eql.RunQuery("checkQuery", "main", statement, gm)
	if err != nil {
		return nil, err
	}
	return checkQuery.Rows(), nil
}

// Function to store query information in the graph database using msg contents
func storeQueryInfo(msg *common.QueryMessage, graphManager *graph.Manager, remotePeerID string) {
	// Create a new transaction
	trans := graph.NewGraphTrans(graphManager)
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
	resultJSON, err := json.Marshal(msg.Result)
	if err != nil {
		logger.Errorf("Error marshalling result: %v", err)
		return
	}
	queryNode.SetAttr("result", string(resultJSON))

	// Store the query node in the graph database
	trans.StoreNode("main", queryNode)

	// Commit the transaction
	if err := trans.Commit(); err != nil {
		logger.Errorf("Error committing transaction: %v", err)
		return
	}
}

// Function to update the query state in the graph database
func updateQueryState(msg *common.QueryMessage, state common.QueryState_State, gm *graph.Manager) {
	trans := graph.NewGraphTrans(gm)
	queryNode := data.NewGraphNode()
	queryNode.SetAttr("key", msg.Uqid)
	queryNode.SetAttr("state", state.String())
	// Add others
	trans.UpdateNode("main", queryNode)
	if err := trans.Commit(); err != nil {
		logger.Errorf("Error updating query state: %v", err)
	}
}

// Function to store the results in the graph database in each peer. 
// This creates a new node for the results to separate them from the query node
func storeResults(msg *common.QueryMessage, results [][]interface{}, gm *graph.Manager) {
	// Convert results into JSON
	jsonResults, err := json.Marshal(results)
	if err != nil {
		logger.Errorf("Error marshalling results: %v", err)
		return
	}

	trans := graph.NewGraphTrans(gm)
	resultsNode := data.NewGraphNode()
	resultsNode.SetAttr("key", fmt.Sprintf("%s_results", msg.Uqid))
	resultsNode.SetAttr("kind", "Results")
	resultsNode.SetAttr("name", "Results")
	resultsNode.SetAttr("query_key", msg.Uqid)
	resultsNode.SetAttr("results", string(jsonResults))
	trans.StoreNode("main", resultsNode)
	if err := trans.Commit(); err != nil {
		logger.Errorf("Failed to store results for %s: %v", msg.Uqid, err)
	}
}

// IDSS function to broadcast the query to connected peers. This function also filters out the originating and parent peers because they are already queried
func broadcastQuery(msg *common.QueryMessage, parentStream network.Stream, config Config, localResults [][]interface{}, gm *graph.Manager, kadDHT *dht.IpfsDHT) {

	logger.Infof("Checking if to continue broadcasting query: %s", msg.Uqid)
	if shouldContinueBroadcastingQuery(msg, gm) {
		var mergedResults [][]interface{} // To hold the merged results from all peers
		targetProtocol := protocol.ID(config.ProtocolID) // Protocol ID for the stream
		connectedPeers := kadDHT.Host().Network().Peers() // Connected peers are the peers in the routing table
		var eligiblePeers []peer.ID

		// Filter peers to exclude the originating peer and the parent stream peer. Because we dont want to query them again
		// Ensure that we don't query self, the parent stream peer or the peer that sent the query
		for _, peerID := range connectedPeers {
			if 	peerID != kadDHT.Host().ID() && 
				(parentStream == nil || parentStream.Conn().RemotePeer() != peerID) {
				// Check if the peer is in the closest peers list then add to eligible peers
				eligiblePeers = append(eligiblePeers, peerID) // Check if there is an active connection to the peer
			}
		}

		if len(eligiblePeers) > 0 {
			logger.Infof("Broadcasting query to %d peers", len(eligiblePeers))
		} else {
			logger.Warn("No eligible peers to broadcast to")
			return
		}

		// Prepare for concurrent broadcasting. We have to use concurrency to run the query 
		// on all eligible peers without waiting for one to complete
		var localWg sync.WaitGroup
		remoteResultsChan := make(chan [][]interface{}, len(eligiblePeers)) // enough to accomodate involved peers
		duration := time.Duration(msg.Ttl) * time.Second // Duration follows the TTL
		responseTimeout := time.After(duration)

		// Broadcast the query, and wait for results from the peers that have received a query from you
		for _, peerID := range eligiblePeers {
			localWg.Add(1)
			go func(p peer.ID){ // Separate goroutine for each peer
				defer localWg.Done()

				
				streamCtx, streamCancel := context.WithTimeout(context.Background(), duration) // Stream life span is the TTL
				defer streamCancel()

				stream, err := kadDHT.Host().NewStream(streamCtx, p, targetProtocol)
				if err != nil {
					return
				}
				defer stream.Close()

				// Update TTL in the graph database and the message
				if err := updateTTL(msg, gm); err != nil {
					logger.Errorf("Error updating TTL: %v", err)
				}
				logger.Infof("Broadcasting query with TTL: %f", msg.Ttl)

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

				logger.Infof("Query sent to peer %s, awaiting for response", p)

				select{
					case <-responseTimeout: // If the response times out
						logger.Warnf("Response timeout from peer %s", p)
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
							logger.Warnf("Error reading remote results from peer %s, %v", p, err)
							return
						}

						logger.Infof("Received %d records from peer %s", len(resultsByte), p)

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

		// Waits for all goroutines (all eligible peers) to complete. 
		// To ensure that we do not bock indefinitely, TTL and timeout are used
		go func(){
			localWg.Wait() 
			close(remoteResultsChan)
			logger.Infof("All remote results collected, now merging remote with local results")
		}()

		for {
			select {
				case result, ok := <-remoteResultsChan:
					if !ok {
						logger.Warn("No more remote results")
						mergedResults = append(mergedResults, result...)
						goto MergedResults
					}
					logger.Infof("Merging %d results", len(result))
					mergedResults = append(mergedResults, result...)
				case <-responseTimeout:
					logger.Warn("Timeout merging results, stepping to next phase")
					goto MergedResults
			}
		}
		MergedResults:
		mergedResults = mergeTwoResults(localResults, mergedResults) // Merge local and remote results

		parentPeerID := parentStream.Conn().RemotePeer() // Parent peer ID

		// get a complete host multiaddress
		peerAddr := kadDHT.Host().Addrs()[0].Encapsulate(multiaddr.StringCast("/p2p/" + kadDHT.Host().ID().String()))
		
		// Check if current peer is an originator or an intermediate peer
		logger.Infof("Comparing originator %s with current peer %s", msg.Originator, peerAddr)
		if msg.Originator != peerAddr.String() {
			// This is an intermediate peer
			insideWg := sync.WaitGroup{}
			insideWg.Add(1)
			go func() {
				defer insideWg.Done()

				// Update the query state to sent back
				msg.State = &common.QueryState{State: common.QueryState_SENT_BACK}
				updateQueryState(msg, common.QueryState_SENT_BACK, gm)
			}()

			insideWg.Add(1)
			go func() {
				defer insideWg.Done()
				logger.Infof("This is an intermediate peer %s. Sending results to parent peer %s", kadDHT.Host().ID(), parentPeerID)
				sendMergedResult(parentStream, parentPeerID, mergedResults, kadDHT) 
			}()

			insideWg.Wait()
		}else{
			//TODO: Process aggregate functions here
			logger.Infof("This is the originator peer %s. Sending results to client", kadDHT.Host().ID())
			// Fetch TTL from the graph database
			queryDetails, err := fetchQueryDetails(msg.Uqid, gm)
			if err != nil {
				logger.Errorf("Error fetching query details: %v", err)
			}

			// Also get client peer ID from the graph database
			if clientPeerID, ok := queryDetails["Sender Address"].(string); ok {
				msg.Sender = clientPeerID
			} else {
				logger.Warn("Client peer ID not found or it is not a string")
			}

			// merge the local results with the merged results
			mergedResults = mergeTwoResults(localResults, mergedResults)

			withClauses := parseWithClauses(msg.Query)
			if len(withClauses) > 0 {
				mergedResults = applyWithClauses(mergedResults, withClauses)
			}

			// Update the query state to completed
			msg.State = &common.QueryState{State: common.QueryState_COMPLETED}
			updateQueryState(msg, common.QueryState_COMPLETED, gm)

			// Send the merged results to the client
			clientPeerID, err := peer.Decode(msg.Sender)
			if err != nil {
				logger.Errorf("Error decoding client peer ID: %v", err)
				return
			}

			sendMergedResult(parentStream, clientPeerID, mergedResults, kadDHT)
		}
	}else{
		logger.Infof("Query %s will not be broadcast further due to state or TTL", msg.Uqid)
        if msg.State.State == common.QueryState_SENT_BACK {
            logger.Info("Sending query results back...")
            parentPeerID := parentStream.Conn().RemotePeer()
            sendMergedResult(parentStream, parentPeerID, localResults, kadDHT) // Send back local results if they weren't broadcast
        }
		return
	}
}

// Function to decide on to continue or stop broadcasting the query
func shouldContinueBroadcastingQuery(msg *common.QueryMessage, gm *graph.Manager) bool {
	queryInfo, err := checkDuplicateQuery(msg.Uqid, gm)
	if err != nil || len(queryInfo) == 0 {
		return true // Continue broadcasting
	}

	stateStr := fmt.Sprintf("%v", queryInfo[0][7]) // Assert type to string
	state, ok := common.QueryState_State_value[stateStr]
	if !ok {
		logger.Warnf("Unknown state %s for query %s", stateStr, msg.Uqid)
		return false
	}

	// Fetch TTL from the graph database
	queryDetails, err := fetchQueryDetails(msg.Uqid, gm)
	if err != nil {
		logger.Errorf("Error fetching query details: %v", err)
		return false
	}

	if ttl, ok := queryDetails["Ttl"].(float32); ok {
		msg.Ttl = ttl
	} else {
		logger.Warn("TTL not found or it is not a float")
		return false
	}

	return 	state != int32(common.QueryState_COMPLETED) && 
			state != int32(common.QueryState_SENT_BACK) && 
			msg.Ttl > 0 // Continue broadcasting
}

// Function to update the TTL in the graph database
func updateTTL(msg *common.QueryMessage, gm *graph.Manager) error {
	newTTL := msg.Ttl * 0.75 // Reduce the TTL by 25%
	trans := graph.NewGraphTrans(gm)
	queryNode := data.NewGraphNode()
	queryNode.SetAttr("key", msg.Uqid)
	queryNode.SetAttr("kind", "Query")
	queryNode.SetAttr("ttl", float64(newTTL)) 

	// Update only the TTL attribute of existing query node
	if err := trans.UpdateNode("main", queryNode); err != nil {
		logger.Errorf("Error updating TTL: %v", err)
		return err
	}

	if err := trans.Commit(); err != nil {
		logger.Errorf("Error committing transaction: %v", err)
		return err
	}

	msg.Ttl = float32(newTTL)

	logger.Infof("TTL updated to: %f", msg.Ttl) 
	return nil
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

// Function to merge aggregate results from local and remote peers
func mergeAggregateResults(agg AggregateInfo, localSum, localCount float64, remoteResults [][2]float64) float64 {
    totalSum := localSum
    totalCount := localCount

    for _, res := range remoteResults {
        totalSum += res[0]
        totalCount += res[1]
    }

    switch agg.Function {
    case "sum":
        return totalSum
    case "avg":
        if totalCount == 0 {
            return 0
        }
        return totalSum / totalCount
    case "max":
        maxVal := localSum
        for _, res := range remoteResults {
            if res[0] > maxVal {
                maxVal = res[0]
            }
        }
        return maxVal
    case "min":
        minVal := localSum
        for _, res := range remoteResults {
            if res[0] < minVal {
                minVal = res[0]
            }
        }
        return minVal
    default:
        return 0
    }
}

func applyAggregateCondition(value float64, operator string, threshold float64) bool {
    switch operator {
    case ">": return value > threshold
    case "<": return value < threshold
    case ">=": return value >= threshold
    case "<=": return value <= threshold
    case "==": return value == threshold
    case "!=": return value != threshold
    default: return false
    }
}

func mergeTwoResults(result1, result2 [][]interface{}) [][]interface{} {
	// Use a map to track unique entries
	estimatedSize := len(result1) + len(result2)
	uniqueRecords := make(map[string]bool, estimatedSize)
	mergedResults := make([][]interface{}, 0, estimatedSize)

	// Ensure headers are preserved from only one source
    if len(result1) > 0 && len(result2) > 0 && fmt.Sprintf("%v", result1[0]) == fmt.Sprintf("%v", result2[0]) {
        result2 = result2[1:] // Remove duplicate header from result2
    }


	// Merge both result sets while keeping only unique records
    for _, record := range append(result1, result2...) {
        key := fmt.Sprintf("%v", record)
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
    }else if len(result) > 0 { //if header is missing, infer it
		inferredHeader := []string{"ID", "Value", "Name", "Score"} // for debugging
		rows = append(rows, &common.Row{Data: inferredHeader})
	}

	// convert result rows
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
func runQuery(command string, peer peer.ID, gm *graph.Manager) ([][]interface{}, eql.SearchResultHeader, error) {
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