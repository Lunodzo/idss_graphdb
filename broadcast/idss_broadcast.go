/*
 * ! \file idss_broadcast.go
 * This file contains functions to handle and broadcast queries in the IDSS system.
 * It also contains functions to handle TTL, update query state, and store query results
 * in the graph database.
 *
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 *
 */
package broadcast

import (
	"context"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"

	"encoding/json"
	"fmt"
	"idss/graphdb/common"
	"idss/graphdb/flags"
	"idss/graphdb/helpers"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/ipfs/go-log/v2"

	"github.com/krotik/eliasdb/eql"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/multiformats/go-multiaddr"
	//"github.com/multiformats/go-multihash/register/all"
	"google.golang.org/protobuf/proto"
)
var logger = log.Logger("IDSS")
//var header []string

// Function to handle and broadcast queries in the IDSS system
func ExecuteAndBroadcastQuery(conn network.Stream, msg *common.QueryMessage, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) {
	// Get query details from the graph database
	queryDetails, err := FetchQueryDetails(msg.Uqid, gm)
	if err != nil {
		logger.Errorf("Error fetching query details: %v", err)
		return
	}

	// Use the details
	logger.Infof("Processing traditional Query: %+v\n", queryDetails)

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
	var header []string

	// Run local query 
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, header, err := RunIDSSQuery(msg.Query, kadDHT.Host().ID(), gm)
		if err != nil {
			logger.Errorf("Error executing local query: %v", err)
			return
		}

		// Remove any prefixes from header labels (e.g., "Client:name" -> "name")
		for i, h := range header {
			parts := strings.FieldsFunc(h, func(r rune) bool { return r == ':' || r == ' ' })
			if len(parts) > 0 {
				header[i] = parts[len(parts)-1]
			}
		}
		
		localResHolder = result
		UpdateQueryState(msg, common.QueryState_LOCALLY_EXECUTED, gm)
		StoreResults(msg, localResHolder, gm)
	}()

	// Incase the TTL has expired, send available results to the parent peer 
	if msg.Ttl <= 0 { // This will only be reached at intermediate peers
		logger.Warn("TTL expired, not broadcasting query")
		UpdateQueryState(msg, common.QueryState_SENT_BACK, gm)
		wg.Wait() //
		helpers.SendMergedResult(conn, conn.Conn().RemotePeer(), localResHolder, header, kadDHT)
		return
	}

	// Run broadcastquery in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Now update sender address to the current peer graph database
		err = UpdateQuerySenderAddress(msg, kadDHT.Host().ID().String(), gm)
		if err != nil {
			logger.Errorf("Error updating sender address: %v", err)
		}
		msg.Sender = kadDHT.Host().ID().String() // So that the overlay peers identify this as parent peer

		

		// Now broadcast
		BroadcastQuery(msg, conn, config, gm, kadDHT)
	}()

	// Wait for all goroutines to complete
	go func(){
		wg.Wait()
		logger.Info("All operations in broadcast are complete parent peer")

		//TODO: Fetch all stored results from the graph database and send to the parent peerHEREHER

		// Fetch all stored results from the graph database and send to the parent peer
		// This is to ensure that the parent peer gets all results even if they were not broadcast

		//finalQuery := "get Results"

		/* allResults, err := FetchAllResults(finalQuery, gm)
		if err != nil {
			logger.Errorf("Error fetching all results: %v", err)
			return
		} */

		// Remove duplicates from the results and send to the parent peer
		//allResults = RemoveDuplicates(allResults)

		//logger.Info("All results: %s", allResults) // for debugging

		// Send the results to the parent peer
		//helpers.SendMergedResult(conn, conn.Conn().RemotePeer(), allResults, header, kadDHT)

		logger.Infof("Time taken to execute query: %v", time.Since(startTime)) // for debugging
		// close the stream
		if err := conn.Close(); err != nil {
			logger.Errorf("Error closing stream: %v", err)
		}
	}()
}

// Function to handle aggregate queries
func HandleAggregateQuery(conn network.Stream, msg *common.QueryMessage, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, agg common.AggregateInfo) {
	// Determine the node kind from the query
	nodeKind := helpers.ExtractNodeKind(msg.Query)

	logger.Infof("Handling aggregate query: %s with node %s", agg.Function, nodeKind) // Debugging logs

    // Execute local aggregate
	localSum, localCount, err := RunAggregatedQuery(agg, nodeKind, gm, kadDHT)
    if err != nil {
        logger.Errorf("Aggregate query failed: %v", err)
        return
    }

	// Determine our full peer address.
    myAddr := kadDHT.Host().Addrs()[0].Encapsulate(multiaddr.StringCast("/p2p/" + kadDHT.Host().ID().String())).String()

	// If this is an intermediate peer, merge and send the result to parent peer.
	if msg.Originator != myAddr {
		remoteResults := BroadcastAggregateQuery(msg, conn, config, gm, kadDHT)
		totalSum, totalCount := localSum, localCount
        for _, r := range remoteResults {
            totalSum += r[0]
            totalCount += r[1]
        }
        switch agg.Function {
        case "avg":
            helpers.SendPartialAggregateResult(conn, agg, totalSum, totalCount, kadDHT)
        case "sum":
            helpers.SendPartialAggregateResult(conn, agg, totalSum, 0, kadDHT)
        case "max":
            finalMax := localSum
            for _, r := range remoteResults {
                if r[0] > finalMax {
                    finalMax = r[0]
                }
            }
            helpers.SendPartialAggregateResult(conn, agg, finalMax, 0, kadDHT)
        case "min":
            finalMin := localSum
            for _, r := range remoteResults {
                if r[0] < finalMin {
                    finalMin = r[0]
                }
            }
            helpers.SendPartialAggregateResult(conn, agg, finalMin, 0, kadDHT)
        }
        return
	}

    // Otherwise, this is the originator. Merge remote results and compute the final aggregate.
    remoteResults := BroadcastAggregateQuery(msg, conn, config, gm, kadDHT)
	totalSum := localSum
	totalCount := localCount

	for _, r := range remoteResults {
		totalSum += r[0]
		totalCount += r[1]
	}

	var finalValue float64
	switch agg.Function {
		case "avg":
			if totalCount != 0 {
				finalValue = totalSum / totalCount
			}else{
				finalValue = 0
			}
		case "sum":
			finalValue = totalSum
		case "max":
			localMax, _, _ := RunAggregatedQuery(common.AggregateInfo{Function: "max", Traversal: agg.Traversal, Filter: agg.Filter, Attribute: agg.Attribute}, nodeKind, gm, kadDHT)
			finalValue = localMax
			for _, r := range remoteResults {
				if r[0] > finalValue {
					finalValue = r[0]
				}
			}
		case "min":
			localMin, _, _ := RunAggregatedQuery(common.AggregateInfo{Function: "min", Traversal: agg.Traversal, Filter: agg.Filter, Attribute: agg.Attribute}, nodeKind, gm, kadDHT)
			finalValue = localMin
			for _, r := range remoteResults {
				if r[0] < finalValue {
					finalValue = r[0]
				}
			}
		default:
			finalValue = 0
	}

	logger.Infof("Computed %s aggregate: %f", agg.Function, finalValue) // Debugging logs

	// Replace the placeholder in the original query.
	placeholder := fmt.Sprintf("@%s(%s)", agg.Function, agg.Traversal)
	finalQuery := strings.Replace(msg.Query, placeholder, fmt.Sprintf("%f", finalValue), 1)
	logger.Infof("Final query: %s", finalQuery)

	// Execute the final query on the originator.
	finalRows, finalHeader, err := RunIDSSQuery(finalQuery, kadDHT.Host().ID(), gm)
	if err != nil {
		logger.Errorf("Error executing final query: %v", err)
		return
	}
	helpers.SendMergedResult(conn, conn.Conn().RemotePeer(), finalRows, finalHeader, kadDHT)
}

// Function to send the partial aggregate result to the parent peer
func RunAggregatedQuery(agg common.AggregateInfo, nodeKind string ,gm *graph.Manager, kadDHT *dht.IpfsDHT) ( float64,  float64,  error) {
	// Construct the base query.
    var baseQuery string
	if !strings.Contains(agg.Traversal, ":") {
		// When there is no colon, assume the attribute is directly on the node.
		baseQuery = fmt.Sprintf("get %s", nodeKind)
	} else {
		// Otherwise, include a traverse clause.
		if strings.TrimSpace(agg.Filter) == "" {
			baseQuery = fmt.Sprintf("get %s traverse %s", nodeKind, agg.Traversal)
		} else {
			baseQuery = fmt.Sprintf("get %s traverse %s where %s", nodeKind, agg.Traversal, agg.Filter)
		}
	}
	// Run the query to get all matching rows.
	logger.Infof("Running a query: %s", baseQuery)
	rows, header, err := RunIDSSQuery(baseQuery, kadDHT.Host().ID(), gm)
	if err != nil {
		return 0, 0, err
	}

	logger.Info("Got rows: ", rows) //debugging

	// Compute the aggregate value.
	sum, count, min, max, err := helpers.ComputeAggregate(rows, header, agg.Attribute)
	if err != nil {
		return 0, 0, err
	}

	// Apply the comparison filter.
	switch agg.Function {
	case "sum":
		return sum, 0, nil
	case "avg":
		return sum, count, nil
	case "max":
		return max, 0, nil
	case "min":
		return min, 0, nil
	default:
		return 0, 0, fmt.Errorf("unsupported aggregate function")
	}
}

func UpdateQuerySenderAddress(s1 *common.QueryMessage, s2 string, gm *graph.Manager) error {
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

func FetchQueryDetails(s string, gm *graph.Manager) (map[string]interface{}, error) {
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
func CheckDuplicateQuery(uqi string, gm *graph.Manager) ([][]interface{}, error) {
	statement := fmt.Sprintf("get Query where key = '%s'", uqi)

	checkQuery, err := eql.RunQuery("checkQuery", "main", statement, gm)
	if err != nil {
		return nil, err
	}
	return checkQuery.Rows(), nil
}

// Function to store query information in the graph database using msg contents
func StoreQueryInfo(msg *common.QueryMessage, graphManager *graph.Manager, remotePeerID string) {
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

// Function to broadcast the aggregate query to connected peers
func BroadcastAggregateQuery(msg *common.QueryMessage, parentStream network.Stream, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) [][2]float64 {
    var remoteResults [][2]float64
    var mu sync.Mutex
    var wg sync.WaitGroup

    targetProtocol := protocol.ID(config.ProtocolID) // Protocol ID for the stream
	connectedPeers := kadDHT.Host().Network().Peers() // Connected peers are the peers
	var eligiblePeers []peer.ID

	for _, peerID := range connectedPeers {
		if 	peerID != kadDHT.Host().ID() && 
			(parentStream == nil || parentStream.Conn().RemotePeer() != peerID) {
			// Check if the peer is in the closest peers list then add to eligible peers
			eligiblePeers = append(eligiblePeers, peerID) // Check if there is an active connection to the peer
		}
	}

	if len(eligiblePeers) > 0 {
		logger.Infof("There are %d eligible peers to broadcast to. Will filter by using protocol", len(eligiblePeers))
	} else {
		logger.Warn("No eligible peers to broadcast to")
	}

	// Update TTL in the graph database and the message
	if err := UpdateTTL(msg, gm); err != nil {
		logger.Errorf("Error updating TTL: %v", err)
	}
	logger.Infof("Broadcasting query with TTL: %f", msg.Ttl)
    
    for _, peerID := range eligiblePeers {
        wg.Add(1)
        go func(p peer.ID) {
            defer wg.Done()

			msgCopy := proto.Clone(msg).(*common.QueryMessage) // Clone the message to
            
            // Stream creation
			streamCtx, streamCancel := context.WithTimeout(
				context.Background(), 
				time.Duration(msgCopy.Ttl)*1000*time.Millisecond) // Stream life span is the TTL
			defer streamCancel()

			stream, err := kadDHT.Host().NewStream(streamCtx, p, targetProtocol)
			if err != nil {
				return
			}
			defer stream.Close()

			// Change msg TYPE to QUERY
			msgCopy.Type = common.MessageType_QUERY
			msgBytes, err := proto.Marshal(msgCopy)
			if err != nil {
				logger.Errorf("Error marshalling query message: %v", err)
				return
			}

			// Send the query to the peer
			if err := helpers.WriteDelimitedMessage(stream, msgBytes); err != nil {
				logger.Errorf("Error writing query to peer %s: %v", p, err)
				return
			}

			logger.Info("Query sent to peer %s, with TTL: %v. Awaiting for response", p, msgCopy.Ttl)
            
            // Receive response
            sum, count, err := ReceiveAggregateResponse(stream, streamCtx)
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

// Function to send the partial aggregate result to the parent peer
func ReceiveAggregateResponse(stream network.Stream, ctx context.Context) (float64, float64, error) {
    defer stream.Close()

    // Read response message
    msgBytes, err := helpers.ReadDelimitedMessage(stream, ctx)
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

// IDSS function to broadcast the query to connected peers. This function also filters out the originating and parent peers because they are already queried
func BroadcastQuery(msg *common.QueryMessage, parentStream network.Stream, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) {
	var headers []string
	var localResults [][]interface{} // To hold the local results
	//logger.Infof(" Received results is %v", localResults) // for debugging
	logger.Infof("Checking if to continue broadcasting query: %s", msg.Uqid)

	if !ShouldContinueBroadcastingQuery(msg, gm) {
		logger.Infof("Query %s will not be broadcast further due to state or TTL", msg.Uqid)
        if msg.State.State == common.QueryState_SENT_BACK {
            logger.Info("Sending query results back...")
            parentPeerID := parentStream.Conn().RemotePeer()
            helpers.SendMergedResult(parentStream, parentPeerID, localResults, headers, kadDHT) // Send back local results if they weren't broadcast
        }
		return
	}
	
	logger.Infof("We can continue broadcasting query: %s, TTL: %v", msg.Uqid, msg.Ttl)
	peersInRoutingTable := kadDHT.RoutingTable().ListPeers() // Get the peers in the routing table
	logger.Debug("The number of peers in routing table is %d", kadDHT.RoutingTable().Size())

	var mergedResults [][]interface{} // To hold the merged results from all peers
	targetProtocol := protocol.ID(config.ProtocolID) // Protocol ID for the stream
	var eligiblePeers []peer.ID

	// Filter peers to exclude the originating peer and the parent stream peer. Because we dont want to query them again
	// Ensure that we don't query self, the parent stream peer or the peer that sent the query
	for _, peerID := range peersInRoutingTable {
		if 	peerID != kadDHT.Host().ID() && 
			(parentStream == nil || parentStream.Conn().RemotePeer() != peerID) {
			// Check if the peer is in the closest peers list then add to eligible peers
			eligiblePeers = append(eligiblePeers, peerID) // Check if there is an active connection to the peer
		}
	}

	// Update TTL in the graph database and the message
	/* if err := UpdateTTL(msg, gm); err != nil {logger.Errorf("Error updating TTL: %v", err)}
	logger.Debug("New TTL: %f", msg.Ttl) */

	if len(eligiblePeers) > 0 { // There must be at least one eligible peer to broadcast to
		logger.Infof("Broadcasting query to %d peers", len(eligiblePeers))
	} else {
		logger.Warn("No eligible peers to broadcast to")
		return
	}

	// Prepare for concurrent broadcasting. We have to use concurrency to run the query 
	// on all eligible peers without waiting for one to complete
	var localWg sync.WaitGroup
	remoteResultsChan := make(chan [][]interface{}, len(eligiblePeers)) // enough to accomodate involved peers
	duration := time.Duration(int64(msg.Ttl)) * 1000 * time.Millisecond // Duration follows the TTL
	//responseTimeout := time.After(duration)

	// Broadcast the query, and wait for results from the peers that have received a query from you
	for _, peerID := range eligiblePeers {
		localWg.Add(1)
		go func(p peer.ID){ // Separate goroutine for each peer
			defer localWg.Done()

			streamCtx, streamCancel := context.WithTimeout(context.Background(), time.Duration(int64(msg.Ttl)) * 1000 * time.Millisecond) // Stream life span is the TTL
			defer streamCancel() // Automatically release resources when the stream is done

			stream, err := kadDHT.Host().NewStream(streamCtx, p, targetProtocol) // Create a new stream to the peer based on the protocol and context
			if err != nil {
				return
			}
			defer stream.Close()

			//TODO: If set here, TTL count works well, with less data fetched. Need to investigate more
			if err := UpdateTTL(msg, gm); err != nil {logger.Errorf("Error updating TTL: %v", err)}
			logger.Debug("New TTL: %f", msg.Ttl)

			// Change msg TYPE to QUERY
			msg.Type = common.MessageType_QUERY
			msgBytes, err := proto.Marshal(msg)
			if err != nil {
				logger.Errorf("Error marshalling query message: %v", err)
				return
			}

			// Send the query to the peer
			if err := helpers.WriteDelimitedMessage(stream, msgBytes); err != nil {
				logger.Debugf("Error writing query to peer %s: %v", p, err)
				return
			}

			logger.Infof("Query sent to peer %s, with TTL: %v. Awaiting for response", p, msg.Ttl)


			// Wait for response from the peer
			type readResult struct {
				data []byte
				err  error
			}

			readCh := make(chan readResult)
			go func() {
				for{
					data, err := helpers.ReadDelimitedMessage(stream, streamCtx)
					if err != nil {
						readCh <- readResult{nil, err}
						break
					}
					readCh <- readResult{data, err}
				}
				close(readCh)
				
				//readCh <- readResult{data, err}
			}()

			var remoteResults common.QueryMessage
			for res := range readCh {
				if res.err != nil {	
					logger.Debugf("Error reading remote results from peer %s: %v", p, res.err)
					return
				}
				logger.Infof("Received %d records from peer %s", len(res.data), p)

				if err := proto.Unmarshal(res.data, &remoteResults); err != nil {
					logger.Errorf("Error unmarshalling remote results from peer %s: %v", p, err)
					return
				}
				if remoteResults.Type == common.MessageType_RESULT {
					remoteResult := helpers.ConvertProtobufRowsToResult(remoteResults.Result)
					remoteResultsChan <- remoteResult
				}
			}
		}(peerID)
	}

	// A channel to signal when all goroutines have completed
	done := make(chan struct{})

	// Waits for all goroutines (all eligible peers) to complete. 
	// To ensure that we do not bock indefinitely, TTL and timeout are used
	go func(){
		localWg.Wait() 
		close(done)

		if len(remoteResultsChan) > 0 {
			logger.Infof("Remote results received, now merging remote with local results") //TODO: Print this only if you receive results
		}
	}()

	// Wait until either all peers have finished or the response times out
	select {
		case <-done:
			logger.Info("Best efforts remote results collected")
		case <-time.After(duration):
			logger.Warn("Timeout waiting for remote results")
	}


	// Close the channel to avoid deadlock
	close(remoteResultsChan)
	for result := range remoteResultsChan {
		mergedResults = append(mergedResults, result...)
	}

	// fetch local results from the graph database
	localResults, headers, err := RunIDSSQuery(msg.Query, kadDHT.Host().ID(), gm)
	if err != nil {
		logger.Errorf("Error executing local query: %v", err)
		return
	}

	logger.Infof("Merge local and remote results")
	mergedResults = helpers.MergeTwoResults(localResults, mergedResults, headers) // Merge local and remote results
	StoreResults(msg, mergedResults, gm) // Store the merged results in the graph database

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
			UpdateQueryState(msg, common.QueryState_SENT_BACK, gm)
		}()

		insideWg.Add(1)
		go func() {
			defer insideWg.Done()
			// fetch header from the graph database
			
			logger.Infof("This is an intermediate peer %s. Sending results to parent peer %s", kadDHT.Host().ID(), parentPeerID)
			helpers.SendMergedResult(parentStream, parentPeerID, mergedResults, headers, kadDHT) 
		}()

		insideWg.Wait()
	}else{
		logger.Infof("This is the originator peer %s. Update state to completed and store into graph", kadDHT.Host().ID())
		// Fetch TTL from the graph database
		queryDetails, err := FetchQueryDetails(msg.Uqid, gm)
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
		mergedResults = helpers.MergeTwoResults(localResults, mergedResults, headers)

		withClauses := helpers.ParseWithClauses(msg.Query)
		if len(withClauses) > 0 {
			mergedResults = helpers.ApplyWithClauses(mergedResults, headers, withClauses)
		}

		// Update the query state to completed
		msg.State = &common.QueryState{State: common.QueryState_COMPLETED}
		UpdateQueryState(msg, common.QueryState_COMPLETED, gm)

		// Send the merged results to the client
		clientPeerID, err := peer.Decode(msg.Sender)
		if err != nil {
			logger.Errorf("Error decoding client peer ID: %v", err)
			return
		}

		StoreResults(msg, mergedResults, gm) // Store the merged results in the graph database

		helpers.SendMergedResult(parentStream, clientPeerID, mergedResults, headers, kadDHT)
	}
}

// Function to decide on to continue or stop broadcasting the query. To proceed, the query must not be in a completed state and the TTL must be greater than 0
func ShouldContinueBroadcastingQuery(msg *common.QueryMessage, gm *graph.Manager) bool {
	queryInfo, err := CheckDuplicateQuery(msg.Uqid, gm)
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
	queryDetails, err := FetchQueryDetails(msg.Uqid, gm)
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
			msg.Ttl >= 0 // Continue broadcasting
}

// Function to update the TTL in the graph database
func UpdateTTL(msg *common.QueryMessage, gm *graph.Manager) error {
	newTTL := msg.Ttl * 0.75 // Reduce the TTL by 25%

	// Avoid negative TTL
	if newTTL < 0 {
		newTTL = 0
	}

	logger.Debug("Updating TTL: old TTL: %f, new TTL: %f", msg.Ttl, newTTL)
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
	return nil
}

// IDSS Function to execute local query
func RunIDSSQuery(command string, peer peer.ID, gm *graph.Manager) ([][]interface{}, []string, error) {
	logger.Debug("Executing %s locally in %s", command, peer)

	result, err := eql.RunQuery("myQuery", "main", command, gm)
	if err != nil {
		logger.Error("Error querying data: ", err)
		return nil, nil, err
	}
	// Filter out metadata rows and keep only actual values
	
	header := result.Header().Labels()
	
    var dataRows [][]interface{}
	for _, row := range result.Rows() {
		if len(row) == 0 || helpers.IsMetadataRow(row[0]) {
			continue
		}
		dataRows = append(dataRows, row)
	}
	logger.Infof("Query result - Header: %v, Data Rows: %d", header, len(dataRows))
	return dataRows, header, nil
}

// Function to update the query state in the graph database
func UpdateQueryState(msg *common.QueryMessage, state common.QueryState_State, gm *graph.Manager) {
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
func StoreResults(msg *common.QueryMessage, results [][]interface{}, gm *graph.Manager) {
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