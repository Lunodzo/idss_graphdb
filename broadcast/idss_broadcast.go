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
	"regexp"
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
		BroadcastQuery(msg, conn, config, gm, kadDHT, header)
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
			msgCopy.Result = nil
            
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
func BroadcastQuery(msg *common.QueryMessage, parentStream network.Stream, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, finalHeader []string) {
    if !ShouldContinueBroadcastingQuery(msg, gm) {
        logger.Infof("Query %s will not be broadcast further due to state or TTL", msg.Uqid)
        return
    }

    logger.Infof("We can continue broadcasting query: %s, TTL: %v", msg.Uqid, msg.Ttl)
    peersInRoutingTable := kadDHT.RoutingTable().ListPeers()

    var mergedResults [][]interface{}
    targetProtocol := protocol.ID(config.ProtocolID)
    var eligiblePeers []peer.ID

    for _, peerID := range peersInRoutingTable {
        if peerID != kadDHT.Host().ID() && (parentStream == nil || parentStream.Conn().RemotePeer() != peerID) {
            eligiblePeers = append(eligiblePeers, peerID)
        }
    }

    if len(eligiblePeers) == 0 {
        logger.Warn("No eligible peers to broadcast to")
        return
    }

    var wg sync.WaitGroup
    remoteResultsChan := make(chan [][]interface{}, len(eligiblePeers))
    duration := time.Duration(int64(msg.Ttl)) * 1000 * time.Millisecond

    localResults, finalHeader, err := RunIDSSQuery(msg.Query, kadDHT.Host().ID(), gm)
    if err != nil {
        logger.Errorf("Error executing local query: %v", err)
        return
    }

	// Use finalHeader if provided, otherwise use local header
    if len(finalHeader) == 0 {
        for i, h := range finalHeader {
            parts := strings.FieldsFunc(h, func(r rune) bool { return r == ':' || r == ' ' })
            if len(parts) > 0 {
                finalHeader[i] = parts[len(parts)-1]
            }
        }
    }

    logger.Infof("Query result - Header: %v, Data Rows: %d", finalHeader, len(localResults))

    for _, peerID := range eligiblePeers {
        wg.Add(1)
        go func(p peer.ID) {
            defer wg.Done()

            streamCtx, cancel := context.WithTimeout(context.Background(), duration)
            defer cancel()

            stream, err := kadDHT.Host().NewStream(streamCtx, p, targetProtocol)
            if err != nil {
                logger.Debugf("Error writing query to peer %s: %v", p, err)
                return
            }
            defer stream.Close()

            if err := UpdateTTL(msg, gm); err != nil {
                logger.Errorf("Error updating TTL: %v", err)
            }

            msg.Type = common.MessageType_QUERY
            msgBytes, err := proto.Marshal(msg)
            if err != nil {
                logger.Errorf("Error marshalling query message: %v", err)
                return
            }

            if err := helpers.WriteDelimitedMessage(stream, msgBytes); err != nil {
                logger.Debugf("Error writing query to peer %s: %v", p, err)
                return
            }

            logger.Infof("Query sent to peer %s, with TTL: %v", p, msg.Ttl)

            data, err := helpers.ReadDelimitedMessage(stream, streamCtx)
            if err != nil {
                logger.Debugf("Error reading remote results from peer %s: %v", p, err)
                return
            }

            var remoteResults common.QueryMessage
            if err := proto.Unmarshal(data, &remoteResults); err != nil {
                logger.Errorf("Error unmarshalling remote results from peer %s: %v", p, err)
                return
            }
            if remoteResults.Type == common.MessageType_RESULT {
                result := helpers.ConvertProtobufRowsToResult(remoteResults.Result)
                logger.Infof("Received %d raw records from peer %s", len(result), p)
                filteredResult := filterHeaderRows(result, finalHeader)
                logger.Debug("Filtered to %d records from peer %s", len(filteredResult), p)
                remoteResultsChan <- filteredResult
            }
        }(peerID)
    }

    go func() {
        wg.Wait()
        close(remoteResultsChan)
    }()

    for result := range remoteResultsChan {
        mergedResults = append(mergedResults, result...)
    }
    logger.Infof("Remote results received, total rows before local: %d", len(mergedResults))

    localResults = filterHeaderRows(localResults, finalHeader)
    logger.Infof("Local results after filtering: %d", len(localResults))
    mergedResults = append(mergedResults, localResults...)
    logger.Infof("Total rows before deduplication: %d", len(mergedResults))
    uniqueResults := deduplicateRows(mergedResults)
    logger.Infof("Merged local and remote results, unique rows: %d", len(uniqueResults))

	// Reapply WITH clause sorting if present in the original query
    withClauses := helpers.ParseWithClauses(msg.Query)
    if withClauses != nil {
		logger.Infof("Applying WITH clause: %v on header: %v", withClauses, finalHeader)
        uniqueResults = helpers.ApplyWithClauses(uniqueResults, finalHeader, withClauses)
        logger.Infof("Applied WITH clause sorting from query '%s', final rows: %d", msg.Query, len(uniqueResults))
		logger.Info("Header after applying WITH clause: ", finalHeader)
    }

    parentPeerID := parentStream.Conn().RemotePeer()
    peerAddr := kadDHT.Host().Addrs()[0].Encapsulate(multiaddr.StringCast("/p2p/" + kadDHT.Host().ID().String())).String()

    logger.Infof("Comparing originator %s with current peer %s", msg.Originator, peerAddr)
    if msg.Originator != peerAddr {
        msg.State = &common.QueryState{State: common.QueryState_SENT_BACK}
        UpdateQueryState(msg, common.QueryState_SENT_BACK, gm)
        logger.Infof("Intermediate peer %s sending %d rows to parent %s", kadDHT.Host().ID(), len(uniqueResults), parentPeerID)
        helpers.SendMergedResult(parentStream, parentPeerID, uniqueResults, finalHeader, kadDHT)
    } else {
        queryDetails, err := FetchQueryDetails(msg.Uqid, gm)
        if err != nil {
            logger.Errorf("Error fetching query details: %v", err)
        }
        if clientPeerID, ok := queryDetails["Sender Address"].(string); ok {
            msg.Sender = clientPeerID
        } else {
            logger.Warn("Client peer ID not found or it is not a string")
        }

        msg.State = &common.QueryState{State: common.QueryState_COMPLETED}
        UpdateQueryState(msg, common.QueryState_COMPLETED, gm)
        StoreResults(msg, uniqueResults, gm)

        clientPeerID, err := peer.Decode(msg.Sender)
        if err != nil {
            logger.Errorf("Error decoding client peer ID: %v", err)
            return
        }
        logger.Infof("Originator peer %s sending %d rows to client %s", kadDHT.Host().ID(), len(uniqueResults), clientPeerID)
        helpers.SendMergedResult(parentStream, clientPeerID, uniqueResults, finalHeader, kadDHT)
    }
}

func deduplicateRows(rows [][]interface{}) [][]interface{} {
    seen := make(map[string]struct{})
    var unique [][]interface{}
    for _, row := range rows {
        // Create a unique key from all fields
        keyParts := make([]string, len(row))
        for i, val := range row {
            keyParts[i] = fmt.Sprintf("%v", val)
        }
        key := strings.Join(keyParts, "|")
        if _, exists := seen[key]; !exists {
            seen[key] = struct{}{}
            unique = append(unique, row)
        }
    }
    return unique
}

// Helper to filter out header rows from results
func filterHeaderRows(rows [][]interface{}, header []string) [][]interface{} {
    var filtered [][]interface{}
    for _, row := range rows {
        if len(header) > 0 && len(row) == len(header) && isHeaderRow(row, header) {
            continue
        }
		filtered = append(filtered, row)
    }
	logger.Debug("The filtered rows are: ", filtered)
    return filtered
}

// Check if a row matches the header
func isHeaderRow(row []interface{}, header []string) bool {
    if len(row) != len(header) {
        return false
    }
    for i, val := range row {
        if fmt.Sprintf("%v", val) != header[i] {
            return false
        }
    }
    return true
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

	// Parse WITH clauses
	withClauses := helpers.ParseWithClauses(command)
	baseQuery := command
	if withClauses != nil {
		// Remove WITH clause from the query for EliasDB
		re := regexp.MustCompile(`(?i)\bwith\s+(.*?)(?:\s*;|\s*$)`)
		baseQuery = re.ReplaceAllString(command, "")
		baseQuery = strings.TrimSpace(baseQuery)
	}

	result, err := eql.RunQuery("myQuery", "main", baseQuery, gm)
	if err != nil {
		logger.Error("Error querying data: ", err)
		return nil, nil, err
	}
	// Filter out metadata rows and keep only actual values
	
	header := result.Header().Labels()
	for i, h := range header {
        parts := strings.FieldsFunc(h, func(r rune) bool { return r == ':' || r == ' ' })
        if len(parts) > 0 {
			header[i] = strings.Title(parts[len(parts)-1])
        }
    }
	
    var dataRows [][]interface{}
	for _, row := range result.Rows() {
		if len(row) == 0 || helpers.IsMetadataRow(row[0]) {
			continue
		}
		dataRows = append(dataRows, row)
	}

	// Apply WITH clauses (e.g., ordering) if present
    if withClauses != nil {
		logger.Infof("Applying WITH clauses: %v with header: %v", withClauses, header)
        dataRows = helpers.ApplyWithClauses(dataRows, header, withClauses)
    }
	
	logger.Infof("Query result - Header: %v, Data Rows: %d", header, len(dataRows))
	logger.Infof("Query result - Header: %v, Data Rows: %d, First Row: %v", header, len(dataRows), dataRows[0])
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