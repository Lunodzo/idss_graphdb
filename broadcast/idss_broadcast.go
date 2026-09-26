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
	"math/rand"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"encoding/json"
	"fmt"
	"idss/graphdb/access"
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

const maxConcurrentPeerQueries = 30
const maxForwardPeers = 30
const ttlReductionFactor = 0.75

// Function to handle and broadcast queries in the IDSS system
func ExecuteAndBroadcastQuery(conn network.Stream, msg *common.QueryMessage, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, decision access.Decision) {
	// Try to get query details from the graph database, but fall back to message fields if not found
	queryDetails, err := FetchQueryDetails(msg.Uqid, gm)
	if err != nil {
		logger.Warnf("Could not fetch query details for %s: %v; using message fields directly", msg.Uqid, err)
		// Continue with message fields as-is; they should be populated from the client or previous hop
	} else if queryDetails != nil && len(queryDetails) > 0 {
		// Use the details from the graph
		logger.Infof("Using query details from graph database for %s", msg.Uqid)

		if query_string, ok := queryDetails["Query String"].(string); ok {
			msg.Query = query_string
		}

		if originator, ok := queryDetails["Originator"]; ok {
			msg.Originator = originator.(string)
		}

		if senderAddress, ok := queryDetails["Sender Address"]; ok {
			msg.Sender = senderAddress.(string)
		}

		if arrivalTime, ok := queryDetails["Arrival Time"]; ok {
			msg.Timestamp = arrivalTime.(string)
		}

		if queryKey, ok := queryDetails["Query Key"]; ok {
			if keyStr, ok := queryKey.(string); ok && strings.TrimSpace(keyStr) != "" {
				msg.Uqid = keyStr
			}
		}
	}

	msg.Result = nil // Clear the result

	logger.Infof("Query UQI: %s, TTL: %f", msg.Uqid, msg.Ttl) // for debugging
	var wg sync.WaitGroup                                     // Wait group to ensure all operations are completed before closing the stream
	var localResHolder [][]interface{}                        // To hold the local results
	startTime := time.Now()                                   // for debugging duration of query execution
	var header []string

	// Run local query
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, header, err := RunIDSSQueryWithDecision(msg.Query, kadDHT.Host().ID(), gm, decision)
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
	if remainingQueryTime(msg) <= 0 { // Return the locally available result at the deadline.
		logger.Warn("TTL expired, not broadcasting query")
		UpdateQueryState(msg, common.QueryState_SENT_BACK, gm)
		wg.Wait() //
		respondingPeerIDs := map[string]struct{}{}
		if decision != access.Deny {
			respondingPeerIDs[kadDHT.Host().ID().String()] = struct{}{}
		}
		helpers.SendMergedResultWithPeers(conn, conn.Conn().RemotePeer(), localResHolder, header, peerIDList(respondingPeerIDs), kadDHT)
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
		BroadcastQuery(msg, conn, config, gm, kadDHT, header, decision)
	}()

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		logger.Info("All operations in broadcast are complete parent peer")

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

	// Execute local aggregate. Average accumulates as a running sum just like sum
	// and is only turned into a mean once, at the originator.
	localValue, hasLocal, err := RunAggregatedQuery(agg, nodeKind, gm, kadDHT)
	if err != nil {
		// No local rows matched (or a schema issue occurred); keep broadcasting so
		// downstream peers holding matching data are not cut off from the query.
		logger.Warnf("No local aggregate contribution: %v", err)
	}

	// Determine our full peer address.
	myAddr := kadDHT.Host().Addrs()[0].Encapsulate(multiaddr.StringCast("/p2p/" + kadDHT.Host().ID().String())).String()

	remoteValues, remotePeerIDs := BroadcastAggregateQuery(msg, conn, config, gm, kadDHT)
	respondingPeerIDs := append([]string{kadDHT.Host().ID().String()}, remotePeerIDs...)

	// Merge local and downstream values identically at every stage: sum/avg add up,
	// min/max compare. Only the final divide for avg is deferred to the originator.
	merged := localValue
	hasValue := hasLocal
	switch agg.Function {
	case "sum", "avg":
		for _, v := range remoteValues {
			merged += v
		}
		hasValue = hasValue || len(remoteValues) > 0
	case "max":
		for _, v := range remoteValues {
			if !hasValue || v > merged {
				merged = v
				hasValue = true
			}
		}
	case "min":
		for _, v := range remoteValues {
			if !hasValue || v < merged {
				merged = v
				hasValue = true
			}
		}
	default:
		logger.Errorf("Unsupported aggregate function: %s", agg.Function)
		return
	}
	if !hasValue {
		merged = 0
	}

	// If this is an intermediate peer, forward the merged partial result upstream.
	if msg.Originator != myAddr {
		helpers.SendPartialAggregateResult(conn, merged, hasValue, respondingPeerIDs, kadDHT)
		return
	}

	// Otherwise, this is the originator. Finalize the aggregate during merge.
	finalValue := merged
	if agg.Function == "avg" {
		if peerCount := float64(len(respondingPeerIDs)); peerCount != 0 {
			finalValue = merged / peerCount
		} else {
			finalValue = 0
		}
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
	helpers.SendMergedResultWithPeers(conn, conn.Conn().RemotePeer(), finalRows, finalHeader, respondingPeerIDs, kadDHT)
}

// Function to send the partial aggregate result to the parent peer
func RunAggregatedQuery(agg common.AggregateInfo, nodeKind string, gm *graph.Manager, kadDHT *dht.IpfsDHT) (float64, bool, error) {
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
		return 0, false, err
	}

	logger.Info("Got rows: ", rows) //debugging

	// Compute the aggregate value.
	sum, _, min, max, err := helpers.ComputeAggregate(rows, header, agg.Attribute)
	if err != nil {
		// No matching local rows for this peer; not fatal, this peer simply has
		// nothing to contribute to the aggregate.
		return 0, false, err
	}

	// Apply the comparison filter.
	switch agg.Function {
	case "sum", "avg":
		// Average is finalized at the originator as sum / responding peers, so
		// locally it is computed and propagated exactly like sum.
		return sum, true, nil
	case "max":
		return max, true, nil
	case "min":
		return min, true, nil
	default:
		return 0, false, fmt.Errorf("unsupported aggregate function")
	}
}

func UpdateQuerySenderAddress(s1 *common.QueryMessage, s2 string, gm *graph.Manager) error {
	if s1 == nil || strings.TrimSpace(s1.Uqid) == "" {
		logger.Warn("Skipping sender address update for empty query UQI")
		return nil
	}
	if gm == nil {
		return fmt.Errorf("graph manager is nil for query %s", s1.Uqid)
	}
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
	if strings.TrimSpace(s) == "" {
		logger.Warn("Empty query UQI provided to FetchQueryDetails")
		return nil, nil // Return nil gracefully for empty UQI
	}
	if gm == nil {
		return nil, fmt.Errorf("graph manager is nil for query %s", s)
	}
	queryStatement := fmt.Sprintf("get Query where key = '%s'", s)
	results, err := eql.RunQuery("fetchQueryDetails", "main", queryStatement, gm)
	if err != nil {
		logger.Debugf("Error fetching query details for %s: %v", s, err)
		return nil, err
	}

	// Check if the query details are not empty
	if len(results.Rows()) == 0 {
		logger.Debugf("Query details not found for UQI: %s", s)
		return nil, nil // Return nil gracefully if not found instead of error
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
	if strings.TrimSpace(uqi) == "" {
		return nil, nil
	}
	if gm == nil {
		return nil, fmt.Errorf("graph manager is nil for query %s", uqi)
	}
	statement := fmt.Sprintf("get Query where key = '%s'", uqi)

	checkQuery, err := eql.RunQuery("checkQuery", "main", statement, gm)
	if err != nil {
		return nil, err
	}
	return checkQuery.Rows(), nil
}

// Function to store query information in the graph database using msg contents
func StoreQueryInfo(msg *common.QueryMessage, graphManager *graph.Manager, remotePeerID string) {
	if msg == nil || strings.TrimSpace(msg.Uqid) == "" {
		logger.Warn("Skipping store of query info for empty UQI")
		return
	}
	if graphManager == nil {
		logger.Warnf("Skipping query info store for %s because graph manager is nil", msg.Uqid)
		return
	}
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
func BroadcastAggregateQuery(msg *common.QueryMessage, parentStream network.Stream, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) ([]float64, []string) {
	if msg == nil || msg.Ttl <= 0 {
		return nil, nil
	}
	var remoteValues []float64
	respondingPeerIDs := make(map[string]struct{})
	var mu sync.Mutex
	var wg sync.WaitGroup
	streamSlots := make(chan struct{}, maxConcurrentPeerQueries)

	targetProtocol := protocol.ID(config.ProtocolID) // Protocol ID for the stream
	var eligiblePeers []peer.ID

	for _, peerID := range kadDHT.RoutingTable().ListPeers() {
		if peerID == kadDHT.Host().ID() || (parentStream != nil && parentStream.Conn().RemotePeer() == peerID) {
			continue
		}
		eligiblePeers = append(eligiblePeers, peerID)
	}
	eligiblePeers = selectForwardPeers(eligiblePeers, remainingQueryTime(msg))

	// Log the eligible peers for broadcasting the aggregate query
	if len(eligiblePeers) > 0 {
		logger.Infof("There are %d eligible peers to broadcast to. Will filter by using protocol", len(eligiblePeers))
	} else {
		logger.Warn("No eligible peers to broadcast to")
	}

	parentDuration := remainingQueryTime(msg)
	forwardMsg := proto.Clone(msg).(*common.QueryMessage)
	// Reduce the remaining wall-clock budget before forwarding so the current
	// peer has time to process and return the downstream response.
	if err := UpdateTTL(forwardMsg, gm); err != nil {
		logger.Errorf("Error updating TTL: %v", err)
	}
	logger.Infof("Broadcasting query with remaining TTL: %f", forwardMsg.Ttl)

	for _, peerID := range eligiblePeers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			streamSlots <- struct{}{}
			defer func() { <-streamSlots }()

			msgCopy := proto.Clone(forwardMsg).(*common.QueryMessage)
			msgCopy.Result = nil

			// Stream creation
			if parentDuration <= 0 || remainingQueryTime(msgCopy) <= 0 {
				return
			}
			streamCtx, streamCancel := context.WithTimeout(context.Background(), parentDuration)
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
			value, hasValue, peerIDs, err := ReceiveAggregateResponse(stream, streamCtx)
			if err != nil {
				return
			}

			mu.Lock()
			if hasValue {
				remoteValues = append(remoteValues, value)
			}
			for _, id := range peerIDs {
				respondingPeerIDs[id] = struct{}{}
			}
			mu.Unlock()
		}(peerID)
	}

	wg.Wait()
	return remoteValues, peerIDList(respondingPeerIDs)
}

// Function to send the partial aggregate result to the parent peer
func ReceiveAggregateResponse(stream network.Stream, ctx context.Context) (float64, bool, []string, error) {
	defer stream.Close()

	// Read response message
	msgBytes, err := helpers.ReadDelimitedMessage(stream, ctx)
	if err != nil {
		return 0, false, nil, fmt.Errorf("error reading response: %v", err)
	}

	// Unmarshal response into QueryMessage
	var response common.QueryMessage
	err = proto.Unmarshal(msgBytes, &response)
	if err != nil {
		return 0, false, nil, fmt.Errorf("error unmarshalling response: %v", err)
	}

	// Ensure the received message is of type RESULT
	if response.Type != common.MessageType_RESULT {
		return 0, false, nil, fmt.Errorf("unexpected message type: %v", response.Type)
	}

	// Extract the aggregate value from the response
	if len(response.Result) < 1 || len(response.Result[0].Data) < 1 {
		return 0, false, nil, fmt.Errorf("received empty aggregate result")
	}

	value, err := strconv.ParseFloat(response.Result[0].Data[0], 64)
	if err != nil {
		return 0, false, nil, fmt.Errorf("error parsing aggregate value: %v", err)
	}

	// RecordCount doubles as a has-value flag: a peer whose subtree found no
	// matching data still responds (so its ID counts for @avg) but contributes no value.
	return value, response.RecordCount > 0, response.RespondingPeerIds, nil
}

// IDSS function to broadcast the query to connected peers. This function also filters out the originating and parent peers because they are already queried
func BroadcastQuery(msg *common.QueryMessage, parentStream network.Stream, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, finalHeader []string, decision access.Decision) {
	if !ShouldContinueBroadcastingQuery(msg, gm) {
		logger.Infof("Query %s will not be broadcast further due to state or TTL", msg.Uqid)
		if parentStream != nil {
			localResults, localHeader, err := RunIDSSQueryWithDecision(msg.Query, kadDHT.Host().ID(), gm, decision)
			if err != nil {
				logger.Errorf("Error executing deadline-local query: %v", err)
				return
			}
			respondingPeerIDs := []string{}
			if decision != access.Deny {
				respondingPeerIDs = []string{kadDHT.Host().ID().String()}
			}
			helpers.SendMergedResultWithPeers(parentStream, parentStream.Conn().RemotePeer(), localResults, localHeader, respondingPeerIDs, kadDHT)
		}
		return
	}

	logger.Infof("We can continue broadcasting query: %s, TTL: %v", msg.Uqid, msg.Ttl)
	peersInRoutingTable := kadDHT.RoutingTable().ListPeers()

	var mergedResults [][]interface{}
	respondingPeerIDs := make(map[string]struct{})
	if decision != access.Deny {
		respondingPeerIDs[kadDHT.Host().ID().String()] = struct{}{}
	}
	targetProtocol := protocol.ID(config.ProtocolID)
	var eligiblePeers []peer.ID

	visited := make(map[string]struct{}, len(msg.Labels)+1)
	for _, peerID := range msg.Labels {
		visited[peerID] = struct{}{}
	}
	visited[kadDHT.Host().ID().String()] = struct{}{}
	if parentStream != nil {
		visited[parentStream.Conn().RemotePeer().String()] = struct{}{}
	}
	for _, peerID := range peersInRoutingTable {
		if _, alreadyVisited := visited[peerID.String()]; alreadyVisited {
			continue
		}
		eligiblePeers = append(eligiblePeers, peerID)
	}
	eligiblePeers = selectForwardPeers(eligiblePeers, remainingQueryTime(msg))

	if len(eligiblePeers) == 0 {
		logger.Warn("No eligible peers to broadcast to")
	}

	var wg sync.WaitGroup
	type remoteResult struct {
		rows    [][]interface{}
		peerIDs []string
	}
	remoteResultsChan := make(chan remoteResult, len(eligiblePeers))
	streamSlots := make(chan struct{}, maxConcurrentPeerQueries)
	localResults, finalHeader, err := RunIDSSQueryWithDecision(msg.Query, kadDHT.Host().ID(), gm, decision)
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
	parentDuration := remainingQueryTime(msg)
	forwardMsg := proto.Clone(msg).(*common.QueryMessage)
	if err := UpdateTTL(forwardMsg, gm); err != nil {
		logger.Errorf("Error reducing TTL for forwarding: %v", err)
		return
	}
	if parentDuration <= 0 || remainingQueryTime(forwardMsg) <= 0 {
		return
	}

	for _, peerID := range eligiblePeers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			streamSlots <- struct{}{}
			defer func() { <-streamSlots }()

			streamCtx, cancel := context.WithTimeout(context.Background(), parentDuration)
			defer cancel()

			stream, err := kadDHT.Host().NewStream(streamCtx, p, targetProtocol)
			if err != nil {
				logger.Debugf("Error writing query to peer %s: %v", p, err)
				return
			}
			defer stream.Close()

			msgCopy := proto.Clone(forwardMsg).(*common.QueryMessage)
			msgCopy.Type = common.MessageType_QUERY
			msgCopy.Labels = append(append([]string{}, msg.Labels...), kadDHT.Host().ID().String())
			msgBytes, err := proto.Marshal(msgCopy)
			if err != nil {
				logger.Errorf("Error marshalling query message: %v", err)
				return
			}

			if err := helpers.WriteDelimitedMessage(stream, msgBytes); err != nil {
				logger.Debugf("Error writing query to peer %s: %v", p, err)
				return
			}

			logger.Infof("Query sent to peer %s, with TTL: %v", p, msgCopy.Ttl)
			var allRemoteRows [][]interface{}
			var remotePeerIDs []string
			for {
				data, readErr := helpers.ReadDelimitedMessage(stream, streamCtx)
				if readErr != nil {
					logger.Debugf("Error reading remote results from peer %s: %v", p, readErr)
					// Preserve chunks already received when the deadline closes the
					// stream before the peer can send its final marker.
					break
				}
				var remoteResults common.QueryMessage
				if err := proto.Unmarshal(data, &remoteResults); err != nil {
					logger.Errorf("Error unmarshalling remote results from peer %s: %v", p, err)
					return
				}
				if remoteResults.Type != common.MessageType_RESULT {
					continue
				}
				allRemoteRows = append(allRemoteRows, helpers.ConvertProtobufRowsToResult(remoteResults.Result)...)
				remotePeerIDs = append(remotePeerIDs, remoteResults.RespondingPeerIds...)
				if remoteResults.RecordCount >= 0 {
					break
				}
			}
			filteredResult := filterHeaderRows(allRemoteRows, finalHeader)
			if len(filteredResult) > 0 || len(remotePeerIDs) > 0 {
				remoteResultsChan <- remoteResult{rows: filteredResult, peerIDs: remotePeerIDs}
			}
		}(peerID)
	}

	go func() {
		wg.Wait()
		close(remoteResultsChan)
	}()

	for result := range remoteResultsChan {
		mergedResults = append(mergedResults, result.rows...)
		for _, peerID := range result.peerIDs {
			respondingPeerIDs[peerID] = struct{}{}
		}
	}
	common.QueryPeersResponded.Observe(float64(len(respondingPeerIDs)))
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
		helpers.SendMergedResultWithPeers(parentStream, parentPeerID, uniqueResults, finalHeader, peerIDList(respondingPeerIDs), kadDHT)
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
		helpers.SendMergedResultWithPeers(parentStream, clientPeerID, uniqueResults, finalHeader, peerIDList(respondingPeerIDs), kadDHT)
	}
}

func peerIDList(peerIDs map[string]struct{}) []string {
	result := make([]string, 0, len(peerIDs)) // initialize the result slice with the capacity of peerIDs
	for peerID := range peerIDs {
		result = append(result, peerID)
	}
	sort.Strings(result)
	return result
}

func selectForwardPeers(peers []peer.ID, budget time.Duration) []peer.ID {
	if len(peers) == 0 || budget <= 0 {
		return nil
	}
	limit := maxForwardPeers
	switch {
	case budget <= 750*time.Millisecond: // this is less than 1 second
		limit = 30
	case budget <= 1500*time.Millisecond: // this is less than 1.5 seconds
		limit = 60
	case budget <= 3*time.Second: // this is less than 3 seconds
		limit = 120
	}

	// Ensure the limit does not exceed the number of available peers
	if v := os.Getenv("IDSS_MAX_FORWARD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			if n <= 0 {
				limit = len(peers) // 0 = flood to the whole routing table
			} else {
				limit = n
			}
		}
	}
	if limit > len(peers) {
		limit = len(peers)
	}
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	return peers[:limit]
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
	if msg == nil || strings.TrimSpace(msg.Uqid) == "" {
		logger.Warn("Query UQI is empty; stopping broadcast to avoid invalid graph updates")
		return false
	}
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

	return state != int32(common.QueryState_COMPLETED) &&
		state != int32(common.QueryState_SENT_BACK) &&
		remainingQueryTime(msg) > 0
}

// Function to update the TTL in the graph database
func UpdateTTL(msg *common.QueryMessage, gm *graph.Manager) error {
	if msg == nil || strings.TrimSpace(msg.Uqid) == "" {
		logger.Warn("Skipping TTL update for empty query UQI")
		return nil
	}
	if gm == nil {
		return fmt.Errorf("graph manager is nil for query %s", msg.Uqid)
	}
	newTTL := msg.Ttl * ttlReductionFactor

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
	if deadline, err := time.Parse(time.RFC3339Nano, msg.Timestamp); err == nil {
		remaining := time.Until(deadline)
		if remaining > 0 {
			childBudget := remaining * time.Duration(ttlReductionFactor*1000) / 1000
			msg.Timestamp = time.Now().Add(childBudget).UTC().Format(time.RFC3339Nano)
		}
	} else {
		msg.Timestamp = time.Now().Add(time.Duration(float64(newTTL) * float64(time.Second))).UTC().Format(time.RFC3339Nano)
	}
	return nil
}

func remainingQueryTime(msg *common.QueryMessage) time.Duration {
	if msg == nil || msg.Ttl <= 0 {
		return 0
	}
	if deadline, err := time.Parse(time.RFC3339Nano, msg.Timestamp); err == nil {
		return time.Until(deadline)
	}
	return time.Duration(float64(time.Second) * float64(msg.Ttl))
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
	baseQuery, projectedFields, rowLimit := parseResultClauses(baseQuery)

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
	if len(projectedFields) > 0 {
		indices := make([]int, 0, len(projectedFields))
		projectedHeader := make([]string, 0, len(projectedFields))
		for _, field := range projectedFields {
			for index, label := range header {
				if strings.EqualFold(label, field) {
					indices = append(indices, index)
					projectedHeader = append(projectedHeader, label)
					break
				}
			}
		}
		if len(indices) > 0 {
			projectedRows := make([][]interface{}, 0, len(dataRows))
			for _, row := range dataRows {
				projectedRow := make([]interface{}, 0, len(indices))
				for _, index := range indices {
					if index < len(row) {
						projectedRow = append(projectedRow, row[index])
					}
				}
				projectedRows = append(projectedRows, projectedRow)
			}
			dataRows, header = projectedRows, projectedHeader
		}
	}
	if rowLimit >= 0 && len(dataRows) > rowLimit {
		dataRows = dataRows[:rowLimit]
	}

	logger.Infof("Query result - Header: %v, Data Rows: %d", header, len(dataRows))
	if len(dataRows) > 0 {
		logger.Infof("Query result - Header: %v, Data Rows: %d, First Row: %v", header, len(dataRows), dataRows[0])
	}
	return dataRows, header, nil
}

func parseResultClauses(query string) (string, []string, int) {
	limit := -1
	limitPattern := regexp.MustCompile(`(?i)\s+limit\s+([0-9]+)\s*$`)
	if match := limitPattern.FindStringSubmatch(query); len(match) == 2 {
		limit, _ = strconv.Atoi(match[1])
		query = strings.TrimSpace(query[:len(query)-len(match[0])])
	}
	fieldsPattern := regexp.MustCompile(`(?i)\s+fields\s+([A-Za-z_][A-Za-z0-9_]*(?:\s*,\s*[A-Za-z_][A-Za-z0-9_]*)*)\s*$`)
	match := fieldsPattern.FindStringSubmatch(query)
	if len(match) != 2 {
		return query, nil, limit
	}
	fields := strings.Split(match[1], ",")
	for index := range fields {
		fields[index] = strings.TrimSpace(fields[index])
	}
	return strings.TrimSpace(query[:len(query)-len(match[0])]), fields, limit
}

// Function to execute a local query according to the receiving peer's access decision.
func RunIDSSQueryWithDecision(command string, peer peer.ID, gm *graph.Manager, decision access.Decision) ([][]interface{}, []string, error) {
	if decision == access.Deny {
		return nil, nil, nil
	}

	rows, header, err := RunIDSSQuery(command, peer, gm)
	if err != nil || decision != access.AggregateOnly {
		return rows, header, err
	}

	return [][]interface{}{{len(rows)}}, []string{"Count"}, nil
}

// LocalSettlementTotals returns aggregate-only settlement values for a period.
func LocalSettlementTotals(gm *graph.Manager, hostID peer.ID, from time.Time, to time.Time) (float64, float64, error) {
	period := fmt.Sprintf(` where timeStamp >= "%s" and timeStamp <= "%s"`, from.Format(time.RFC3339), to.Format(time.RFC3339))
	meterRows, meterHeader, err := RunIDSSQuery("get MeterReading"+period, hostID, gm)
	if err != nil {
		return 0, 0, fmt.Errorf("querying meter readings: %v", err)
	}
	tradeRows, tradeHeader, err := RunIDSSQuery("get Trade"+period, hostID, gm)
	if err != nil {
		return 0, 0, fmt.Errorf("querying trades: %v", err)
	}
	return sumColumn(meterRows, meterHeader, "Value"), sumColumn(tradeRows, tradeHeader, "Volume"), nil
}

// CompileSettlement collects aggregate-only peer totals and stores local summaries.
// Phase timings are logged so the experiment harness can report Q6's phase breakdown.
func CompileSettlement(gm *graph.Manager, kadDHT *dht.IpfsDHT, from time.Time, to time.Time) error {
	totalStart := time.Now()
	host := kadDHT.Host()
	results := []*common.SettlementResult{}

	localTotalsStart := time.Now()
	meterSum, tradeSum, err := LocalSettlementTotals(gm, host.ID(), from, to)
	if err != nil {
		return err
	}
	logger.Infof("Settlement phase=local_totals duration_ms=%.3f", time.Since(localTotalsStart).Seconds()*1000)
	results = append(results, &common.SettlementResult{PeerId: host.ID().String(), MeterReadingSum: meterSum, TradeVolumeSum: tradeSum})

	broadcastStart := time.Now()
	for _, remotePeer := range kadDHT.RoutingTable().ListPeers() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := host.NewStream(ctx, remotePeer, protocol.ID(common.IDSS_PROTOCOL_LOCAL))
		if err == nil {
			request := &common.QueryMessage{Type: common.MessageType_SETTLEMENT_REQUEST, SettlementRequest: &common.SettlementRequest{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)}}
			payload, marshalErr := proto.Marshal(request)
			if marshalErr == nil {
				err = helpers.WriteDelimitedMessage(stream, payload)
			}
			if err == nil {
				responseData, readErr := helpers.ReadDelimitedMessage(stream, ctx)
				if readErr == nil {
					response := &common.QueryMessage{}
					if proto.Unmarshal(responseData, response) == nil && response.SettlementResult != nil {
						results = append(results, response.SettlementResult)
					}
				}
			}
			stream.Close()
		}
		cancel()
	}
	logger.Infof("Settlement phase=broadcast_collect duration_ms=%.3f responding_peers=%d", time.Since(broadcastStart).Seconds()*1000, len(results))

	writeStart := time.Now()
	for _, result := range results {
		summary := data.NewGraphNode()
		summary.SetAttr("key", fmt.Sprintf("settlement-%s-%d-%d", result.PeerId, from.Unix(), to.Unix()))
		summary.SetAttr("kind", "SettlementSummary")
		summary.SetAttr("member", result.PeerId)
		summary.SetAttr("from", from.Format(time.RFC3339))
		summary.SetAttr("to", to.Format(time.RFC3339))
		summary.SetAttr("meterReadingSum", result.MeterReadingSum)
		summary.SetAttr("tradeVolumeSum", result.TradeVolumeSum)
		if err := gm.StoreNode("main", summary); err != nil {
			return fmt.Errorf("storing settlement summary: %v", err)
		}
	}
	logger.Infof("Settlement phase=write_summaries duration_ms=%.3f", time.Since(writeStart).Seconds()*1000)
	logger.Infof("Settlement phase=total duration_ms=%.3f", time.Since(totalStart).Seconds()*1000)
	logger.Infof("Compiled %d settlement summaries", len(results))
	return nil
}

func sumColumn(rows [][]interface{}, header []string, name string) float64 {
	index := -1
	for position, label := range header {
		if strings.EqualFold(label, name) {
			index = position
			break
		}
	}
	if index < 0 {
		return 0
	}
	var total float64
	for _, row := range rows {
		if index < len(row) {
			value, err := strconv.ParseFloat(fmt.Sprint(row[index]), 64)
			if err == nil {
				total += value
			}
		}
	}
	return total
}

// Function to update the query state in the graph database
func UpdateQueryState(msg *common.QueryMessage, state common.QueryState_State, gm *graph.Manager) {
	if msg == nil || strings.TrimSpace(msg.Uqid) == "" {
		logger.Warn("Skipping query state update for empty UQI")
		return
	}
	if gm == nil {
		logger.Warnf("Skipping query state update for %s because graph manager is nil", msg.Uqid)
		return
	}
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
	if msg == nil || strings.TrimSpace(msg.Uqid) == "" {
		logger.Warn("Skipping result storage for empty query UQI")
		return
	}
	if gm == nil {
		logger.Warnf("Skipping result storage for %s because graph manager is nil", msg.Uqid)
		return
	}
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
