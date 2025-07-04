/*
 * ! \file helper.go
 * Helper functions for IDSS. Contains functions to parse queries, apply WITH clauses, 
 * and manipulation of query results.
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 *
 */

package helpers

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	_ "net/http/pprof"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"fmt"
	"idss/graphdb/common"

	"github.com/ipfs/go-log/v2"
)

var logger = log.Logger("IDSS")

type OverlayInfo struct {
	MyPeerID string   `json:"my_peer_id"`
	NumPeers int      `json:"num_peers"`
	PeerIDs  []string `json:"peer_ids"`
	BootstrapPeers []string `json:"bootstrap_peers"`
	ActiveConnections int `json:"active_connections"`
	DroppedPeers []string `json:"dropped_peers"`
	NumDropPeers int `json:"num_drop_peers"`
	ConnectedPeers []string `json:"connected_peers"`
}

// Function to gather overlay information
func GetOverlayInfo(kadDHT *dht.IpfsDHT) OverlayInfo {
	bootstrapPeers := kadDHT.BootstrapPeers()
	peers := kadDHT.Host().Network().Peers()
	activeConnections := kadDHT.Host().Network().Conns()
	droppedPeers := kadDHT.DroppedPeers()
	return OverlayInfo{
		MyPeerID: kadDHT.Host().ID().String(),
		NumPeers: len(peers),
		PeerIDs:  peerIDs(peers),
		BootstrapPeers: peerIDs(bootstrapPeers.([]peer.ID)),
		ActiveConnections: len(activeConnections),
		ConnectedPeers: peerIDs(peers),
		DroppedPeers: peerIDs(droppedPeers.([]peer.ID)),
		NumDropPeers: len(droppedPeers.([]peer.ID)),
	}
}

func peerIDs(peers []peer.ID) []string {
	var ids []string
	for _, p := range peers {
		ids = append(ids, p.String())
	}
	return ids
}

// Function to parse the query string and extract the aggregates
func ParseAggregates(query string) []common.AggregateInfo {
    var aggregates []common.AggregateInfo
    re := regexp.MustCompile(
        `@(sum|avg|max|min)\(([^)]+)\)`,
    )

    matches := re.FindAllStringSubmatch(query, -1)
    for _, m := range matches {
        if len(m) < 3 {
            continue
        }

        parts := strings.SplitN(m[2], " where ", 2)
        traversal := parts[0]
        filter := ""
        if len(parts) > 1 {
            filter = parts[1]
        }

		// Default comparison and threshold for aggregates without explicit conditions
        comparison := ">"
        threshold := 0.0

		// Check if the query contains a comparison and threshold
        comparisonRe := regexp.MustCompile(`\s*(>|<|>=|<=|==|!=)\s*([\d\.]+)`)
        comparisonMatches := comparisonRe.FindStringSubmatch(query)
        if len(comparisonMatches) == 3 {
            comparison = comparisonMatches[1]
            threshold, _ = strconv.ParseFloat(comparisonMatches[2], 64)
        }

        aggregates = append(aggregates, common.AggregateInfo{
            Function:   m[1],
            Traversal:  traversal,
            Filter:     filter,
            Comparison: comparison,
            Attribute:  traversal, // USe the content inside the aggregate function as the attribute
            Threshold:  threshold,
        })
    }

	if len(aggregates) > 0 {
		logger.Infof("First aggregate details: Function: %s, Traversal: %s, Filter: %s, Comparison: %s, Attribute: %s, Threshold: %f", 
			aggregates[0].Function, 
			aggregates[0].Traversal, 
			aggregates[0].Filter, 
			aggregates[0].Comparison, 
			aggregates[0].Attribute, 
			aggregates[0].Threshold) // Debugging logs
	}
    return aggregates
}

// Support for WITH operations
func ParseWithClauses(query string) []string {
    re := regexp.MustCompile(`(?i)\bwith\s+(.*?)(?:\s*;|\s*$)`)
    matches := re.FindStringSubmatch(query)
    if len(matches) < 2 {
        return nil
    }
    return strings.Split(matches[1], ",")
}

func ApplyWithClauses(results [][]interface{}, header [] string, clauses []string) [][]interface{} {
    for _, clause := range clauses {
        clause = strings.TrimSpace(clause)
        if strings.HasPrefix(clause, "ordering(") {
            ApplyOrdering(results, header, clause)
        }
    }
    return results
}

// Function to apply ordering to the results. This must be called after the query has been 
// executed and results are available in the results slice of the initiator peer
func ApplyOrdering(results [][]interface{}, header [] string, clause string) [][]interface{} {
    if len(results) < 2 {
        return results
    }

	// Assume the header is the first row (as []interface{}).
	// Convert it to []string.
	for _, v := range results[0] {
		header = append(header, fmt.Sprintf("%v", v))
	}

    // Improved regex to capture order and column
    re := regexp.MustCompile(`(?i)ordering\s*\(\s*(asc|desc|ascending|descending)\s+([\w:]+)\s*\)`)
    matches := re.FindStringSubmatch(clause)
    if len(matches) < 3 {
		logger.Warn("Invalid ordering clause.")
        return results // Invalid clause
    }

	orderType := strings.ToLower(matches[1])
    targetCol := strings.ToLower(matches[2])
    if strings.Contains(targetCol, ":") {
        targetCol = strings.Split(targetCol, ":")[1]
    }

    // Find column index using cleaned headers
    colIndex := -1
    for i, h := range header {
        // Optionally, split on colon or space if your header includes prefixes.
		parts := strings.FieldsFunc(strings.ToLower(h), func(r rune) bool { return r == ':' || r == ' ' })
		if len(parts) > 0 && parts[len(parts)-1] == targetCol {
			colIndex = i
			break
		}
    }
	
    // If the header is missing assume column index is provided in the query
    if colIndex == -1 {
        logger.Warnf("Column '%s' not found in headers: %v", targetCol, header)
        return results
    }

	dataRows := results[1:] // Exclude the header


	// Sort data rows (excluding header)
    sort.Slice(dataRows, func(i, j int) bool {
		a := fmt.Sprintf("%v", dataRows[i][colIndex])
        b := fmt.Sprintf("%v", dataRows[j][colIndex])

        if orderType == "ascending" {
            return a < b
        }
        return a > b
    })
	// Reassemble header + sorted data
    return append([][]interface{}{results[0]}, dataRows...)
}

// Helper to determine if a row is the header.
func IsHeaderRow(row []interface{}, header []string) bool {
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

func IsMetadataRow(val interface{}) bool {
    if s, ok := val.(string); ok {
        return strings.Contains(s, ":")
    }
    return false
}

// Function to convert EliasDB query results to Protobuf rows
func CovertResultToProtobufRows(result [][]interface{}, header []string) []*common.Row {
	var rows []*common.Row

	logger.Infof("Received header in convertion: %v", header) //TODO: Prints nothing. Why?

	// Send headers first as a special row, if available
    if len(header) > 0 {
        rows = append(rows, &common.Row{Data: header})
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

func ComputeAggregate(rows [][]interface{}, header []string, attribute string) (sum float64, count float64, min float64, max float64, err error) {
	// Find the column index for the attribute (case-insensitive).
	idx := -1
	for i, h := range header {
		if strings.EqualFold(h, attribute) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return 0, 0, 0, 0, fmt.Errorf("attribute %s not found in header %v", attribute, header)
	}
	var values []float64
	// Extract valid values for the attribute
	for _, row := range rows {
		var v float64
		switch val := row[idx].(type) {
		case float64:
			v = val
		case int:
			v = float64(val)
		case string:
			v, err = strconv.ParseFloat(val, 64)
			if err != nil {
				continue
			}
		default:
			continue
		}
		values = append(values, v)
	}
	if len(values) == 0 {
		return 0, 0, 0, 0, fmt.Errorf("no valid values for attribute %s", attribute)
	}

	// Compute the aggregate values
	sum = 0
	count = float64(len(values))
	min = values[0]
	max = values[0]
	for _, v := range values {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return sum, count, min, max, nil
}

// Function to convert Protobuf rows to EliasDB query results
func ConvertProtobufRowsToResult(rows []*common.Row) [][]interface{} {
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

// Function to extract the node kind from the query
func ExtractNodeKind(query string) (string) {
    // Assumes the query starts with "get <nodeKind>"
    parts := strings.Fields(query)
    if len(parts) >= 2 {
        return parts[1]
    }
    return ""
}


// Function to send an error message to the client
func SendErrorMessage(conn network.Stream, remotePeerID peer.ID, errorMsg string) {
    logger.Errorf("Sending error message to %s: %s", remotePeerID, errorMsg)
    _, err := conn.Write([]byte(errorMsg))
    if err != nil {
        logger.Errorf("Error sending error message: %v", err)
    }
}

// Function to send the merged result to the client
func SendMergedResult(parentStream network.Stream, parentPeerD peer.ID,   result [][]interface{}, headers[] string,kadDHT *dht.IpfsDHT) {
	var wg sync.WaitGroup
	defer parentStream.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Infof("Sending merged result to peer %s", parentStream.Conn().RemotePeer()) 
		recordCount := len(result)
		logger.Info("Record count: ", recordCount)
		resultRows := CovertResultToProtobufRows(result, headers)

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

		if err := WriteDelimitedMessage(parentStream, resultBytes); err != nil {
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

// Function to merge local and remote results
func MergeTwoResults(localResults, remoteResults [][]interface{}, header []string) [][]interface{} {
	merged := [][]interface{}{make([]interface{}, len(header))}
	for i, h := range header {
		merged[0][i] = h
	}
    seen := make(map[string]bool)
    
    addRows := func(rows [][]interface{}) {
        for _, row := range rows {
			if IsHeaderRow(row, header) { continue }
            if len(row) != len(header) { continue }

            key := ""
            for _, v := range row {
                key += fmt.Sprintf("%v|", v)
            }
            if !seen[key] {
                merged = append(merged, row)
                seen[key] = true
            }
        }
    }
    
	addRows(localResults)
	addRows(remoteResults)

	// remove duplicates.
	unique := make(map[string]bool)
	final := [][]interface{}{merged[0]}
	for _, row := range merged[1:] {
		key := fmt.Sprintf("%v", row)
		if !unique[key] {
			unique[key] = true
			final = append(final, row)
		}
	}
    return merged 
}

// Function to read a delimited message from the stream
func ReadDelimitedMessage(r io.Reader, ctx context.Context) ([]byte, error) {
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

// Function to write a delimited message to the stream
func WriteDelimitedMessage(w io.Writer, data []byte) error {
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

// Function to send partial aggregate results to the parent peer
func SendPartialAggregateResult(conn network.Stream, agg common.AggregateInfo, sum float64, count float64, kadDHT *dht.IpfsDHT) {
    var data []string
    if agg.Function == "avg" {
        // For avg, send both sum and count.
        data = []string{fmt.Sprintf("%f", sum), fmt.Sprintf("%f", count)}
    } else {
        // For sum, min, or max, only one value is needed.
        data = []string{fmt.Sprintf("%f", sum)}
    }
    resultMsg := &common.QueryMessage{
        Type:        common.MessageType_RESULT,
        Result:      []*common.Row{{Data: data}},
        RecordCount: 1,
    }
    resultBytes, err := proto.Marshal(resultMsg)
    if err != nil {
        logger.Errorf("Error marshalling partial aggregate result: %v in Peer: %s", err, kadDHT.Host().ID())
        return
    }
    WriteDelimitedMessage(conn, resultBytes)
}