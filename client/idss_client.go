package main

/*
===================================
=== IDSS Client Implementation ===
Requirements:
1. The client should be able to connect to the server using the server's multiaddress.
2. The client should be able to send queries to the server.
3. The client should be able to receive responses from the server.
4. The client should be able to exit gracefully.
5. The client should be able to discover and connect to peers in the network.
6. The client should be able to refresh the routing table and periodically find and connect to peers.
7. The client should be able to handle termination signals.
8. The client should be able to generate a key pair for the host, create libp2p host.
10. The client should be able to create, write and read a stream to the server.

Implementation:
1. The client connects to the server using the server's multiaddress.
2. The client sends queries to the server.
3. The client receives responses from the server.
4. The client exits gracefully when the user types 'exit'.
5. The client discovers and connects to peers in the network.
6. The client refreshes the routing table and periodically finds and connects to peers.

Usage:
1. More instructions will appear on the terminal when the client is run.
2. Run the client using the command: go run idss_client.go -s <server_multiaddress>
3. Enter your query or 'exit' to quit. The client will send the query to the server and print the response.
4. The client will continue to send queries until the user types 'exit'.
===================================
*/

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"idss/graphdb/common"
	"idss/graphdb/identity"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"google.golang.org/protobuf/proto"

	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
)

var (
	log = logrus.New()
)

type Result struct {
	Data []string `json:"data"`
}

type QueryResponse struct {
	Uqid        string          `json:"uqid,omitempty"`
	Query       string          `json:"query,omitempty"`
	ElapsedMs   float64         `json:"elapsedMs"`
	WireBytes   int             `json:"wireBytesReceived"`
	ResultCount int             `json:"resultCount"`
	Header      []string        `json:"header,omitempty"`
	Rows        [][]string      `json:"rows"`
	Manifest    []ManifestEntry `json:"manifest,omitempty"`
}

// ManifestEntry is one data partition's status for a grouped (@group) query.
type ManifestEntry struct {
	Partition string `json:"partition"`
	Peer      string `json:"peer"`
	Status    string `json:"status"`
	Detail    string `json:"detail,omitempty"`
}

// splitQueryAndTTL separates an optional trailing ",<ttl>" from the query. Only
// the last comma is considered, and only when what follows is a number, so
// queries containing commas (show clauses, @group(keys=0,1; ...)) stay intact.
func splitQueryAndTTL(input string) (string, float64, error) {
	input = strings.TrimSpace(input)
	if i := strings.LastIndex(input, ","); i >= 0 {
		tail := strings.TrimSpace(input[i+1:])
		if ttl, err := strconv.ParseFloat(tail, 64); err == nil {
			if ttl <= 0 {
				return "", 0, fmt.Errorf("TTL must be positive, got %v", ttl)
			}
			return strings.TrimSpace(input[:i]), ttl, nil
		}
	}
	return input, 1, nil // default TTL
}

// loadExpectedPartitions reads the -expected option: a generator manifest.json
// (its "partitions" keys) or a comma-separated list of partition labels.
func loadExpectedPartitions(spec string) ([]string, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	if b, err := os.ReadFile(spec); err == nil {
		var m struct {
			Partitions map[string]json.RawMessage `json:"partitions"`
		}
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("reading %s: %v", spec, err)
		}
		out := make([]string, 0, len(m.Partitions))
		for name := range m.Partitions {
			out = append(out, name)
		}
		sort.Strings(out)
		return out, nil
	}
	var out []string
	for _, p := range strings.Split(spec, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out, nil
}

// completeManifest adds an UNREACHABLE entry for every expected partition that
// did not report at all, so that partitions nobody reached are never silent.
func completeManifest(m []ManifestEntry, expected []string) []ManifestEntry {
	seen := map[string]bool{}
	for _, e := range m {
		seen[e.Partition] = true
	}
	for _, p := range expected {
		if !seen[p] {
			m = append(m, ManifestEntry{Partition: p, Status: "UNREACHABLE", Detail: "not reached"})
		}
	}
	return m
}

func manifestSummary(m []ManifestEntry) string {
	byStatus := map[string][]string{}
	for _, e := range m {
		byStatus[e.Status] = append(byStatus[e.Status], e.Partition)
	}
	missing := []string{}
	for _, st := range []string{"DENIED", "TIMEOUT", "UNREACHABLE", "ERROR"} {
		if ids := byStatus[st]; len(ids) > 0 {
			missing = append(missing, fmt.Sprintf("%s %v", strings.ToLower(st), ids))
		}
	}
	s := fmt.Sprintf("answered %d/%d", len(byStatus["ANSWERED"]), len(m))
	if len(missing) > 0 {
		s += "; " + strings.Join(missing, "; ")
	}
	return s
}

func main() {
	// Define flag
	serverAddrFlag := flag.String("s", "", "Server multiaddress")
	IDSSProtocol := flag.String("p", "/lan/kad/1.0.0", "IDSS protocol") // By default use the local network protocol
	requesterRole := flag.String("role", "member", "Requester role: member, manager, or observer")
	keyFlag := flag.String("key", "", "Requester key file (created if missing); gives this client a stable peer ID that DSO policies can list")
	printID := flag.Bool("print-id", false, "Print this client's peer ID (from -key) and exit")
	expectedFlag := flag.String("expected", "", "Grouped queries: partitions expected to answer, as a comma-separated list or a generator manifest.json")
	flag.Parse()

	if *serverAddrFlag == "" && !*printID {
		log.Fatal("Please provide the server's multiaddress using the -s flag.")
	}

	expectedPartitions, err := loadExpectedPartitions(*expectedFlag)
	if err != nil {
		log.Fatal("Invalid -expected: ", err)
	}

	if *IDSSProtocol == "global" {
		*IDSSProtocol = "/kad/1.0.0"
	}

	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Generate key pair for the server/host
	var priv crypto.PrivKey
	if *keyFlag != "" {
		priv, err = identity.LoadOrCreateKey(*keyFlag)
		if err != nil {
			log.Fatal("Error loading requester key:", err)
		}
	} else {
		priv, _, err = crypto.GenerateKeyPair(crypto.RSA, 2048)
		if err != nil {
			log.Fatal("Error generating RSA key pair:", err)
		}
	}
	if *printID {
		id, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id.String())
		return
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
		libp2p.Security(noise.ID, noise.New), // Ensure secure communication
	)
	if err != nil {
		panic(err)
	}
	defer host.Close()

	// Print the client's listening addresses
	for _, addr := range host.Addrs() {
		completePeerAddr := addr.Encapsulate(multiaddr.StringCast("/p2p/" + host.ID().String()))
		log.Info("Client listening on: \n", completePeerAddr)
	}

	// Initialize the DHT
	kademliaDHT, err := dht.New(host)
	if err != nil {
		log.Fatal("Error creating DHT:", err)
	}

	// Bootstrap the DHT
	if err := kademliaDHT.Bootstrap(ctx); err != nil {
		log.Fatal("Error bootstrapping DHT:", err)
	}

	// Handle termination signals
	handleTerminationSignals(host)
	reader := bufio.NewReader(os.Stdin)

	// Extract the peer ID from the server multiaddress
	serverMultiaddr := *serverAddrFlag
	serverPeer, err := multiaddr.NewMultiaddr(serverMultiaddr)
	if err != nil {
		log.Error("Error creating server multiaddress:", err)
	}

	_, peerID := peer.SplitAddr(serverPeer)
	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{serverPeer},
	}

	if peerID == "" {
		log.Fatal("No peer ID found in the multiaddress")
	}

	// Discover and connect to peers
	discoverAndConnectPeers(ctx, host, kademliaDHT, addrInfo, *IDSSProtocol)

	// Get server peer info
	serverPeerInfo, err := peer.AddrInfoFromP2pAddr(multiaddr.StringCast(*serverAddrFlag))
	CheckError(err, "Error creating multiaddress")

	// Continuous query loop
	for {
		fmt.Println("\n\nEnter your query and TTL or 'exit' to quit: \nUse comma to separate query and TTL")
		fmt.Println("Append '-l' or '-local' for local execution, or use 'add', 'update', 'delete' for modifications.")
		fmt.Println("Long-running queries: 'submit <query>, <ttl>' returns at once; 'result <uqid>' fetches the result or its state.")
		fmt.Print("-> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Error("Error reading query from user:", err)
			continue
		}
		input = strings.TrimSpace(input)
		if input == "exit" {
			log.Warning("Client exiting...")
			break
		}
		// Long-running queries: "submit <query>, <ttl>" sends the query without
		// waiting (the initiator peer stores the result); "result <uqid>" fetches
		// it, or its current state, from the same peer later.
		detach := false
		if strings.HasPrefix(input, "submit ") {
			detach, input = true, strings.TrimSpace(strings.TrimPrefix(input, "submit "))
		}
		if f := strings.Fields(input); len(f) == 2 && f[0] == "result" {
			input = "@result " + f[1] + ", 10"
		}

		// Split the input into query and TTL
		query, ttl, err := splitQueryAndTTL(input)
		if err != nil {
			log.Error("Invalid TTL value:", err)
			continue
		}

		// Define UQI
		uqi := fmt.Sprintf("%s-%d", host.ID(), time.Now().UnixNano())

		// Create the QueryMessage
		msg := common.QueryMessage{
			Uqid:          uqi,
			Query:         query,
			Ttl:           float32(ttl),
			Timestamp:     time.Now().Add(time.Duration(float64(ttl) * float64(time.Second))).UTC().Format(time.RFC3339Nano),
			Originator:    serverPeer.String(),
			Type:          common.MessageType_QUERY,
			Sender:        host.ID().String(),
			RequesterId:   host.ID().String(),
			RequesterRole: *requesterRole,
		}

		// Open stream
		stream, err := host.NewStream(ctx, serverPeerInfo.ID, protocol.ID(*IDSSProtocol))
		if err != nil {
			log.Error("Error opening stream:", err)
			continue
		}
		defer stream.Close()
		log.Info("Stream opened.")

		// Marshal and send the QueryMessage
		// Sign the query so that every peer can verify who asked (Green Power
		// peers deny unsigned or tampered queries; energy-community peers ignore it).
		if err := identity.Sign(&msg, priv); err != nil {
			log.Error("Error signing query:", err)
			continue
		}
		msgBytes, err := proto.Marshal(&msg)
		if err != nil {
			log.Error("Error marshalling query message:", err)
			continue
		}

		startTime := time.Now()
		err = writeDelimitedMessage(stream, msgBytes)
		if err != nil {
			log.Errorf("Error writing to stream: %s", err)
			continue
		}

		log.Infoln("Query sent to server.")
		log.Infoln("UQI: ", uqi)
		if detach {
			log.Infof("Detached. Fetch the result later from the same peer with: result %s", uqi)
			stream.Close()
			continue
		}

		// Read and accumulate responses in loop to handle chunks
		var allRows [][]string
		var header []string
		var errorMsg string
		var respondingPeerCount int
		var totalWireBytes int
		var manifest []ManifestEntry
		var lastProgress string
		resultUqid, resultQuery := uqi, query

		for {
			responseBytes, wireSize, err := readDelimitedMessage(stream)
			totalWireBytes += wireSize

			if err != nil {
				if err == io.EOF || strings.Contains(err.Error(), "EOF") {
					break // All chunks received
				}
				log.Error("Error reading response chunk from server:", err)
				break
			}

			var part common.QueryMessage
			err = proto.Unmarshal(responseBytes, &part)
			if err != nil {
				log.Error("Error unmarshalling response chunk:", err)
				break
			}

			if part.Error != "" {
				errorMsg = part.Error // Capture error for FAILED state
				break
			}
			if part.Type == common.MessageType_RESULT && part.RecordCount == -1 { // progress heartbeat
				if st := part.GetState().GetState().String(); st != lastProgress {
					lastProgress = st
					log.Infof("Progress: %s (%s)", st, time.Since(startTime).Round(time.Millisecond))
				}
				continue
			}
			if part.Uqid != "" && part.Query != "" { // a fetched stored result names the original query
				resultUqid, resultQuery = part.Uqid, part.Query
			}
			for _, e := range part.PartitionPartials {
				manifest = append(manifest, ManifestEntry{Partition: e.PartitionId, Peer: e.PeerId, Status: e.Status.String(), Detail: e.Detail})
			}
			if len(part.RespondingPeerIds) > respondingPeerCount {
				respondingPeerCount = len(part.RespondingPeerIds)
			}

			// Accumulate rows, extract header from first chunk if present
			if len(header) == 0 && len(part.Labels) > 0 {
				header = part.Labels // grouped (@group) results carry their column names here
			}
			for _, row := range part.Result {
				data := row.Data
				if len(header) == 0 && isHeaderRow(data) { // Assume first non-empty is header
					header = data
					continue // Skip adding header multiple times
				}
				allRows = append(allRows, data)
			}

			if part.Type == common.MessageType_RESULT && part.RecordCount >= 0 {
				break
			}
		}

		// Dedup any stray headers (best-effort)
		allRows = dedupRows(allRows, header)

		// Process as before, but with allRows and header
		timeTaken := time.Since(startTime)
		if errorMsg != "" {
			log.Error("Error in response:", errorMsg)
		} else {
			log.Infoln("\n----------- Response -----------")
			dataNonHeaders := len(allRows) // Exclude header from count
			log.Infof("Got %d records", dataNonHeaders)
			log.Infof("Responding peers: %d", respondingPeerCount)
			log.Infof("Wire bytes received: %d", totalWireBytes)
			log.Infof("Time spent: %s", timeTaken)
			if len(manifest) > 0 || len(expectedPartitions) > 0 {
				manifest = completeManifest(manifest, expectedPartitions)
				log.Infof("Partitions: %s", manifestSummary(manifest))
			}

			// Check if it's a status message or query result
			queryResponse := QueryResponse{Uqid: resultUqid, Query: resultQuery, ElapsedMs: float64(timeTaken.Microseconds()) / 1000,
				WireBytes: totalWireBytes, ResultCount: dataNonHeaders, Header: header, Rows: allRows, Manifest: manifest}
			isStatus := len(allRows) == 1 && allRows[0][0] == "Status" // Adjusted for accumulated rows

			if isStatus {
				// Handle status message
				statusMsg := strings.Join(queryResponse.Rows[0], " ")
				log.Infof("Server response: %s", statusMsg)
			} else {
				// Handle query result and save structured JSON.
				jsonResponse, err := json.MarshalIndent(queryResponse, "", "    ")
				if err != nil {
					log.Error("Error marshalling JSON response:", err)
					continue
				}

				resultsDir := os.Getenv("IDSS_CLIENT_RESULTS_DIR")
				if resultsDir == "" {
					resultsDir = "results"
				}
				err = os.MkdirAll(resultsDir, os.ModePerm)
				if err != nil {
					log.Error("Error creating results directory:", err)
					continue
				}

				filename := filepath.Join(resultsDir, fmt.Sprintf("%s.json", resultUqid))
				err = os.WriteFile(filename, jsonResponse, 0644)
				if err != nil {
					log.Error("Error writing JSON response to file:", err)
					continue
				}

				log.Info("Response saved to file: ", filename)
				log.Info("Results will be deleted when the client exits.")
			}
		}
	}
}

func writeDelimitedMessage(w io.Writer, data []byte) error {
	data = encodeFrame(data)
	sizeBuf := make([]byte, binary.MaxVarintLen64)
	size := binary.PutUvarint(sizeBuf, uint64(len(data)))

	_, err := w.Write(sizeBuf[:size])
	if err != nil {
		return fmt.Errorf("failed to write message size: %w", err)
	}

	_, err = w.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write message data: %w", err)
	}
	return err
}

// readDelimitedMessage returns the decoded payload plus the wire size of the frame
// (pre-decompression) so callers can measure bytes actually transferred.
func readDelimitedMessage(r io.Reader) ([]byte, int, error) {
	var size uint64
	for shift := uint(0); ; shift += 7 {
		if shift >= 64 {
			return nil, 0, fmt.Errorf("invalid frame length")
		}
		var one [1]byte
		if _, err := io.ReadFull(r, one[:]); err != nil {
			return nil, 0, err
		}
		size |= uint64(one[0]&0x7f) << shift
		if one[0]&0x80 == 0 {
			break
		}
	}
	if size > 256<<20 {
		return nil, 0, fmt.Errorf("frame too large: %d bytes", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, 0, err
	}
	decoded, err := decodeFrame(buf)
	return decoded, len(buf), err
}

const compressionThreshold = 4096

func encodeFrame(data []byte) []byte {
	if len(data) < compressionThreshold {
		return append([]byte{0}, data...)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(data); err != nil || writer.Close() != nil || compressed.Len() >= len(data) {
		return append([]byte{0}, data...)
	}
	return append([]byte{1}, compressed.Bytes()...)
}

func decodeFrame(frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return frame, nil
	}
	if frame[0] == 0 {
		return frame[1:], nil
	}
	if frame[0] != 1 {
		return frame, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(frame[1:]))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func handleTerminationSignals(host host.Host) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-c
		log.Warnf("Received signal: %s. You are exiting the network.", sig)

		resultsDir := os.Getenv("IDSS_CLIENT_RESULTS_DIR")
		if resultsDir == "" {
			resultsDir = "results"
		}
		err := os.RemoveAll(resultsDir)
		if err != nil {
			log.Error("Error cleaning up results directory:", err)
		}

		if err := host.Close(); err != nil {
			log.Error("Error shutting down IDSS: ", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
}

func discoverAndConnectPeers(ctx context.Context, host host.Host, kademliaDHT *dht.IpfsDHT, addrInfo peer.AddrInfo, IDSSProtocol string) {
	log.Info("Client is discovering and connecting to server...")

	routingDiscovery := drouting.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, common.DISCOVERY_SERVICE)

	peerChan, err := routingDiscovery.FindPeers(ctx, common.DISCOVERY_SERVICE)
	if err != nil {
		log.Error("Error finding peers:", err)
		return
	}

	if peerChan == nil {
		log.Warning("No peers found.")
		return
	}

	err = host.Connect(ctx, addrInfo)
	if err != nil {
		log.Error("Error connecting to peer:", err)
		os.Exit(1)
		return
	} else {
		log.Info("Connected to server: ", addrInfo.ID.String())
		log.Info("Protocol: ", protocol.ID(IDSSProtocol))
	}
}

func CheckError(err error, message ...string) {
	if err != nil {
		log.Fatalf("Error during operation '%s': %v", message, err)
	}
}

// Helper functions for header detection and deduplication
func isHeaderRow(data []string) bool {
	// Simple check: headers don't start with numbers, or match known pattern
	if len(data) > 0 && strings.Contains(data[0], "Key") { // Customize based on schema (e.g., "Client Key")
		return true
	}
	return false
}

func dedupRows(rows [][]string, header []string) [][]string {
	seen := make(map[string]bool)
	var unique [][]string
	headerStr := strings.Join(header, ",")
	for _, row := range rows {
		rowStr := strings.Join(row, ",")
		if rowStr == headerStr {
			continue // Skip extra headers
		}
		if !seen[rowStr] {
			seen[rowStr] = true
			unique = append(unique, row)
		}
	}
	return unique
}
