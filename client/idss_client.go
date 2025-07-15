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
	"context"
	"encoding/binary"
	"encoding/xml"
	"flag"
	"fmt"
	"idss/graphdb/common"
	"io"
	"os"
	"os/signal"
	"path/filepath"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
)

var (
	log = logrus.New()
)

type Result struct {
	XMLName xml.Name `xml:"result"`
	Data    string   `xml:"data"`
}

type QueryResponse struct {
	XMLName     xml.Name `xml:"response"`
	ResultCount int      `xml:"resultCount"`
	Results     []Result `xml:"results"`
}

func main() {
	// Define flag
	serverAddrFlag := flag.String("s", "", "Server multiaddress")
	IDSSProtocol := flag.String("p", "/lan/kad/1.0.0", "IDSS protocol") // By default use the local network protocol
	flag.Parse()

	if *serverAddrFlag == "" {
		log.Fatal("Please provide the server's multiaddress using the -s flag.")
	}

	if *IDSSProtocol == "global" {
		*IDSSProtocol = "/kad/1.0.0"
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
	kademliaDHT, err := dht.New(ctx, host)
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

		// Split the input into query and TTL
		parts := strings.Split(input, ",")
		query := strings.TrimSpace(parts[0])
		var ttl float64 = 1 // Default TTL
		if len(parts) >= 2 {
			ttlStr := strings.TrimSpace(parts[1])
			ttl, err = strconv.ParseFloat(ttlStr, 64)
			if err != nil {
				log.Error("Invalid TTL value:", err)
				continue
			}
		}

		// Define UQI
		uqi := fmt.Sprintf("%s-%d", host.ID(), time.Now().UnixNano())

		// Create the QueryMessage
		msg := common.QueryMessage{
			Uqid:       uqi,
			Query:      query,
			Ttl:        float32(ttl),
			Timestamp:  timestamppb.New(time.Now()).String(),
			Originator: serverPeer.String(),
			Type:       common.MessageType_QUERY,
			Sender:     host.ID().String(),
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

		// Read and accumulate responses in loop to handle chunks
		var allRows [][]string
		var header []string
		var errorMsg string

		for {
			responseBytes, err := readDelimitedMessage(stream)
			
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

			// Accumulate rows, extract header from first chunk if present
			for _, row := range part.Result {
				data := row.Data
				if len(header) == 0 && isHeaderRow(data) { // Assume first non-empty is header
					header = data
					continue // Skip adding header multiple times
				}
				allRows = append(allRows, data)
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
			log.Infof("Time spent: %s", timeTaken)

			// Check if it's a status message or query result
			queryResponse := QueryResponse{ResultCount: dataNonHeaders}
			isStatus := len(allRows) == 1 && allRows[0][0] == "Status" // Adjusted for accumulated rows

			if len(header) > 0 {
				queryResponse.Results = append(queryResponse.Results, Result{Data: strings.Join(header, ",")})
			}

			for _, row := range allRows {
				queryResponse.Results = append(queryResponse.Results, Result{Data: strings.Join(row, ",")})
			}

			if isStatus {
				// Handle status message
				statusMsg := queryResponse.Results[1].Data // Assuming header + status
				log.Infof("Server response: %s", statusMsg)
			} else {
				// Handle query result and save XML
				xmlResponse, err := xml.MarshalIndent(queryResponse, "", "    ")
				if err != nil {
					log.Error("Error marshalling XML response:", err)
					continue
				}

				resultsDir := "results"
				err = os.MkdirAll(resultsDir, os.ModePerm)
				if err != nil {
					log.Error("Error creating results directory:", err)
					continue
				}

				filename := filepath.Join(resultsDir, fmt.Sprintf("%s.xml", uqi))
				err = os.WriteFile(filename, xmlResponse, 0644)
				if err != nil {
					log.Error("Error writing XML response to file:", err)
					continue
				}

				log.Info("Response saved to file: ", filename)
				log.Info("Results will be deleted when the client exits.")
			}
		}
	}
}

func writeDelimitedMessage(w io.Writer, data []byte) error {
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

func readDelimitedMessage(r io.Reader) ([]byte, error) {
	size, err := binary.ReadUvarint(bufio.NewReader(r))
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}

	buf := make([]byte, size)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	return buf, nil
}

func handleTerminationSignals(host host.Host) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-c
		log.Warnf("Received signal: %s. You are exiting the network.", sig)

		err := os.RemoveAll("results")
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