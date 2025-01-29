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
10. The client should be able to create a DHT, bootstrap the DHT.
12. The client should be able to create, write and read a stream to the server.

Implementation:
1. The client connects to the server using the server's multiaddress.
2. The client sends queries to the server.
3. The client receives responses from the server.
4. The client exits gracefully when the user types 'exit'.
5. The client discovers and connects to peers in the network.
6. The client refreshes the routing table and periodically finds and connects to peers.

Usage:
1. Run the server using the command: go run idss_main.go
2. Run the client using the command: go run idss_client.go -s <server_multiaddress>
3. Enter your query or 'exit' to quit.
4. The client will send the query to the server and print the response.
5. The client will continue to send queries until the user types 'exit'.
===================================
*/

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/xml"
	"errors"
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

const (
	IDSS_PROTOCOL       = protocol.ID("/idss/1.0.0") // The protocol string for the IDSS protocol
	discoveryServiceTag = "kadProtocol"              // The tag used to advertise and discover the IDSS service
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
	flag.Parse()

	if *serverAddrFlag == "" {
		log.Fatal("Please provide the server's multiaddress using the -s flag.")
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
	kademliaDHT, err := dht.New(ctx, host) // can be set to client mode, however, it comes with limitations
	if err != nil {
		log.Fatal("Error creating DHT:", err)
	}

	// Bootstrapping the DHT on the client is important for it to participate in the network
	if err := kademliaDHT.Bootstrap(ctx); err != nil {
		log.Fatal("Error bootstrapping DHT:", err)
	}

	// Handle termination signals
	handleTerminationSignals(host)
	reader := bufio.NewReader(os.Stdin)

	// Extract the peer ID from the server multiaddress 
	// Server address must be supplied as a flag during client startup
	serverMultiaddr := *serverAddrFlag
	serverPeer, err := multiaddr.NewMultiaddr(serverMultiaddr)
	if err != nil {
		log.Error("Error creating server multiaddress:", err)
	}

	// Split the multiaddress to find the peer ID component
	_, peerID := peer.SplitAddr(serverPeer)
	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{serverPeer},
	}

	if peerID == "" {
		log.Fatal("No peer ID found in the multiaddress")
	}
	// A go routine to refresh the routing table and periodically find and connect to peers
	discoverAndConnectPeers(ctx, host, kademliaDHT, addrInfo)

	// get server peer info
	serverPeerInfo, err := peer.AddrInfoFromP2pAddr(multiaddr.StringCast(*serverAddrFlag))
	CheckError(err, "Error creating multiaddress") // Handle potential errors

	// Loop to continouesly accept queries from the user or exit command
	for {
		fmt.Println("\n\nEnter your query and TTL or 'exit' to quit: \nUse comma to separate query and TTL")
		fmt.Print("-> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Error("Error reading query from user:", err)
			continue
		}
		if strings.TrimSpace(input) == "exit" {
			log.Warning("Client exiting...")
			break
		}

		// Split the input into query and TTL
		parts := strings.Split(input, ",")
		query := strings.TrimSpace(parts[0])
		var ttl float64

		if len(parts) == 2{
			ttlStr := strings.TrimSpace(parts[1])
			ttl, err = strconv.ParseFloat(ttlStr, 64)
			if err != nil {
				log.Error("Invalid TTL value:", err)
				continue
			}
		}else{
			ttl = 1 // default TTL
		}
		
		// Define UQI
		uqi := fmt.Sprintf("%s-%d", host.ID(), time.Now().UnixNano())

		// Create the QueryMassage
		msg := common.QueryMessage{
			Uqid:       uqi,
			Query:      query,
			Ttl:        float32(ttl),
			Timestamp: 	timestamppb.New(time.Now()).String(),
			Originator: serverPeer.String(), // store the address to which the client is connecting
			Type:       common.MessageType_QUERY,
			Sender:     host.ID().String(), // This has to be updated in each hop at the server to allow results merging, or else, rely on streams.
		}

		// Open stream
		stream, err := host.NewStream(ctx, serverPeerInfo.ID, IDSS_PROTOCOL)
		if err != nil {
			log.Error("Error opening stream:", err)
			continue
		}
		defer stream.Close()
		log.Info("Stream opened.")

		// Marshal and send the QueryMessage using Protobuf
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

		// Read and print response using Protobuf
		responseBytes, err := readDelimitedMessage(stream)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				log.Error("Timeout reading response from server:", err)
			} else {
				log.Error("Error reading response from server:", err)
			}
			continue
		}

		timeTaken := time.Since(startTime)

		var response common.QueryMessage
		err = proto.Unmarshal(responseBytes, &response)
		if err != nil {
			log.Error("Error unmarshalling response:", err)
			continue
		}

		// Print the response
		if response.Error != "" {
			log.Error("Error in response:", response.Error)
		} else {
			log.Infoln("\n----------- Response -----------")
			log.Infof("Got %d records", response.RecordCount)
			log.Infof("Time spent: %s", timeTaken)
			// Create the XML structure
			queryResponse := QueryResponse{
				ResultCount: int(response.RecordCount),
			}
			//header := response.Result[0].Data
			//fmt.Println(header) // Print the header
			for _, result := range response.Result {
				queryResponse.Results = append(queryResponse.Results, Result{Data: strings.Join(result.Data, ",")})
			}

			// Marshal the XML structure
			xmlResponse, err := xml.MarshalIndent(queryResponse, "", "    ")
			if err != nil {
				log.Error("Error marshalling XML response:", err)
				return
			}

			// Ensure the results directory exists
			resultsDir := "results"
			err = os.MkdirAll(resultsDir, os.ModePerm)
			if err != nil {
				log.Fatal("Error creating results directory:", err)
				return
			}

			// Save the XML data to a file 
			filename := filepath.Join(resultsDir, fmt.Sprintf("%s.xml", uqi))
			err = os.WriteFile(filename, xmlResponse, 0644)
			if err != nil {
				log.Error("Error writing XML response to file:", err)
				return
			}

			log.Info("Response saved to file: ", filename)
			log.Info("Results will be deleted when the client exits.") // valid for simulation purposes
		}
	}
}

func writeDelimitedMessage(w io.Writer, data []byte) error {
	// Write the message size
	sizeBuf := make([]byte, binary.MaxVarintLen64)
	size := binary.PutUvarint(sizeBuf, uint64(len(data)))

	_, err := w.Write(sizeBuf[:size])
	if err != nil {
		return fmt.Errorf("failed to write message size: %w", err)
	}

	// Write the message data
	_, err = w.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write message data: %w", err)
	}
	return err
}

// Function to read a delimited message from a stream
func readDelimitedMessage(r io.Reader) ([]byte, error) {
	// Read the message size
	size, err := binary.ReadUvarint(bufio.NewReader(r))
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}

	// Read the message data
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

		// Clean results directory
		err := os.RemoveAll("results") // valid for simulation purposes
		if err != nil {
			log.Error("Error cleaning up results directory:", err)
		}

		// Clean up
		if err := host.Close(); err != nil {
			log.Error("Error shutting down IDSS: ", err)
			os.Exit(1) 
		}
		os.Exit(0) // normal termination
	}()
}

func discoverAndConnectPeers(ctx context.Context, host host.Host, kademliaDHT *dht.IpfsDHT, addrInfo peer.AddrInfo) {
	log.Info("Client is discovering and connecting to server...")

	routingDiscovery := drouting.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, discoveryServiceTag)

	peerChan, err := routingDiscovery.FindPeers(ctx, discoveryServiceTag)
	if err != nil {
		log.Error("Error finding peers:", err)
		return
	}

	// Check peerChan if there are any peers
	if peerChan == nil {
		log.Warning("No peers found.")
		return
	}

	// Only attempt to connect to the serverPeer
	err = host.Connect(ctx, addrInfo)
	if err != nil {
		log.Error("Error connecting to peer:", err)
		os.Exit(1) // Exit if connection fails
		return
	} else {
		log.Info("Connected to server: ", addrInfo.ID.String())
	}
}

// Function to check for errors
func CheckError(err error, message ...string) {
	if err != nil {
		log.Fatalf("Error during operation '%s': %v", message, err)
	}
}
