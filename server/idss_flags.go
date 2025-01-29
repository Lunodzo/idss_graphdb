package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"math/big"
	"strings"

	dht "github.com/libp2p/go-libp2p-kad-dht" // This is needed to get the default bootstrap peers
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	maddr "github.com/multiformats/go-multiaddr"
)

// GenerateRandomString generates a random string of the specified length
func GenerateRandomString(length int) (string, error) {
    const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    result := make([]byte, length)
    for i := range result {
        num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
        if err != nil {
            return "", err
        }
        result[i] = charset[num.Int64()]
    }
    return string(result), nil
}

// GenerateIDSSString generates a random string containing the keyword "idss_"
func GenerateIDSSString() (string, error) {
    randomStr, err := GenerateRandomString(1) // Generate a random string of length 5
    if err != nil {
        return "", err
    }
    keyword := "idss_string"
    return keyword + randomStr, nil
}

// CreateLocalBootstrapPeers creates a local bootstrap peer with the specified port
// This is optional because the default bootstrap peers are already provided by the libp2p library
func CreateLocalBootstrapPeers(port int) (peer.AddrInfo, error){
	// gen new keypair
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		return peer.AddrInfo{}, err
	}

	// Generate the peer ID from the public key
	peerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		logger.Fatalf("Failed to generate peer ID: %v", err)
	}
	fmt.Println("Generated peer ID:", peerID)

	ours := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)

	// Create a multiaddress to listen on a specific port
	addr, err := maddr.NewMultiaddr(ours)
    if err != nil {
        return peer.AddrInfo{}, err
    }

	completeAddr := addr.Encapsulate(maddr.StringCast("/p2p/" + peerID.String()))
    return peer.AddrInfo{
        ID:    peerID,
        Addrs: []maddr.Multiaddr{completeAddr},
    }, nil
}

type addrList []maddr.Multiaddr

func (al *addrList) String() string {
	strs := make([]string, len(*al))
	for i, addr := range *al {
		strs[i] = addr.String()
	}
	return strings.Join(strs, ",")
}

func (al *addrList) Set(value string) error {
	addr, err := maddr.NewMultiaddr(value)
	if err != nil {
		return err
	}
	*al = append(*al, addr)
	return nil
}

func StringsToAddrs(addrStrings []string) (maddrs []maddr.Multiaddr, err error) {
	for _, addrString := range addrStrings {
		addr, err := maddr.NewMultiaddr(addrString)
		if err != nil {
			return maddrs, err
		}
		maddrs = append(maddrs, addr)
	}
	return
}

type Config struct {
	IDSSString       string
	BootstrapPeers   addrList
	ListenAddresses  addrList
	ProtocolID       string
	Filename         string
}

func ParseFlags(peerID string) (Config, error) {
	config := Config{}
	flag.StringVar(&config.IDSSString, "IDSS", "idss_stringaaaa",
		"Unique string to identify group of nodes. Share this with peers to let them connect.")
	flag.Var(&config.BootstrapPeers, "peer", "Adds a peer multiaddress to the bootstrap list.")
	flag.Var(&config.ListenAddresses, "listen", "Adds a multiaddress to the listen list.")
	flag.StringVar(&config.ProtocolID, "pid", "/idss/1.0.0", "Sets a protocol id for stream headers.")

	// Dynamically set the filename based on the peer ID
	defaultFileName := "./idss_graph_db/" + peerID + "/" + peerID + "_data.json"
	flag.StringVar(&config.Filename, "f", defaultFileName, "JSON file containing the graph data for this peer.")

	flag.Parse()

	if len(config.BootstrapPeers) == 0 {
		logger.Warn("No bootstrap peers provided. Using default bootstrap peers.")
		config.BootstrapPeers = dht.DefaultBootstrapPeers
		// Create a local bootstrap peer
		/* for i := 0; i < 5; i++ {
			port := 4001 + i
			localBootPeer, err := CreateLocalBootstrapPeers(port)
			if err != nil {
				return config, err
			}
			config.BootstrapPeers = append(config.BootstrapPeers, localBootPeer.Addrs...)
			logger.Info("Created local bootstrap peer: ", localBootPeer.ID)
		} */
	}
	return config, nil
}