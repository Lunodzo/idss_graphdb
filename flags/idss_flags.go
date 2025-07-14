/*
 * ! \file flags/idss_flags.go
 * The flags file for the IDSS project. It contains the flags to be used by the IDSS project.
 * If the flags are not provided, default values are used. The flags are used to set the 
 * configuration of the IDSS project.
 * 
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 *
 */

package flags

import (
	"flag"
	"idss/graphdb/common"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	maddr "github.com/multiformats/go-multiaddr"
)

type addrList 		[]maddr.Multiaddr
type peerAddrList 	[]peer.AddrInfo

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

// Function to convert a list of string addresses to multiaddresses
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


func (pal *peerAddrList) String() string {
	strs := make([]string, len(*pal))
	for i, addr := range *pal {
		strs[i] = addr.String()
	}
	return strings.Join(strs, ",")
}

func (pal *peerAddrList) Set(value string) error {
	addr, err := peer.AddrInfoFromString(value)
	if err != nil {
		return err
	}
	*pal = append(*pal, *addr)
	return nil
}

type Config struct {
	IDSSString       string
	BootstrapPeers   peerAddrList
	ListenAddresses  addrList
	ProtocolID       string
	Filename         string
}

// Function to parse the flags and return the configuration
func ParseFlags(peerID string) (Config, error) {
	config := Config{}
	flag.StringVar(&config.IDSSString, "IDSS", common.DISCOVERY_SERVICE,
		"Unique string to identify group of nodes. Share this with peers to let them connect.")
	flag.Var(&config.BootstrapPeers, "peer", "Adds a peer multiaddress to the bootstrap list (required for offline cluster)")
	flag.Var(&config.ListenAddresses, "listen", "Adds a multiaddress to the listen list.")
	flag.StringVar(&config.ProtocolID, "pid", string(common.IDSS_PROTOCOL_LOCAL), 
		"Sets a protocol id for stream headers. Supply /ipfs/kad/1.0.0 for global network communication.")

	// Dynamically set the filename based on the peer ID
	defaultFileName := "./idss_graph_db/" + peerID + "/" + peerID + "_data.json" // Will be used to store the graph data incase file name is not provided
	flag.StringVar(&config.Filename, "f", defaultFileName, "JSON file containing the graph data for this peer.") // JSON file containing the graph data for this peer

	flag.Parse()
	return config, nil
}