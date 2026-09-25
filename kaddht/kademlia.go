/*
 * ! \file kademlia.go
 * kademlia DHT implementation for IDSS
 *
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 *
 */

package kaddht

import (
	"context"
	"idss/graphdb/flags"
	_ "net/http/pprof"
	"os"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/util"
)

var logger = log.Logger("IDSS")

type discoveryNotifee struct {
	ctx  context.Context
	host host.Host
}

func (n *discoveryNotifee) HandlePeerFound(p peer.AddrInfo) {
	n.host.Connect(n.ctx, p)
	logger.Debug("Connected to LAN peer: %s", p.ID)
}

// IDSS Function to discover and connect to peers
func DiscoverAndConnectPeers(ctx context.Context, host host.Host, config flags.Config, kadDHT *dht.IpfsDHT) {
	startTime := time.Now() //for debugging
	logger.Info("Starting peer discovery...")

	// mDNS only finds peers on the same broadcast domain, which does not hold
	// across HPC compute nodes; disable it there via IDSS_DISABLE_MDNS=1 and
	// rely on DHT routing discovery below instead.
	if os.Getenv("IDSS_DISABLE_MDNS") == "1" {
		logger.Info("mDNS discovery disabled (IDSS_DISABLE_MDNS=1)")
	} else {
		go func() {
			ser := mdns.NewMdnsService(host, config.IDSSString, &discoveryNotifee{
				ctx:  ctx,
				host: host,
			})
			if err := ser.Start(); err != nil {
				logger.Errorf("Error starting mDNS service: %v", err)
			}
		}()
	}

	// Provide a value for the given key. Where a key in this case is the service name (config.IDSSString)
	routingDiscovery := routing.NewRoutingDiscovery(kadDHT)
	util.Advertise(ctx, routingDiscovery, config.IDSSString)
	logger.Debug("Announced the IDSS service")

	// Look for other peers in the network who announced and connect to them
	anyConnected := false
	logger.Infoln("Searching for other peers...")

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
				break
			} else {
				logger.Infof("Connected to peer %s", peerInfo.ID)
				anyConnected = true
				break // Connect to only one
			}
		}
	}

	logger.Infof("Num of found peers in the Routing Table: %d", len(kadDHT.RoutingTable().GetPeerInfos()))
	logger.Debug("Peers in the Routing Table: ", kadDHT.RoutingTable().ListPeers())
	logger.Debug("Time taken to find peers: ", time.Since(startTime))
	logger.Info("Peer discovery completed")
}

// Function to initialise the DHT and bootstrap the peer with default bootstrap nodes
func InitialiseDHT(ctx context.Context, host host.Host, config flags.Config) *dht.IpfsDHT {
	kadDHT, err := dht.New( // Create a new DHT
		host,
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix("/idss"), // Prefix for the DHT protocol
		dht.BootstrapPeers(config.BootstrapPeers...),
	)

	if err != nil {
		logger.Debug("Error creating DHT: %v", err)
		os.Exit(1)
	}

	if len(config.BootstrapPeers) == 0 {
		logger.Warn("No bootstrap peers provided - starting in isolated mode")
	}

	if err := kadDHT.Bootstrap(ctx); err != nil {
		logger.Warnf("Partial DHT bootstrap: %v", err)
	}

	// Periodically refresh routing table
	go func() {
		for {
			time.Sleep(1 * time.Hour) // Refresh every 1 hours
			kadDHT.RefreshRoutingTable()
			logger.Debug("Refreshed routing table")
		}
	}()

	// Log routing table size every 2s so the launcher can wait for actual
	// convergence: "Peer discovery completed" only means this peer connected
	// to one other peer, but queries fan out via kadDHT.RoutingTable(),
	// which fills in later as the DHT's own lookups discover the rest.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			logger.Infof("Routing table peers: %d", len(kadDHT.RoutingTable().ListPeers()))
		}
	}()

	logger.Info("Bootstrapped the DHT")
	return kadDHT
}
