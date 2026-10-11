// Package dhtcast plans structured query propagation over the Kademlia DHT.
//
// A query is disseminated along a spanning tree derived from the Kademlia ID
// space instead of being flooded (broadcast scheme of Czirkos and Hosszú,
// "Solution for the broadcasting in the Kademlia peer-to-peer overlay",
// Computer Networks 57(8), 2013; cf. El-Ansary et al., "Efficient broadcast in
// structured P2P networks", IPTPS 2003).
//
// Peer X is responsible for the part of the ID space whose keys share at least
// MinCPL leading bits with X's Kademlia ID. That part splits into the regions
// R(X, c), c = MinCPL..KeyBits-1: the keys sharing exactly c leading bits with
// X. X delegates each non-empty region to one contact Y in it, with
// MinCPL(Y) = c+1: the keys sharing at least c+1 bits with Y are exactly
// R(X, c). Regions are disjoint, so every peer receives the query exactly once
// (N-1 messages for N peers), the tree depth is O(log N), and results are
// merged on the way back up (convergecast). A region with several contacts
// gives failover candidates.
//
// Kademlia IDs are SHA-256 hashes of the peer ID, as in go-libp2p-kbucket.
package dhtcast

import (
	"bytes"
	"crypto/sha256"
	"math/bits"
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
)

// KeyBits is the length of a Kademlia ID in bits.
const KeyBits = 256

// Leaf is a MinCPL value that makes the receiver answer without forwarding
// (used for direct requests, e.g. to the providers of a public register).
const Leaf = KeyBits + 1

// KadID returns the Kademlia ID of a peer (SHA-256 of the peer ID bytes).
func KadID(p peer.ID) []byte {
	h := sha256.Sum256([]byte(p))
	return h[:]
}

// CPL returns the number of leading bits two IDs have in common.
func CPL(a, b []byte) int {
	for i := range a {
		if x := a[i] ^ b[i]; x != 0 {
			return i*8 + bits.LeadingZeros8(x)
		}
	}
	return len(a) * 8
}

// Region is one delegated part of the ID space: the contacts sharing exactly
// CPL leading bits with the planning peer, in order of preference.
type Region struct {
	CPL   int
	Peers []peer.ID
}

// ChildMinCPL is the MinCPL a delegate of this region is responsible for.
func (r Region) ChildMinCPL() int { return r.CPL + 1 }

// Plan groups the contacts of self into the regions self must delegate when it
// is responsible for keys sharing at least minCPL bits with it. Regions are
// returned in ascending CPL. Within a region, contacts for which preferred
// returns true (e.g. already connected) come first, then the rest; ties are
// broken by XOR distance to self, so plans are deterministic.
func Plan(self peer.ID, contacts []peer.ID, minCPL int, preferred func(peer.ID) bool) []Region {
	if minCPL >= KeyBits {
		return nil
	}
	selfID := KadID(self)
	byCPL := map[int][]peer.ID{}
	dist := map[peer.ID][]byte{}
	seen := map[peer.ID]bool{}
	for _, p := range contacts {
		if p == self || seen[p] {
			continue
		}
		seen[p] = true
		id := KadID(p)
		c := CPL(selfID, id)
		if c < minCPL {
			continue
		}
		d := make([]byte, len(id))
		for i := range id {
			d[i] = id[i] ^ selfID[i]
		}
		dist[p] = d
		byCPL[c] = append(byCPL[c], p)
	}
	regions := make([]Region, 0, len(byCPL))
	for c, ps := range byCPL {
		sort.Slice(ps, func(i, j int) bool {
			if preferred != nil {
				pi, pj := preferred(ps[i]), preferred(ps[j])
				if pi != pj {
					return pi
				}
			}
			return bytes.Compare(dist[ps[i]], dist[ps[j]]) < 0
		})
		regions = append(regions, Region{CPL: c, Peers: ps})
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i].CPL < regions[j].CPL })
	return regions
}
