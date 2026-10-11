package dhtcast

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func peers(t *testing.T, n int) []peer.ID {
	t.Helper()
	out := make([]peer.ID, n)
	for i := range out {
		_, pub, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if out[i], err = peer.IDFromPublicKey(pub); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// kBucketTable keeps at most k contacts per CPL bucket, as a Kademlia routing
// table would (k = 0: everyone).
func kBucketTable(self peer.ID, all []peer.ID, k int) []peer.ID {
	count := map[int]int{}
	var out []peer.ID
	for _, p := range all {
		if p == self {
			continue
		}
		c := CPL(KadID(self), KadID(p))
		if k > 0 && count[c] >= k {
			continue
		}
		count[c]++
		out = append(out, p)
	}
	return out
}

// broadcast simulates the tree dissemination from root and returns how often
// each peer received the query, the number of messages and the tree depth.
func broadcast(root peer.ID, table map[peer.ID][]peer.ID, dead map[peer.ID]bool) (map[peer.ID]int, int, int) {
	got := map[peer.ID]int{root: 1}
	msgs, depth := 0, 0
	var visit func(p peer.ID, minCPL, d int)
	visit = func(p peer.ID, minCPL, d int) {
		if d > depth {
			depth = d
		}
		for _, r := range Plan(p, table[p], minCPL, nil) {
			for _, child := range r.Peers { // failover: first live contact in the region
				msgs++
				if dead[child] {
					continue
				}
				got[child]++
				visit(child, r.ChildMinCPL(), d+1)
				break
			}
		}
	}
	visit(root, 0, 0)
	return got, msgs, depth
}

func TestCPL(t *testing.T) {
	cases := []struct {
		a, b []byte
		want int
	}{
		{[]byte{0xff, 0x00}, []byte{0xff, 0x00}, 16},
		{[]byte{0x80, 0x00}, []byte{0x00, 0x00}, 0},
		{[]byte{0xff, 0x01}, []byte{0xff, 0x00}, 15},
		{[]byte{0x10, 0x00}, []byte{0x18, 0x00}, 4},
	}
	for _, c := range cases {
		if got := CPL(c.a, c.b); got != c.want {
			t.Errorf("CPL(%x, %x) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestBroadcastReachesEveryPeerExactlyOnce(t *testing.T) {
	for _, n := range []int{2, 8, 14, 32, 62, 300} {
		for _, k := range []int{0, 20, 3} {
			t.Run(fmt.Sprintf("n=%d k=%d", n, k), func(t *testing.T) {
				all := peers(t, n)
				table := map[peer.ID][]peer.ID{}
				for _, p := range all {
					table[p] = kBucketTable(p, all, k)
				}
				got, msgs, depth := broadcast(all[0], table, nil)
				for _, p := range all {
					if got[p] != 1 {
						t.Fatalf("peer %s received the query %d times", p, got[p])
					}
				}
				if msgs != n-1 {
					t.Fatalf("%d messages for %d peers, want %d", msgs, n, n-1)
				}
				if depth > 2*bitsLen(n)+2 {
					t.Fatalf("tree depth %d for %d peers", depth, n)
				}
			})
		}
	}
}

func bitsLen(n int) int {
	b := 0
	for ; n > 0; n >>= 1 {
		b++
	}
	return b
}

func TestFailoverCoversRegionOfDeadDelegate(t *testing.T) {
	all := peers(t, 62)
	table := map[peer.ID][]peer.ID{}
	for _, p := range all {
		table[p] = kBucketTable(p, all, 0)
	}
	// Kill the first delegate the root would pick in its largest region.
	plan := Plan(all[0], table[all[0]], 0, nil)
	victim := plan[0].Peers[0]
	got, _, _ := broadcast(all[0], table, map[peer.ID]bool{victim: true})
	for _, p := range all {
		want := 1
		if p == victim {
			want = 0
		}
		if got[p] != want {
			t.Fatalf("peer %s received %d times, want %d", p, got[p], want)
		}
	}
}

func TestPlanRespectsMinCPLAndPreference(t *testing.T) {
	all := peers(t, 40)
	self := all[0]
	for _, r := range Plan(self, all, 2, nil) {
		if r.CPL < 2 {
			t.Fatalf("region with CPL %d below minCPL 2", r.CPL)
		}
		for _, p := range r.Peers {
			if CPL(KadID(self), KadID(p)) != r.CPL {
				t.Fatalf("peer in wrong region")
			}
		}
	}
	if Plan(self, all, Leaf, nil) != nil {
		t.Fatal("Leaf must not delegate")
	}
	pref := map[peer.ID]bool{}
	full := Plan(self, all, 0, nil)
	last := full[0].Peers[len(full[0].Peers)-1]
	pref[last] = true
	if got := Plan(self, all, 0, func(p peer.ID) bool { return pref[p] })[0].Peers[0]; got != last {
		t.Fatalf("preferred contact not first")
	}
}
