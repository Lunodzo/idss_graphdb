package broadcast

import (
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"idss/graphdb/common"
)

func TestParseResultClauses(t *testing.T) {
	query, fields, limit := parseResultClauses("get Trade fields mRID, volume, price limit 10")
	if query != "get Trade" || limit != 10 || len(fields) != 3 || fields[1] != "volume" {
		t.Fatalf("unexpected parsed clauses: query=%q fields=%v limit=%d", query, fields, limit)
	}
}

func TestUpdateTTLDoesNotUpdateEmptyQueryID(t *testing.T) {
	msg := &common.QueryMessage{Uqid: "   ", Ttl: 4.0}

	if err := UpdateTTL(msg, nil); err != nil {
		t.Fatalf("UpdateTTL() with empty Uqid returned error: %v", err)
	}
	if msg.Ttl != 4.0 {
		t.Fatalf("UpdateTTL() should leave TTL unchanged for empty Uqid, got %v", msg.Ttl)
	}
}

func TestRemainingQueryTimeUsesAbsoluteDeadline(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second).UTC()
	remaining := remainingQueryTime(&common.QueryMessage{Ttl: 3, Timestamp: deadline.Format(time.RFC3339Nano)})
	if remaining <= 0 || remaining > 3*time.Second {
		t.Fatalf("remainingQueryTime() = %s, want a positive duration no greater than 3s", remaining)
	}
}

func TestTTLReductionFactor(t *testing.T) {
	if ttlReductionFactor != 0.75 {
		t.Fatalf("ttlReductionFactor = %v, want 0.75", ttlReductionFactor)
	}
}

func TestSelectForwardPeersAdaptsToBudget(t *testing.T) {
	peers := make([]peer.ID, 40)
	for index := range peers {
		peers[index] = peer.ID(fmt.Sprintf("peer-%02d", index))
	}
	for _, test := range []struct {
		budget time.Duration
		want   int
	}{
		{500 * time.Millisecond, 20},
		{1 * time.Second, 40},
		{2 * time.Second, 40},
		{5 * time.Second, maxForwardPeers},
	} {
		selected := selectForwardPeers(append([]peer.ID(nil), peers...), test.budget)
		if len(selected) != test.want {
			t.Errorf("selectForwardPeers(%s) selected %d peers, want %d", test.budget, len(selected), test.want)
		}
	}
}
