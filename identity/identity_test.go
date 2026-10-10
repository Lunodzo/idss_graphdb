package identity

import (
	"path/filepath"
	"testing"

	"idss/graphdb/common"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func signed(t *testing.T) (*common.QueryMessage, crypto.PrivKey) {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	msg := &common.QueryMessage{Uqid: "u1", Query: "@group(keys=0; aggs=count) get FailureEvent show failureYear", Timestamp: "2026-10-10T12:00:00Z"}
	if err := Sign(msg, priv); err != nil {
		t.Fatal(err)
	}
	return msg, priv
}

func TestSignVerify(t *testing.T) {
	msg, _ := signed(t)
	if err := Verify(msg); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestForwardingMayShortenDeadline(t *testing.T) {
	msg, _ := signed(t)
	msg.Timestamp = "2026-10-10T11:59:00Z" // UpdateTTL rewrites the deadline at every hop
	msg.Ttl = 7.5
	if err := Verify(msg); err != nil {
		t.Fatalf("deadline change must not invalidate the signature: %v", err)
	}
}

func TestTamperingIsDetected(t *testing.T) {
	for name, tamper := range map[string]func(m *common.QueryMessage){
		"query changed": func(m *common.QueryMessage) { m.Query = "get FailureEvent" },
		"uqid changed":  func(m *common.QueryMessage) { m.Uqid = "u2" },
		"requester swapped": func(m *common.QueryMessage) {
			other, _ := signed(t)
			m.RequesterId = other.RequesterId
		},
		"key swapped": func(m *common.QueryMessage) {
			other, _ := signed(t)
			m.RequesterPubkey = other.RequesterPubkey
		},
		"unsigned": func(m *common.QueryMessage) { m.RequesterSignature = nil },
	} {
		msg, _ := signed(t)
		tamper(msg)
		if err := Verify(msg); err == nil {
			t.Errorf("%s: tampered message accepted", name)
		}
	}
}

func TestLoadOrCreateKeyIsStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "requester.key")
	a, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Equals(b) {
		t.Fatalf("reloaded key differs")
	}
}
