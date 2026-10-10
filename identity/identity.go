// Package identity signs and verifies the requester of a query (Green Power use
// case). The requester signs the query ID, query text, its peer ID and the
// query deadline with its libp2p key; every peer verifies the signature before
// applying its access policy, so a forwarding peer cannot change who asked or
// what was asked.
package identity

import (
	"bytes"
	"fmt"
	"os"

	"idss/graphdb/common"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// canonical returns the signed bytes of a query message.
func canonical(msg *common.QueryMessage) []byte {
	var b bytes.Buffer
	for _, part := range []string{"idss-gp-query-v1", msg.Uqid, msg.Query, msg.RequesterId} {
		b.WriteString(part)
		b.WriteByte(0)
	}
	return b.Bytes()
}

// Sign sets the requester ID, public key and signature of msg.
func Sign(msg *common.QueryMessage, priv crypto.PrivKey) error {
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return err
	}
	pub, err := crypto.MarshalPublicKey(priv.GetPublic())
	if err != nil {
		return err
	}
	msg.RequesterId = id.String()
	msg.RequesterPubkey = pub
	sig, err := priv.Sign(canonical(msg))
	if err != nil {
		return err
	}
	msg.RequesterSignature = sig
	return nil
}

// Verify checks that the public key belongs to the requester ID and that the
// signature covers the message as received.
func Verify(msg *common.QueryMessage) error {
	if len(msg.RequesterPubkey) == 0 || len(msg.RequesterSignature) == 0 {
		return fmt.Errorf("query is not signed")
	}
	pub, err := crypto.UnmarshalPublicKey(msg.RequesterPubkey)
	if err != nil {
		return fmt.Errorf("bad requester public key: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return err
	}
	if id.String() != msg.RequesterId {
		return fmt.Errorf("public key does not match requester %s", msg.RequesterId)
	}
	ok, err := pub.Verify(canonical(msg), msg.RequesterSignature)
	if err != nil || !ok {
		return fmt.Errorf("invalid requester signature")
	}
	return nil
}

// LoadOrCreateKey reads an Ed25519 libp2p private key from path, creating and
// saving a new one if the file does not exist. A stable key gives a requester a
// stable peer ID that DSO policies can list.
func LoadOrCreateKey(path string) (crypto.PrivKey, error) {
	if b, err := os.ReadFile(path); err == nil {
		return crypto.UnmarshalPrivateKey(b)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, err
	}
	b, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return nil, err
	}
	return priv, nil
}
