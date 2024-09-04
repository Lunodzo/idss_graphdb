package common

import (
	//"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const(
	discoveryServiceTag = "kadProtocol"             // The tag used to advertise and discover the IDSS service
	IDSS_PROTOCOL       = protocol.ID("/idss/1.0.0")
)

/* type QueryMessage struct {
	UQI   string
	Query string
	TTL   int
} */

/* type QueryResult struct {
	PeerID peer.ID
	Result [][]interface{}
	Labels []string
	Error  error
} */