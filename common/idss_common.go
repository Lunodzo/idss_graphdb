package common

import (
	"github.com/libp2p/go-libp2p/core/protocol"
)

const(
	discoveryServiceTag = "kadProtocol"             // The tag used to advertise and discover the IDSS service
	IDSS_PROTOCOL       = protocol.ID("/idss/1.0.0")
)