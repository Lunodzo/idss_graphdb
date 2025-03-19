/*
 * ! \file idss_common.go
 * The common file for the IDSS project. It contains the constants, variables and types used across the project.
 * 
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 *
 */

package common

import (
	"time"

	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const(
	DISCOVERY_SERVICE 		  = "idss_stringa"             				// The tag used to advertise and discover the IDSS service
	IDSS_PROTOCOL_LOCAL       = protocol.ID("/lan/kad/1.0.0") 		// The protocol used for local network communication
	IDSS_PROTOCOL_GLOBAL      = protocol.ID("/kad/1.0.0") 			// The protocol used for global network communication
	DB_PATH 				  = "../server/idss_graph_db" 				// Path to the graph database
)

var(
	ActiveConnections int64
	Logger            = log.Logger("IDSS")
)

type AggregateInfo struct {
    Function    string
    Traversal   string
    Filter      string
    Comparison  string
    Attribute   string
    Threshold   float64
}

type QueryState_ struct {
    CreatedAt  time.Time
    ExpiresAt  time.Time
    Hops       int
}