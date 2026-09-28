/*
 * ! \file idss_settlement_scoped.go
 * Community-scoped settlement: the manager of an energy community collects
 * aggregate-only totals from an explicit list of peers (its registered
 * members) instead of from whatever peers its routing table happens to hold.
 *
 * The legacy, unscoped CompileSettlement in idss_broadcast.go is unchanged
 * and is still used whenever a peer runs without a community (-community "").
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package broadcast

import (
	"context"
	"fmt"
	"time"

	"idss/graphdb/common"
	"idss/graphdb/helpers"

	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
)

// SettlementStats summarises one scoped settlement run.
type SettlementStats struct {
	CommunityID string
	Scope       string // "registry" (the registered members) or "routing-table" (probe)
	Targets     int    // peers asked, excluding the manager itself
	Responded   int    // peers that returned totals, excluding the manager itself
	Refused     int    // peers that answered with a refusal
	Unreachable int    // peers that could not be reached or did not answer
}

// Summary is a one-line, log- and client-friendly rendering of the stats.
func (s SettlementStats) Summary() string {
	return fmt.Sprintf("community=%s scope=%s targets=%d responded=%d refused=%d unreachable=%d",
		s.CommunityID, s.Scope, s.Targets, s.Responded, s.Refused, s.Unreachable)
}

// CompileCommunitySettlement collects aggregate-only totals for [from, to]
// from the manager itself and from each peer in targets, over protocolID.
// Every request carries requestCommunity, and each receiving member checks
// it (and the authenticated identity of the sender) before answering.
// Only totals from peers that confirm membership of communityID are stored
// as SettlementSummary nodes. Phase timings use the same log format as
// CompileSettlement so the experiment harness can parse both.
func CompileCommunitySettlement(gm *graph.Manager, kadDHT *dht.IpfsDHT, protocolID protocol.ID,
	communityID string, requestCommunity string, scope string, targets []peer.ID,
	from time.Time, to time.Time) (SettlementStats, error) {

	stats := SettlementStats{CommunityID: communityID, Scope: scope, Targets: len(targets)}
	totalStart := time.Now()
	host := kadDHT.Host()
	results := []*common.SettlementResult{}

	localTotalsStart := time.Now()
	meterSum, tradeSum, err := LocalSettlementTotals(gm, host.ID(), from, to)
	if err != nil {
		return stats, err
	}
	logger.Infof("Settlement phase=local_totals duration_ms=%.3f", time.Since(localTotalsStart).Seconds()*1000)
	results = append(results, &common.SettlementResult{PeerId: host.ID().String(), MeterReadingSum: meterSum, TradeVolumeSum: tradeSum, CommunityId: communityID})

	broadcastStart := time.Now()
	request := &common.QueryMessage{
		Type:              common.MessageType_SETTLEMENT_REQUEST,
		SettlementRequest: &common.SettlementRequest{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339), CommunityId: requestCommunity},
	}
	payload, err := proto.Marshal(request)
	if err != nil {
		return stats, fmt.Errorf("encoding settlement request: %v", err)
	}
	for _, remotePeer := range targets {
		if remotePeer == host.ID() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		// Registered members are normally still connected from registration;
		// otherwise resolve their current addresses through the DHT.
		if len(host.Peerstore().Addrs(remotePeer)) == 0 {
			if info, findErr := kadDHT.FindPeer(ctx, remotePeer); findErr == nil {
				host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.TempAddrTTL)
			}
		}
		response, askErr := askSettlement(ctx, kadDHT, protocolID, remotePeer, payload)
		cancel()
		switch {
		case askErr != nil:
			stats.Unreachable++
			logger.Debugf("Settlement request to %s failed: %v", remotePeer, askErr)
		case response.Refusal != "":
			stats.Refused++
			logger.Infof("Settlement refused by %s: %s", remotePeer, response.Refusal)
		case response.CommunityId != communityID:
			// A peer outside this community that still answered (e.g. a
			// legacy peer without a community): never count its totals.
			stats.Refused++
			logger.Infof("Settlement result from %s ignored: community %q is not %q", remotePeer, response.CommunityId, communityID)
		default:
			stats.Responded++
			results = append(results, response)
		}
	}
	logger.Infof("Settlement phase=broadcast_collect duration_ms=%.3f responding_peers=%d %s",
		time.Since(broadcastStart).Seconds()*1000, len(results), stats.Summary())

	writeStart := time.Now()
	for _, result := range results {
		summary := data.NewGraphNode()
		summary.SetAttr("key", fmt.Sprintf("settlement-%s-%s-%d-%d", communityID, result.PeerId, from.Unix(), to.Unix()))
		summary.SetAttr("kind", "SettlementSummary")
		summary.SetAttr("communityId", communityID)
		summary.SetAttr("member", result.PeerId)
		summary.SetAttr("from", from.Format(time.RFC3339))
		summary.SetAttr("to", to.Format(time.RFC3339))
		summary.SetAttr("meterReadingSum", result.MeterReadingSum)
		summary.SetAttr("tradeVolumeSum", result.TradeVolumeSum)
		if err := gm.StoreNode("main", summary); err != nil {
			return stats, fmt.Errorf("storing settlement summary: %v", err)
		}
	}
	logger.Infof("Settlement phase=write_summaries duration_ms=%.3f", time.Since(writeStart).Seconds()*1000)
	logger.Infof("Settlement phase=total duration_ms=%.3f", time.Since(totalStart).Seconds()*1000)
	logger.Infof("Compiled %d settlement summaries (%s)", len(results), stats.Summary())
	return stats, nil
}

// askSettlement sends one settlement request and waits for the single reply.
func askSettlement(ctx context.Context, kadDHT *dht.IpfsDHT, protocolID protocol.ID, remotePeer peer.ID, payload []byte) (*common.SettlementResult, error) {
	stream, err := kadDHT.Host().NewStream(ctx, remotePeer, protocolID)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	if err := helpers.WriteDelimitedMessage(stream, payload); err != nil {
		return nil, err
	}
	responseData, err := helpers.ReadDelimitedMessage(stream, ctx)
	if err != nil {
		return nil, err
	}
	response := &common.QueryMessage{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, err
	}
	if response.SettlementResult == nil {
		return nil, fmt.Errorf("reply carried no settlement result")
	}
	return response.SettlementResult, nil
}
