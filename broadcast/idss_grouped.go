/*
 * ! \file idss_grouped.go
 * Grouped, decomposable aggregates (@group queries) for the Green Power use
 * case. Every peer contributes one entry tagged with its data partition; peers
 * forward the union of entries (deduplicated by partition, never by row
 * content) and only the originator folds them into the final result. Each
 * entry also carries a status, which forms the completeness manifest.
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package broadcast

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"idss/graphdb/access"
	"idss/graphdb/common"
	"idss/graphdb/flags"
	"idss/graphdb/groupagg"
	"idss/graphdb/helpers"

	"github.com/krotik/eliasdb/graph"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

// PartitionLabel returns the configured partition label or, if empty, the peer ID.
func PartitionLabel(config flags.Config, self peer.ID) string {
	if strings.TrimSpace(config.Partition) != "" {
		return strings.TrimSpace(config.Partition)
	}
	return self.String()
}

// isEmptyResultError reports EliasDB errors that mean "this partition holds no
// matching data" rather than a failure: the queried kind does not exist here,
// or a traversal found nothing.
func isEmptyResultError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Unknown node kind") || strings.Contains(msg, "Empty traversal")
}

// LocalGroupedEntry runs the EQL part of a grouped query on this peer and
// returns its partition entry together with the local result header.
func LocalGroupedEntry(spec groupagg.Spec, eqlQuery string, decision access.Decision, partition string, self peer.ID, gm *graph.Manager) (*common.PartitionPartial, []string) {
	entry := &common.PartitionPartial{PartitionId: partition, PeerId: self.String()}
	if decision == access.Deny {
		entry.Status = common.PartitionPartial_DENIED
		entry.Detail = "denied by local access policy"
		return entry, nil
	}
	rows, header, err := RunIDSSQuery(eqlQuery, self, gm)
	if err != nil && !isEmptyResultError(err) {
		entry.Status = common.PartitionPartial_ERROR
		entry.Detail = err.Error()
		return entry, nil
	}
	partials, err := groupagg.Local(spec, rows)
	if err != nil {
		entry.Status = common.PartitionPartial_ERROR
		entry.Detail = err.Error()
		return entry, header
	}
	entry.Status = common.PartitionPartial_ANSWERED
	for _, r := range groupagg.Encode(partials) {
		entry.Partials = append(entry.Partials, &common.Row{Data: r})
	}
	return entry, header
}

// UnionEntries merges partition entries. An ANSWERED entry for a partition wins
// over any other status for the same partition; failure entries recorded by a
// parent for a child peer (keyed "peer:<id>") are dropped when that peer's
// partition answered through another path. The result is sorted by partition.
func UnionEntries(groups ...[]*common.PartitionPartial) []*common.PartitionPartial {
	byPartition := map[string]*common.PartitionPartial{}
	answeredPeers := map[string]bool{}
	for _, g := range groups {
		for _, e := range g {
			if e == nil {
				continue
			}
			if e.Status == common.PartitionPartial_ANSWERED || e.Status == common.PartitionPartial_DENIED {
				answeredPeers[e.PeerId] = true
			}
			cur, ok := byPartition[e.PartitionId]
			if !ok || (cur.Status != common.PartitionPartial_ANSWERED && e.Status == common.PartitionPartial_ANSWERED) {
				byPartition[e.PartitionId] = e
			}
		}
	}
	out := make([]*common.PartitionPartial, 0, len(byPartition))
	for id, e := range byPartition {
		if strings.HasPrefix(id, "peer:") && answeredPeers[e.PeerId] && e.Status != common.PartitionPartial_ANSWERED {
			continue // the child did reply, via another parent
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartitionId < out[j].PartitionId })
	return out
}

// FoldEntries merges the partials of all ANSWERED entries.
func FoldEntries(spec groupagg.Spec, entries []*common.PartitionPartial) (groupagg.Partials, error) {
	merged := groupagg.Partials{}
	for _, e := range entries {
		if e.Status != common.PartitionPartial_ANSWERED {
			continue
		}
		rows := make([][]string, len(e.Partials))
		for i, r := range e.Partials {
			rows[i] = r.Data
		}
		p, err := groupagg.Decode(spec, rows)
		if err != nil {
			return nil, fmt.Errorf("partition %s: %v", e.PartitionId, err)
		}
		groupagg.Merge(spec, merged, p)
	}
	return merged, nil
}

// ManifestSummary renders e.g. "answered 7/8, denied 0, timeout 1 [dso04], ...".
func ManifestSummary(entries []*common.PartitionPartial) string {
	counts := map[common.PartitionPartial_Status][]string{}
	for _, e := range entries {
		counts[e.Status] = append(counts[e.Status], e.PartitionId)
	}
	part := func(s common.PartitionPartial_Status, name string) string {
		ids := counts[s]
		if len(ids) == 0 || s == common.PartitionPartial_ANSWERED {
			return fmt.Sprintf("%s %d", name, len(ids))
		}
		return fmt.Sprintf("%s %d %v", name, len(ids), ids)
	}
	return fmt.Sprintf("answered %d/%d, %s, %s, %s, %s",
		len(counts[common.PartitionPartial_ANSWERED]), len(entries),
		part(common.PartitionPartial_DENIED, "denied"),
		part(common.PartitionPartial_TIMEOUT, "timeout"),
		part(common.PartitionPartial_UNREACHABLE, "unreachable"),
		part(common.PartitionPartial_ERROR, "error"))
}

// HandleGroupedQuery executes a @group query on this peer, forwards it, and
// either returns the union of partition entries to the parent or, at the
// originator, the finalised result plus the manifest to the client.
func HandleGroupedQuery(conn network.Stream, msg *common.QueryMessage, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, decision access.Decision) {
	start := time.Now()
	self := kadDHT.Host().ID()
	spec, eqlQuery, err := groupagg.Parse(msg.Query)
	if err != nil {
		helpers.SendErrorMessage(conn, conn.Conn().RemotePeer(), err.Error())
		return
	}
	partition := PartitionLabel(config, self)

	var wg sync.WaitGroup
	var local *common.PartitionPartial
	var header []string
	wg.Add(1)
	go func() {
		defer wg.Done()
		local, header = LocalGroupedEntry(spec, eqlQuery, decision, partition, self, gm)
	}()
	remote, respondingPeers := forwardGrouped(msg, conn, config, gm, kadDHT)
	wg.Wait()
	logStepTiming(msg.Uqid, "grouped_local_and_forward", start)

	entries := UnionEntries([]*common.PartitionPartial{local}, remote)
	respondingPeers = append([]string{self.String()}, respondingPeers...)

	myAddr := kadDHT.Host().Addrs()[0].Encapsulate(multiaddr.StringCast("/p2p/" + self.String())).String()
	if msg.Originator != myAddr {
		reply := &common.QueryMessage{
			Type:              common.MessageType_RESULT,
			RecordCount:       int32(len(entries)),
			RespondingPeerIds: respondingPeers,
			PartitionPartials: entries,
		}
		sendGroupedReply(conn, reply)
		return
	}

	merged, err := FoldEntries(spec, entries)
	if err != nil {
		helpers.SendErrorMessage(conn, conn.Conn().RemotePeer(), err.Error())
		return
	}
	outHeader, rows := groupagg.Finalise(spec, header, merged)
	logger.Infof("Grouped query %s finished in %v: %d groups; %s", msg.Uqid, time.Since(start), len(rows), ManifestSummary(entries))

	// Manifest for the client: statuses only, partials stripped.
	manifest := make([]*common.PartitionPartial, len(entries))
	for i, e := range entries {
		manifest[i] = &common.PartitionPartial{PartitionId: e.PartitionId, PeerId: e.PeerId, Status: e.Status, Detail: e.Detail}
	}
	result := make([][]interface{}, len(rows))
	for i, r := range rows {
		result[i] = make([]interface{}, len(r))
		for j, c := range r {
			result[i][j] = c
		}
	}
	reply := &common.QueryMessage{
		Type:              common.MessageType_RESULT,
		Labels:            outHeader, // column names travel in labels, not as a data row
		Result:            helpers.CovertResultToProtobufRows(result, nil),
		RecordCount:       int32(len(rows)),
		RespondingPeerIds: respondingPeers,
		PartitionPartials: manifest,
	}
	sendGroupedReply(conn, reply)
}

func sendGroupedReply(conn network.Stream, reply *common.QueryMessage) {
	b, err := proto.Marshal(reply)
	if err != nil {
		logger.Errorf("Error marshalling grouped reply: %v", err)
		return
	}
	if err := helpers.WriteDelimitedMessage(conn, b); err != nil {
		logger.Errorf("Error sending grouped reply: %v", err)
	}
}

// forwardGrouped sends the query to eligible peers and collects their partition
// entries. A child that cannot be reached, does not answer in time or sends a
// malformed reply is recorded as a "peer:<id>" entry with the matching status.
func forwardGrouped(msg *common.QueryMessage, parentStream network.Stream, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) ([]*common.PartitionPartial, []string) {
	if msg == nil || msg.Ttl <= 0 {
		return nil, nil
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var entries []*common.PartitionPartial
	responding := map[string]struct{}{}
	slots := make(chan struct{}, maxConcurrentPeerQueries)

	var eligible []peer.ID
	for _, p := range kadDHT.RoutingTable().ListPeers() {
		if p == kadDHT.Host().ID() || (parentStream != nil && parentStream.Conn().RemotePeer() == p) {
			continue
		}
		eligible = append(eligible, p)
	}
	eligible = selectForwardPeers(eligible, remainingQueryTime(msg))

	parentDuration := remainingQueryTime(msg)
	forwardMsg := proto.Clone(msg).(*common.QueryMessage)
	if err := UpdateTTL(forwardMsg, gm); err != nil {
		logger.Errorf("Error updating TTL: %v", err)
	}
	forwardMsg.Type = common.MessageType_QUERY
	forwardMsg.Result = nil
	forwardMsg.PartitionPartials = nil
	msgBytes, err := proto.Marshal(forwardMsg)
	if err != nil {
		logger.Errorf("Error marshalling grouped query: %v", err)
		return nil, nil
	}

	fail := func(p peer.ID, status common.PartitionPartial_Status, detail string) {
		mu.Lock()
		entries = append(entries, &common.PartitionPartial{PartitionId: "peer:" + p.String(), PeerId: p.String(), Status: status, Detail: detail})
		mu.Unlock()
	}

	for _, p := range eligible {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			if parentDuration <= 0 || remainingQueryTime(forwardMsg) <= 0 {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), parentDuration)
			defer cancel()
			stream, err := kadDHT.Host().NewStream(ctx, p, protocol.ID(config.ProtocolID))
			if err != nil {
				fail(p, common.PartitionPartial_UNREACHABLE, err.Error())
				return
			}
			defer stream.Close()
			if err := helpers.WriteDelimitedMessage(stream, msgBytes); err != nil {
				fail(p, common.PartitionPartial_UNREACHABLE, err.Error())
				return
			}
			b, err := helpers.ReadDelimitedMessage(stream, ctx)
			if err != nil {
				status := common.PartitionPartial_ERROR
				if ctx.Err() != nil {
					status = common.PartitionPartial_TIMEOUT
				}
				fail(p, status, err.Error())
				return
			}
			var resp common.QueryMessage
			if err := proto.Unmarshal(b, &resp); err != nil || resp.Type != common.MessageType_RESULT {
				fail(p, common.PartitionPartial_ERROR, "malformed reply")
				return
			}
			if resp.Error != "" {
				fail(p, common.PartitionPartial_ERROR, resp.Error)
				return
			}
			// A peer that had already claimed this query replies with no
			// entries: it is not a failure, its data arrives via another parent.
			mu.Lock()
			entries = append(entries, resp.PartitionPartials...)
			for _, id := range resp.RespondingPeerIds {
				responding[id] = struct{}{}
			}
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	return entries, peerIDList(responding)
}
