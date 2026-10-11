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
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"idss/graphdb/access"
	"idss/graphdb/common"
	"idss/graphdb/dhtcast"
	"idss/graphdb/flags"
	"idss/graphdb/gpjoin"
	"idss/graphdb/groupagg"
	"idss/graphdb/helpers"
	"idss/graphdb/identity"

	"github.com/ipfs/go-cid"
	"github.com/krotik/eliasdb/graph"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
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
//
// keep, when not nil, filters the local rows before grouping (used by @join
// queries to keep only rows matching the shipped public events).
//
// denyReason explains a Deny decision in the manifest. hidden, when not nil,
// reports attributes this peer never discloses: a query whose group keys or
// aggregates use such a column is declined by this partition only.
func LocalGroupedEntry(spec groupagg.Spec, eqlQuery string, decision access.Decision, denyReason string, hidden func(string) bool, partition string, self peer.ID, gm *graph.Manager, keep func([][]interface{}) ([][]interface{}, error)) (*common.PartitionPartial, []string) {
	entry := &common.PartitionPartial{PartitionId: partition, PeerId: self.String()}
	if decision == access.Deny {
		entry.Status = common.PartitionPartial_DENIED
		entry.Detail = denyReason
		if entry.Detail == "" {
			entry.Detail = "denied by local access policy"
		}
		return entry, nil
	}
	rows, header, err := RunIDSSQuery(eqlQuery, self, gm)
	if err != nil && !isEmptyResultError(err) {
		entry.Status = common.PartitionPartial_ERROR
		entry.Detail = err.Error()
		return entry, nil
	}
	if col := HiddenColumnUsed(spec, header, hidden); col != "" {
		entry.Status = common.PartitionPartial_DENIED
		entry.Detail = "query uses hidden attribute " + col
		return entry, header
	}
	if keep != nil && len(rows) > 0 {
		if rows, err = keep(rows); err != nil {
			entry.Status = common.PartitionPartial_ERROR
			entry.Detail = err.Error()
			return entry, header
		}
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

// HiddenColumnUsed returns the label of the first group key or aggregate column
// whose attribute is hidden by the local policy, or "" if none is.
func HiddenColumnUsed(spec groupagg.Spec, header []string, hidden func(string) bool) string {
	if hidden == nil {
		return ""
	}
	check := func(col int) string {
		if col < len(header) && hidden(header[col]) {
			return header[col]
		}
		return ""
	}
	for _, k := range spec.Keys {
		if c := check(k.Col); c != "" {
			return c
		}
	}
	for _, a := range spec.Aggs {
		if a.Func == "count" {
			continue
		}
		if c := check(a.Col); c != "" {
			return c
		}
		if a.Func == "exposure" {
			if c := check(a.YearCol); c != "" {
				return c
			}
		}
	}
	return ""
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
func HandleGroupedQuery(conn network.Stream, msg *common.QueryMessage, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, decision access.Decision, denyReason string, hidden func(string) bool) {
	start := time.Now()
	self := kadDHT.Host().ID()
	fail := func(err error) { helpers.SendErrorMessage(conn, conn.Conn().RemotePeer(), err.Error()) }

	groupQuery := msg.Query
	var join *gpjoin.Spec
	if gpjoin.IsJoin(msg.Query) {
		js, rest, err := gpjoin.Parse(msg.Query)
		if err != nil {
			fail(err)
			return
		}
		join, groupQuery = &js, rest
	}
	spec, eqlQuery, err := groupagg.Parse(groupQuery)
	if err != nil {
		fail(err)
		return
	}
	if join != nil {
		if err := gpjoin.CheckNoGeometryLeak(*join, spec); err != nil {
			fail(err)
			return
		}
	}
	partition := PartitionLabel(config, self)
	myAddr := kadDHT.Host().Addrs()[0].Encapsulate(multiaddr.StringCast("/p2p/" + self.String())).String()
	isOriginator := msg.Originator == myAddr

	// @join phase 1 (originator only): collect the public events, which are
	// then shipped with the query so every peer joins locally.
	var keep func([][]interface{}) ([][]interface{}, error)
	var phase1Entries []*common.PartitionPartial
	if join != nil {
		if isOriginator && len(msg.JoinEvents) == 0 {
			p1 := time.Now()
			events, entries, mode, err := collectJoinEvents(msg, join.Type, config, gm, kadDHT)
			if err != nil {
				fail(fmt.Errorf("join phase 1: %v", err))
				return
			}
			msg.JoinEvents = events
			phase1Entries = entries
			logger.Infof("Join %s phase 1: %d %s events in %v (via %s)", msg.Uqid, len(events), join.Type, time.Since(p1), mode)
		}
		rows := make([][]string, len(msg.JoinEvents))
		for i, r := range msg.JoinEvents {
			rows[i] = r.Data
		}
		events, err := gpjoin.ParseEvents(join.Type, rows)
		if err != nil {
			fail(err)
			return
		}
		keep = gpjoin.NewMatcher(*join, events).Filter
	}

	var wg sync.WaitGroup
	var local *common.PartitionPartial
	var header []string
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.Now()
		local, header = LocalGroupedEntry(spec, eqlQuery, decision, denyReason, hidden, partition, self, gm, keep)
		logStepTiming(msg.Uqid, "grouped_local", t)
	}()
	remote, respondingPeers := forwardGrouped(msg, conn, config, gm, kadDHT)
	wg.Wait()
	logStepTiming(msg.Uqid, "grouped_local_and_forward", start)

	entries := UnionEntries([]*common.PartitionPartial{local}, remote)
	respondingPeers = append([]string{self.String()}, respondingPeers...)

	if !isOriginator {
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
		fail(err)
		return
	}
	// A public partition that did not deliver its events in phase 1 makes the
	// join incomplete even though it answered phase 2: report it as missing.
	entries = MergePhase1(entries, phase1Entries)
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

// MergePhase1 adds the join phase-1 entries that did not answer to the final
// manifest, replacing the phase-2 entry of the same partition (which answered
// phase 2 with no failures and would otherwise hide the missing events).
func MergePhase1(entries, phase1 []*common.PartitionPartial) []*common.PartitionPartial {
	for _, e := range phase1 {
		if e == nil || e.Status == common.PartitionPartial_ANSWERED {
			continue
		}
		f := &common.PartitionPartial{PartitionId: e.PartitionId, PeerId: e.PeerId, Status: e.Status,
			Detail: "join phase 1: " + e.Detail}
		replaced := false
		for i, x := range entries {
			if x.PartitionId == e.PartitionId {
				entries[i], replaced = f, true
			}
		}
		if !replaced {
			entries = append(entries, f)
		}
	}
	return entries
}

// collectJoinEvents runs phase 1 of a join: the grouped event query (every
// distinct public event is one group) is sent directly to the peers that
// announce the public register's node kind as DHT provider records (see
// KindKey). Only when no provider is found is the query disseminated along
// the DHT tree instead. Phase 1 uses at most 40 % of the remaining time. It
// returns the event rows, the phase-1 manifest entries and the mode used.
func collectJoinEvents(msg *common.QueryMessage, joinType string, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) ([]*common.Row, []*common.PartitionPartial, string, error) {
	query, err := gpjoin.EventQuery(joinType)
	if err != nil {
		return nil, nil, "", err
	}
	kind, err := gpjoin.EventKind(joinType)
	if err != nil {
		return nil, nil, "", err
	}
	spec, eqlQuery, err := groupagg.Parse(query)
	if err != nil {
		return nil, nil, "", err
	}
	budget := time.Duration(float64(remainingQueryTime(msg)) * 0.4)
	if budget <= 0 {
		return nil, nil, "", fmt.Errorf("no time left for phase 1")
	}
	start := time.Now()
	self := kadDHT.Host().ID()
	p1 := &common.QueryMessage{
		Type:          common.MessageType_QUERY,
		Query:         query,
		Uqid:          msg.Uqid + "-events",
		Ttl:           float32(budget.Seconds()),
		Timestamp:     time.Now().Add(budget).UTC().Format(time.RFC3339Nano),
		Originator:    msg.Originator,
		Sender:        self.String(),
		RequesterId:   msg.RequesterId,
		RequesterRole: msg.RequesterRole,
		State:         &common.QueryState{State: common.QueryState_QUEUED},
	}
	// Phase 1 is requested by the originator peer itself: sign it with the
	// peer's own key (the client's signature covers the original query only).
	if priv := kadDHT.Host().Peerstore().PrivKey(self); priv != nil {
		if err := identity.Sign(p1, priv); err != nil {
			return nil, nil, "", fmt.Errorf("signing phase-1 query: %v", err)
		}
	}
	StoreQueryInfo(p1, gm, self.String())

	lookupCtx, cancel := context.WithTimeout(context.Background(), budget/2)
	providers := FindKindProviders(lookupCtx, kadDHT, kind)
	cancel()
	logger.Infof("timing uqi=%s step=provider_lookup ms=%.1f", msg.Uqid, time.Since(start).Seconds()*1000)

	var entries []*common.PartitionPartial
	mode := "providers"
	if len(providers) == 0 {
		// No provider record found: fall back to dissemination along the DHT tree.
		mode = "dht-tree"
		local, _ := LocalGroupedEntry(spec, eqlQuery, access.Allow, "", nil, PartitionLabel(config, self), self, gm, nil)
		remote, _ := forwardGrouped(p1, nil, config, gm, kadDHT)
		entries = UnionEntries([]*common.PartitionPartial{local}, remote)
	} else {
		entries = askProviders(p1, kind, providers, spec, eqlQuery, config, gm, kadDHT)
	}
	logger.Infof("Join phase 1 partitions (%s, %d providers of %s): %s", mode, len(providers), kind, ManifestSummary(entries))
	merged, err := FoldEntries(spec, entries)
	if err != nil {
		return nil, nil, "", err
	}
	_, rows := groupagg.Finalise(spec, nil, merged)
	out := make([]*common.Row, len(rows))
	for i, r := range rows {
		out[i] = &common.Row{Data: r[:len(spec.Keys)]} // key cells = the event; drop the count
	}
	return out, entries, mode, nil
}

// askProviders sends the phase-1 query directly to each provider (as a leaf:
// providers answer without forwarding) and answers locally if this peer is one.
// A provider that does not answer is reported as "events:<kind>@<peer>".
func askProviders(p1 *common.QueryMessage, kind string, providers []peer.ID, spec groupagg.Spec, eqlQuery string, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) []*common.PartitionPartial {
	self := kadDHT.Host().ID()
	leaf := proto.Clone(p1).(*common.QueryMessage)
	leaf.SubtreeMinCpl = dhtcast.Leaf
	b, err := proto.Marshal(leaf)
	if err != nil {
		return []*common.PartitionPartial{{PartitionId: "events", Status: common.PartitionPartial_ERROR, Detail: err.Error()}}
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var entries []*common.PartitionPartial
	var traffic TrafficCounter
	for _, p := range providers {
		if p == self {
			local, _ := LocalGroupedEntry(spec, eqlQuery, access.Allow, "", nil, PartitionLabel(config, self), self, gm, nil)
			entries = append(entries, local)
			continue
		}
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			got, status, detail := askPeer(leaf, p, b, config, kadDHT, &traffic)
			mu.Lock()
			defer mu.Unlock()
			if status != common.PartitionPartial_ANSWERED {
				// The provider's partition label is unknown: name the register.
				entries = append(entries, &common.PartitionPartial{PartitionId: "events:" + kind + "@" + p.String(), PeerId: p.String(), Status: status, Detail: detail})
				return
			}
			entries = append(entries, got.PartitionPartials...)
		}(p)
	}
	wg.Wait()
	logger.Infof("%s", traffic.LogLine(p1.Uqid))
	return UnionEntries(entries)
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

// maxRegionAttempts bounds failover: how many contacts of one DHT region are
// tried before the region is reported as unreachable.
const maxRegionAttempts = 3

// forwardGrouped propagates the query along the Kademlia tree (package
// dhtcast) and collects the partition entries of the subtree. This peer covers
// the IDs sharing at least msg.SubtreeMinCpl leading bits with it; it delegates
// each non-empty region of that space to one routing-table contact (connected
// contacts first). Failover: a contact that cannot be reached, or does not
// reply before its deadline, is replaced by the next contact of the same region
// (at most maxRegionAttempts). A final timeout or an error reply is recorded as
// "peer:<id>" TIMEOUT or ERROR; a region with no reachable contact as
// "region:<cpl>@<peer>" UNREACHABLE. Results are merged on the way back.
func forwardGrouped(msg *common.QueryMessage, parentStream network.Stream, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) ([]*common.PartitionPartial, []string) {
	if msg == nil || msg.Ttl <= 0 {
		return nil, nil
	}
	h := kadDHT.Host()
	self := h.ID()
	connected := func(p peer.ID) bool { return h.Network().Connectedness(p) == network.Connected }
	regions := dhtcast.Plan(self, kadDHT.RoutingTable().ListPeers(), int(msg.SubtreeMinCpl), connected)

	base := proto.Clone(msg).(*common.QueryMessage)
	if err := UpdateTTL(base, gm); err != nil { // query bookkeeping; deadlines are set per attempt below
		logger.Errorf("Error updating TTL: %v", err)
	}
	base.Type = common.MessageType_QUERY
	base.Result = nil
	base.PartitionPartials = nil

	var mu sync.Mutex
	var wg sync.WaitGroup
	var entries []*common.PartitionPartial
	responding := map[string]struct{}{}
	traffic := TrafficCounter{Regions: int64(len(regions))}
	for _, r := range regions {
		wg.Add(1)
		go func(r dhtcast.Region) {
			defer wg.Done()
			var tried []string
			for i, p := range r.Peers {
				if i >= maxRegionAttempts || remainingQueryTime(msg) <= 0 {
					break
				}
				if i > 0 {
					atomic.AddInt64(&traffic.Failovers, 1)
				}
				// Each attempt gets ttlReductionFactor of this peer's remaining
				// time, so a timed-out delegate leaves time for one more contact.
				child := proto.Clone(base).(*common.QueryMessage)
				child.SubtreeMinCpl = int32(r.ChildMinCPL())
				budget := time.Duration(float64(remainingQueryTime(msg)) * ttlReductionFactor)
				child.Ttl = float32(budget.Seconds())
				child.Timestamp = time.Now().Add(budget).UTC().Format(time.RFC3339Nano)
				b, err := proto.Marshal(child)
				if err != nil {
					logger.Errorf("Error marshalling grouped query: %v", err)
					return
				}
				resp, status, detail := askPeer(child, p, b, config, kadDHT, &traffic)
				retry := i+1 < len(r.Peers) && i+1 < maxRegionAttempts && remainingQueryTime(msg) > 0
				if status == common.PartitionPartial_UNREACHABLE {
					tried = append(tried, p.String())
					continue // not delivered: try the next contact of this region
				}
				if (status == common.PartitionPartial_TIMEOUT || (status == common.PartitionPartial_ERROR && resp == nil && detail != "malformed reply")) && retry {
					// Delivered but no reply (hung or slow delegate, or the
					// connection broke while waiting): try another
					// contact of the region. Peers the first delegate already
					// reached have claimed the query and answer the retry with no
					// entries, so nothing is counted twice; they show up as not
					// reached in the client's manifest. The delegate itself is
					// reported where the retry meets it again.
					tried = append(tried, p.String()+" ("+status.String()+")")
					continue
				}
				mu.Lock()
				if status == common.PartitionPartial_ANSWERED {
					// A peer that had already claimed this query replies with no
					// entries; with tree propagation this should not happen.
					if len(resp.PartitionPartials) == 0 && len(resp.RespondingPeerIds) == 0 {
						atomic.AddInt64(&traffic.Duplicates, 1)
					}
					entries = append(entries, resp.PartitionPartials...)
					for _, id := range resp.RespondingPeerIds {
						responding[id] = struct{}{}
					}
				} else {
					entries = append(entries, &common.PartitionPartial{PartitionId: "peer:" + p.String(), PeerId: p.String(), Status: status, Detail: detail})
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			entries = append(entries, &common.PartitionPartial{
				PartitionId: fmt.Sprintf("region:%d@%s", r.CPL, self),
				Status:      common.PartitionPartial_UNREACHABLE,
				Detail:      fmt.Sprintf("no reachable contact in DHT region cpl=%d (tried %s)", r.CPL, strings.Join(tried, ", ")),
			})
			mu.Unlock()
		}(r)
	}
	wg.Wait()
	logger.Infof("%s", traffic.LogLine(msg.Uqid))
	return entries, peerIDList(responding)
}

// askPeer sends an encoded query to one peer and reads its reply. UNREACHABLE
// means the query was not delivered (safe to try another peer); TIMEOUT and
// ERROR mean it was delivered but no usable reply arrived.
func askPeer(msg *common.QueryMessage, p peer.ID, b []byte, config flags.Config, kadDHT *dht.IpfsDHT, traffic *TrafficCounter) (*common.QueryMessage, common.PartitionPartial_Status, string) {
	remaining := remainingQueryTime(msg)
	if remaining <= 0 {
		return nil, common.PartitionPartial_TIMEOUT, "no time left"
	}
	dial := remaining / 4
	if dial > 3*time.Second {
		dial = 3 * time.Second
	}
	dctx, dcancel := context.WithTimeout(context.Background(), dial)
	raw, err := kadDHT.Host().NewStream(dctx, p, protocol.ID(config.ProtocolID))
	dcancel()
	if err != nil {
		return nil, common.PartitionPartial_UNREACHABLE, err.Error()
	}
	defer raw.Close()
	atomic.AddInt64(&traffic.Streams, 1)
	stream := &countedStream{Stream: raw, c: traffic}
	if err := helpers.WriteDelimitedMessage(stream, b); err != nil {
		return nil, common.PartitionPartial_UNREACHABLE, err.Error()
	}
	ctx, cancel := context.WithTimeout(context.Background(), remainingQueryTime(msg))
	defer cancel()
	rb, err := helpers.ReadDelimitedMessage(stream, ctx)
	if err != nil {
		// The read deadline is the context deadline; it can fire just before
		// the context reports it.
		if ctx.Err() != nil || errors.Is(err, os.ErrDeadlineExceeded) || strings.Contains(err.Error(), "deadline") {
			return nil, common.PartitionPartial_TIMEOUT, err.Error()
		}
		return nil, common.PartitionPartial_ERROR, err.Error()
	}
	var resp common.QueryMessage
	if err := proto.Unmarshal(rb, &resp); err != nil || resp.Type != common.MessageType_RESULT {
		return nil, common.PartitionPartial_ERROR, "malformed reply"
	}
	if resp.Error != "" {
		return &resp, common.PartitionPartial_ERROR, resp.Error // an error reply: not retried
	}
	return &resp, common.PartitionPartial_ANSWERED, ""
}

// TrafficCounter counts the application-level wire bytes (frames as sent,
// after optional compression, including the length prefix) on the streams a
// peer opens to forward one query. Every stream is counted by its opener only,
// so summing the counters of all peers gives the overlay traffic of a query
// without double counting. Client-to-entry traffic is measured by the client.
type TrafficCounter struct {
	Streams, Duplicates, BytesOut, BytesIn int64
	Regions, Failovers                     int64 // DHT regions delegated; extra contacts tried
}

// LogLine is the line parsed by experiments/gp/collect_metrics.py.
func (t *TrafficCounter) LogLine(uqid string) string {
	return fmt.Sprintf("gptraffic uqi=%s streams=%d dup=%d bytes_out=%d bytes_in=%d regions=%d failover=%d",
		uqid, atomic.LoadInt64(&t.Streams), atomic.LoadInt64(&t.Duplicates),
		atomic.LoadInt64(&t.BytesOut), atomic.LoadInt64(&t.BytesIn),
		atomic.LoadInt64(&t.Regions), atomic.LoadInt64(&t.Failovers))
}

// KindKey is the DHT key under which a peer announces (as a provider record)
// that its partition holds nodes of a kind, e.g. the public registers
// ActivityRecord and MeasurementValue used by join phase 1.
func KindKey(kind string) (cid.Cid, error) {
	hash, err := multihash.Sum([]byte("/idss/gp/kind/"+kind), multihash.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, hash), nil
}

// FindKindProviders returns the peers announcing a node kind in the DHT.
func FindKindProviders(ctx context.Context, kadDHT *dht.IpfsDHT, kind string) []peer.ID {
	key, err := KindKey(kind)
	if err != nil {
		return nil
	}
	var out []peer.ID
	seen := map[peer.ID]bool{}
	for info := range kadDHT.FindProvidersAsync(ctx, key, 0) {
		if info.ID != "" && !seen[info.ID] {
			seen[info.ID] = true
			out = append(out, info.ID)
		}
	}
	return out
}

// countedStream wraps a stream and adds the bytes read and written to a
// TrafficCounter. Embedding keeps SetReadDeadline and the other stream methods.
type countedStream struct {
	network.Stream
	c *TrafficCounter
}

func (s *countedStream) Read(b []byte) (int, error) {
	n, err := s.Stream.Read(b)
	atomic.AddInt64(&s.c.BytesIn, int64(n))
	return n, err
}

func (s *countedStream) Write(b []byte) (int, error) {
	n, err := s.Stream.Write(b)
	atomic.AddInt64(&s.c.BytesOut, int64(n))
	return n, err
}

// RunGroupedOnGraph executes a @group or @join query entirely on one graph, with
// the same parsing, join matching, partial aggregation and finalisation code as
// the overlay. It is the central baseline (tools/baseline): all partitions in
// one EliasDB instance, public events read from the same instance. It returns
// the result header and rows and, for joins, the number of public events.
func RunGroupedOnGraph(query string, gm *graph.Manager) ([]string, [][]string, int, error) {
	groupQuery := query
	var join *gpjoin.Spec
	if gpjoin.IsJoin(query) {
		js, rest, err := gpjoin.Parse(query)
		if err != nil {
			return nil, nil, 0, err
		}
		join, groupQuery = &js, rest
	}
	spec, eqlQuery, err := groupagg.Parse(groupQuery)
	if err != nil {
		return nil, nil, 0, err
	}
	var keep func([][]interface{}) ([][]interface{}, error)
	nEvents := 0
	if join != nil {
		evQuery, err := gpjoin.EventQuery(join.Type)
		if err != nil {
			return nil, nil, 0, err
		}
		evSpec, evEQL, err := groupagg.Parse(evQuery)
		if err != nil {
			return nil, nil, 0, err
		}
		entry, _ := LocalGroupedEntry(evSpec, evEQL, access.Allow, "", nil, "baseline", "", gm, nil)
		if entry.Status != common.PartitionPartial_ANSWERED {
			return nil, nil, 0, fmt.Errorf("event query: %s", entry.Detail)
		}
		merged, err := FoldEntries(evSpec, []*common.PartitionPartial{entry})
		if err != nil {
			return nil, nil, 0, err
		}
		_, evRows := groupagg.Finalise(evSpec, nil, merged)
		keys := make([][]string, len(evRows))
		for i, r := range evRows {
			keys[i] = r[:len(evSpec.Keys)]
		}
		events, err := gpjoin.ParseEvents(join.Type, keys)
		if err != nil {
			return nil, nil, 0, err
		}
		nEvents = len(events)
		keep = gpjoin.NewMatcher(*join, events).Filter
	}
	entry, header := LocalGroupedEntry(spec, eqlQuery, access.Allow, "", nil, "baseline", "", gm, keep)
	if entry.Status != common.PartitionPartial_ANSWERED {
		return nil, nil, nEvents, fmt.Errorf("query: %s", entry.Detail)
	}
	merged, err := FoldEntries(spec, []*common.PartitionPartial{entry})
	if err != nil {
		return nil, nil, nEvents, err
	}
	outHeader, rows := groupagg.Finalise(spec, header, merged)
	return outHeader, rows, nEvents, nil
}
