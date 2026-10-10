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
	"sync/atomic"
	"time"

	"idss/graphdb/access"
	"idss/graphdb/common"
	"idss/graphdb/flags"
	"idss/graphdb/gpjoin"
	"idss/graphdb/groupagg"
	"idss/graphdb/helpers"
	"idss/graphdb/identity"

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
	if join != nil {
		if isOriginator && len(msg.JoinEvents) == 0 {
			p1 := time.Now()
			events, err := collectJoinEvents(msg, join.Type, config, gm, kadDHT)
			if err != nil {
				fail(fmt.Errorf("join phase 1: %v", err))
				return
			}
			msg.JoinEvents = events
			logger.Infof("Join %s phase 1: %d %s events in %v", msg.Uqid, len(events), join.Type, time.Since(p1))
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

// collectJoinEvents runs the phase-1 grouped query for a join type (every
// distinct public event is one group) over the overlay and returns the event
// rows. Phase 1 uses at most 40 % of the remaining query time.
func collectJoinEvents(msg *common.QueryMessage, joinType string, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) ([]*common.Row, error) {
	query, err := gpjoin.EventQuery(joinType)
	if err != nil {
		return nil, err
	}
	spec, eqlQuery, err := groupagg.Parse(query)
	if err != nil {
		return nil, err
	}
	budget := time.Duration(float64(remainingQueryTime(msg)) * 0.4)
	if budget <= 0 {
		return nil, fmt.Errorf("no time left for phase 1")
	}
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
			return nil, fmt.Errorf("signing phase-1 query: %v", err)
		}
	}
	StoreQueryInfo(p1, gm, self.String())
	local, _ := LocalGroupedEntry(spec, eqlQuery, access.Allow, "", nil, PartitionLabel(config, self), self, gm, nil)
	remote, _ := forwardGrouped(p1, nil, config, gm, kadDHT)
	entries := UnionEntries([]*common.PartitionPartial{local}, remote)
	logger.Infof("Join phase 1 partitions: %s", ManifestSummary(entries))
	merged, err := FoldEntries(spec, entries)
	if err != nil {
		return nil, err
	}
	_, rows := groupagg.Finalise(spec, nil, merged)
	out := make([]*common.Row, len(rows))
	for i, r := range rows {
		out[i] = &common.Row{Data: r[:len(spec.Keys)]} // key cells = the event; drop the count
	}
	return out, nil
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
	var traffic TrafficCounter

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
			raw, err := kadDHT.Host().NewStream(ctx, p, protocol.ID(config.ProtocolID))
			if err != nil {
				fail(p, common.PartitionPartial_UNREACHABLE, err.Error())
				return
			}
			defer raw.Close()
			atomic.AddInt64(&traffic.Streams, 1)
			stream := &countedStream{Stream: raw, c: &traffic}
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
			if len(resp.PartitionPartials) == 0 && len(resp.RespondingPeerIds) == 0 {
				atomic.AddInt64(&traffic.Duplicates, 1)
			}
			mu.Lock()
			entries = append(entries, resp.PartitionPartials...)
			for _, id := range resp.RespondingPeerIds {
				responding[id] = struct{}{}
			}
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	logger.Infof("%s", traffic.LogLine(msg.Uqid))
	return entries, peerIDList(responding)
}

// TrafficCounter counts the application-level wire bytes (frames as sent,
// after optional compression, including the length prefix) on the streams a
// peer opens to forward one query. Every stream is counted by its opener only,
// so summing the counters of all peers gives the overlay traffic of a query
// without double counting. Client-to-entry traffic is measured by the client.
type TrafficCounter struct {
	Streams, Duplicates, BytesOut, BytesIn int64
}

// LogLine is the line parsed by experiments/gp/collect_metrics.py.
func (t *TrafficCounter) LogLine(uqid string) string {
	return fmt.Sprintf("gptraffic uqi=%s streams=%d dup=%d bytes_out=%d bytes_in=%d",
		uqid, atomic.LoadInt64(&t.Streams), atomic.LoadInt64(&t.Duplicates),
		atomic.LoadInt64(&t.BytesOut), atomic.LoadInt64(&t.BytesIn))
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
