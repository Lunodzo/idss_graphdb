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
//
// The query state is recorded as in G-IDSS (QUEUED, LOCALLY_EXECUTED,
// COMPLETED, then SENT_BACK at intermediate peers; FAILED on error), progress
// heartbeats are sent upstream while the query runs, and the originator
// stores the result so that the requester can fetch it later ("@result <uqid>").
func HandleGroupedQuery(conn network.Stream, msg *common.QueryMessage, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT, decision access.Decision, denyReason string, hidden func(string) bool) {
	start := time.Now()
	self := kadDHT.Host().ID()
	rs := newReplyStream(conn, msg, gm)
	rs.startHeartbeat()
	defer rs.stopHeartbeat()
	myAddr := kadDHT.Host().Addrs()[0].Encapsulate(multiaddr.StringCast("/p2p/" + self.String())).String()
	isOriginator := msg.Originator == myAddr
	var stored StoredResult
	if isOriginator {
		stored = StoredResult{Uqid: msg.Uqid, Query: msg.Query, Requester: msg.RequesterId,
			State: common.QueryState_QUEUED.String(), Submitted: time.Now().UTC().Format(time.RFC3339)}
		if err := SaveStoredResult(gm, stored); err != nil {
			logger.Errorf("Storing query record %s: %v", msg.Uqid, err)
		}
	}
	fail := func(err error) {
		if isOriginator {
			stored.State, stored.Error = common.QueryState_FAILED.String(), err.Error()
			if serr := SaveStoredResult(gm, stored); serr != nil {
				logger.Errorf("Storing failed query %s: %v", msg.Uqid, serr)
			}
		}
		rs.fail(err)
	}

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

	// @join: if this partition has rows to join, it pulls the public events
	// inside its own service area from the register's providers (pullJoinEvents)
	// and joins locally. Peers without matching rows pull nothing.
	var keep func([][]interface{}) ([][]interface{}, error)
	if join != nil {
		js := *join
		keep = func(rows [][]interface{}) ([][]interface{}, error) {
			events, err := pullJoinEvents(msg, js.Type, config, gm, kadDHT)
			if err != nil {
				return nil, fmt.Errorf("join events unavailable: %v", err)
			}
			return gpjoin.NewMatcher(js, events).Filter(rows)
		}
	}

	var wg sync.WaitGroup
	var local *common.PartitionPartial
	var header []string
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.Now()
		if jt, box, ok := gpjoin.ParseEventBoxQuery(msg.Query); ok {
			// A DSO pulling the public events of its service area: answered
			// from this register's cached event list (no graph scan per pull).
			local, header = boxedEventEntry(spec, jt, box, decision, denyReason, hidden, partition, self, gm)
		} else {
			local, header = LocalGroupedEntry(spec, eqlQuery, decision, denyReason, hidden, partition, self, gm, keep)
		}
		logStepTiming(msg.Uqid, "grouped_local", t)
		rs.setState(common.QueryState_LOCALLY_EXECUTED)
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
		// Reply first, then record the states: the graph writes stay off the
		// critical path of the query (one per tree level otherwise).
		rs.final(reply)
		rs.setState(common.QueryState_COMPLETED)
		rs.setState(common.QueryState_SENT_BACK)
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
	// Reply, then store the result: a client that disconnected (long query)
	// fetches it later with "@result <uqid>".
	rs.final(reply)
	stored.State, stored.Completed = common.QueryState_COMPLETED.String(), time.Now().UTC().Format(time.RFC3339)
	stored.Header, stored.Rows, stored.Manifest = outHeader, rows, manifestEntries(manifest)
	if err := SaveStoredResult(gm, stored); err != nil {
		logger.Errorf("Storing result of %s: %v", msg.Uqid, err)
	}
	rs.setState(common.QueryState_COMPLETED)
}

// eventCacheTTL is how long a register reuses its event list for pulls.
const eventCacheTTL = time.Minute

var (
	eventCacheMu sync.Mutex
	eventCache   = map[string]cachedEvents{} // join type -> full event rows
)

type cachedEvents struct {
	rows   [][]interface{}
	header []string
	at     time.Time
}

// registerEvents returns all public events of a join type held by this peer
// (the rows of gpjoin.EventQuery), cached for eventCacheTTL. Concurrent pulls
// wait for one scan instead of each scanning the graph.
func registerEvents(gm *graph.Manager, joinType string) ([][]interface{}, []string, error) {
	eventCacheMu.Lock()
	defer eventCacheMu.Unlock()
	if c, ok := eventCache[joinType]; ok && time.Since(c.at) < eventCacheTTL {
		return c.rows, c.header, nil
	}
	q, err := gpjoin.EventQuery(joinType)
	if err != nil {
		return nil, nil, err
	}
	_, eqlQuery, err := groupagg.Parse(q)
	if err != nil {
		return nil, nil, err
	}
	rows, header, err := RunIDSSQuery(eqlQuery, "", gm)
	if err != nil && !isEmptyResultError(err) {
		return nil, nil, err
	}
	eventCache[joinType] = cachedEvents{rows, header, time.Now()}
	return rows, header, nil
}

// boxedEventEntry answers a service-area event pull (gpjoin.EventQueryInBox)
// with the same policy checks and result as LocalGroupedEntry would, but from
// the cached event list filtered by the box.
func boxedEventEntry(spec groupagg.Spec, joinType string, box gpjoin.Box, decision access.Decision, denyReason string, hidden func(string) bool, partition string, self peer.ID, gm *graph.Manager) (*common.PartitionPartial, []string) {
	entry := &common.PartitionPartial{PartitionId: partition, PeerId: self.String()}
	if decision == access.Deny {
		entry.Status = common.PartitionPartial_DENIED
		entry.Detail = denyReason
		if entry.Detail == "" {
			entry.Detail = "denied by local access policy"
		}
		return entry, nil
	}
	all, header, err := registerEvents(gm, joinType)
	if err != nil {
		entry.Status, entry.Detail = common.PartitionPartial_ERROR, err.Error()
		return entry, nil
	}
	if col := HiddenColumnUsed(spec, header, hidden); col != "" {
		entry.Status, entry.Detail = common.PartitionPartial_DENIED, "query uses hidden attribute "+col
		return entry, header
	}
	rows := make([][]interface{}, 0, 256)
	for _, r := range all {
		if len(r) < 4 {
			continue
		}
		v := [4]float64{}
		ok := true
		for i := 0; i < 4 && ok; i++ { // minLat, maxLat, minLon, maxLon lead both event queries
			f, err := groupagg.ToFloat(r[i])
			v[i], ok = f, err == nil
		}
		if ok && box.Overlaps(v[0], v[1], v[2], v[3]) {
			rows = append(rows, r)
		}
	}
	partials, err := groupagg.Local(spec, rows)
	if err != nil {
		entry.Status, entry.Detail = common.PartitionPartial_ERROR, err.Error()
		return entry, header
	}
	entry.Status = common.PartitionPartial_ANSWERED
	for _, r := range groupagg.Encode(partials) {
		entry.Partials = append(entry.Partials, &common.Row{Data: r})
	}
	return entry, header
}

// providerTTL is how long a peer reuses the providers found for a node kind.
const providerTTL = time.Minute

var providerCache sync.Map // kind -> cachedProviders

type cachedProviders struct {
	peers []peer.ID
	at    time.Time
}

// kindProviders returns the providers of a node kind, from the cache when fresh.
func kindProviders(kadDHT *dht.IpfsDHT, kind string, budget time.Duration, fresh bool) []peer.ID {
	if c, ok := providerCache.Load(kind); ok && !fresh && time.Since(c.(cachedProviders).at) < providerTTL {
		return c.(cachedProviders).peers
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	ps := FindKindProviders(ctx, kadDHT, kind)
	if len(ps) > 0 {
		providerCache.Store(kind, cachedProviders{ps, time.Now()})
	}
	return ps
}

var serviceAreas sync.Map // *graph.Manager -> serviceArea

type serviceArea struct {
	box gpjoin.Box
	ok  bool
}

// ServiceArea returns the bounding box of every cable segment of this peer
// (its service area), computed once per graph. ok is false when the peer holds
// no segments. GP peers compute it at start-up, so queries do not pay for it.
func ServiceArea(gm *graph.Manager) (gpjoin.Box, bool, error) {
	if a, found := serviceAreas.Load(gm); found {
		return a.(serviceArea).box, a.(serviceArea).ok, nil
	}
	rows, _, err := RunIDSSQuery("get ACLineSegment show minLat, maxLat, minLon, maxLon, lat, lon", "", gm)
	if err != nil && !isEmptyResultError(err) {
		return gpjoin.Box{}, false, err
	}
	var b gpjoin.Box
	ok := false
	for _, r := range rows {
		v := make([]float64, 0, 6)
		for _, c := range r {
			if f, err := groupagg.ToFloat(c); err == nil {
				v = append(v, f)
			}
		}
		if len(v) != 6 {
			continue
		}
		b.Extend(v[0], v[1], v[2], v[3], !ok)
		b.Extend(v[4], v[4], v[5], v[5], false) // midpoint (weather join)
		ok = true
	}
	serviceAreas.Store(gm, serviceArea{b, ok})
	return b, ok, nil
}

// pullJoinEvents fetches, for this peer only, the public events of a join type
// that overlap its service area: a grouped event query restricted to the area
// is sent directly (as a leaf) to the providers of the register's node kind,
// found by DHT provider records. The request is signed with this peer's key and
// reveals only the service area, which is public for a DSO (concession area);
// no cable geometry leaves the peer. Any provider that does not answer makes
// the pull fail, so the partition reports an error instead of a partial join.
func pullJoinEvents(msg *common.QueryMessage, joinType string, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) ([]gpjoin.Event, error) {
	start := time.Now()
	box, ok, err := ServiceArea(gm)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // no segments, nothing can match
	}
	kind, err := gpjoin.EventKind(joinType)
	if err != nil {
		return nil, err
	}
	query, err := gpjoin.EventQueryInBox(joinType, box)
	if err != nil {
		return nil, err
	}
	spec, _, err := groupagg.Parse(query)
	if err != nil {
		return nil, err
	}
	budget := time.Duration(float64(remainingQueryTime(msg)) * 0.4)
	if budget <= 0 {
		return nil, fmt.Errorf("no time left to fetch %s events", kind)
	}
	h := kadDHT.Host()
	self := h.ID()
	var traffic TrafficCounter
	defer func() { logger.Infof("%s", traffic.LogLine(msg.Uqid+"-events")) }()
	attempt := 0
	ask := func(providers []peer.ID) ([]*common.PartitionPartial, error) {
		attempt++
		p1 := &common.QueryMessage{
			Type:  common.MessageType_QUERY,
			Query: query,
			// One UQI per pulling peer and attempt: a retry under the same UQI
			// would be answered empty by a provider that already claimed it.
			Uqid:          fmt.Sprintf("%s-events-%s-%d", msg.Uqid, self, attempt),
			Ttl:           float32(budget.Seconds()),
			Timestamp:     time.Now().Add(budget).UTC().Format(time.RFC3339Nano),
			Originator:    h.Addrs()[0].Encapsulate(multiaddr.StringCast("/p2p/" + self.String())).String(),
			Sender:        self.String(),
			RequesterId:   self.String(),
			State:         &common.QueryState{State: common.QueryState_QUEUED},
			SubtreeMinCpl: dhtcast.Leaf, // providers answer without forwarding
		}
		if priv := h.Peerstore().PrivKey(self); priv != nil {
			if err := identity.Sign(p1, priv); err != nil {
				return nil, err
			}
		}
		b, err := proto.Marshal(p1)
		if err != nil {
			return nil, err
		}
		var mu sync.Mutex
		var wg sync.WaitGroup
		var entries []*common.PartitionPartial
		var failed []string
		for _, p := range providers {
			if p == self {
				continue // a register never pulls from itself (it holds no segments)
			}
			wg.Add(1)
			go func(p peer.ID) {
				defer wg.Done()
				resp, status, detail := askPeer(p1, p, b, config, kadDHT, &traffic)
				mu.Lock()
				defer mu.Unlock()
				if status != common.PartitionPartial_ANSWERED {
					failed = append(failed, fmt.Sprintf("%s %s (%s)", p, status, detail))
					return
				}
				if len(resp.PartitionPartials) == 0 { // e.g. a duplicate claim: no data, not an answer
					failed = append(failed, fmt.Sprintf("%s returned no partition entry", p))
					return
				}
				for _, e := range resp.PartitionPartials {
					if e.Status != common.PartitionPartial_ANSWERED {
						failed = append(failed, fmt.Sprintf("%s %s (%s)", e.PartitionId, e.Status, e.Detail))
					}
				}
				entries = append(entries, resp.PartitionPartials...)
			}(p)
		}
		wg.Wait()
		if len(failed) > 0 {
			return nil, fmt.Errorf("%s providers did not answer: %s", kind, strings.Join(failed, "; "))
		}
		return entries, nil
	}

	lookup := time.Now()
	providers := kindProviders(kadDHT, kind, budget/2, false)
	logStepTiming(msg.Uqid, "provider_lookup", lookup)
	if len(providers) == 0 {
		return nil, fmt.Errorf("no provider of %s found in the DHT", kind)
	}
	entries, err := ask(providers)
	if err != nil { // stale cache entry? look up once more
		if providers = kindProviders(kadDHT, kind, budget/2, true); len(providers) > 0 {
			entries, err = ask(providers)
		}
	}
	if err != nil {
		return nil, err
	}
	merged, err := FoldEntries(spec, UnionEntries(entries))
	if err != nil {
		return nil, err
	}
	_, rows := groupagg.Finalise(spec, nil, merged)
	keys := make([][]string, len(rows))
	for i, r := range rows {
		keys[i] = r[:len(spec.Keys)] // key cells = the event; drop the count
	}
	events, err := gpjoin.ParseEvents(joinType, keys)
	if err != nil {
		return nil, err
	}
	logger.Infof("Join %s pull: %d %s events from %d providers in %v", msg.Uqid, len(events), joinType, len(providers), time.Since(start))
	logStepTiming(msg.Uqid, "join_pull", start)
	return events, nil
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
	// A hung peer may never accept the data: bound the write as well.
	wd := idleTimeout()
	if r := remainingQueryTime(msg); r < wd {
		wd = r
	}
	_ = raw.SetWriteDeadline(time.Now().Add(wd))
	if err := helpers.WriteDelimitedMessage(stream, b); err != nil {
		return nil, common.PartitionPartial_UNREACHABLE, err.Error()
	}
	_ = raw.SetWriteDeadline(time.Time{})
	// Read frames until the final reply. Heartbeats (progress) reset the idle
	// timer: a working subtree may take until the query deadline, a silent one
	// is treated as hung after idleTimeout so that the region can fail over.
	idle := idleTimeout()
	var resp common.QueryMessage
	for {
		wait := remainingQueryTime(msg)
		if wait <= 0 {
			return nil, common.PartitionPartial_TIMEOUT, "query deadline reached"
		}
		silent := false
		if wait > idle {
			wait, silent = idle, true
		}
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		rb, err := helpers.ReadDelimitedMessage(stream, ctx)
		ctxErr := ctx.Err()
		cancel()
		if err != nil {
			// The read deadline is the context deadline; it can fire just before
			// the context reports it.
			if ctxErr != nil || errors.Is(err, os.ErrDeadlineExceeded) || strings.Contains(err.Error(), "deadline") {
				if silent {
					return nil, common.PartitionPartial_TIMEOUT, fmt.Sprintf("no progress for %v", idle)
				}
				return nil, common.PartitionPartial_TIMEOUT, err.Error()
			}
			return nil, common.PartitionPartial_ERROR, err.Error()
		}
		resp.Reset()
		if err := proto.Unmarshal(rb, &resp); err != nil || resp.Type != common.MessageType_RESULT {
			return nil, common.PartitionPartial_ERROR, "malformed reply"
		}
		if IsHeartbeat(&resp) {
			atomic.AddInt64(&traffic.Heartbeats, 1)
			continue
		}
		break
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
	Heartbeats                             int64 // progress frames received from delegates
}

// LogLine is the line parsed by experiments/gp/collect_metrics.py.
func (t *TrafficCounter) LogLine(uqid string) string {
	return fmt.Sprintf("gptraffic uqi=%s streams=%d dup=%d bytes_out=%d bytes_in=%d regions=%d failover=%d heartbeats=%d",
		uqid, atomic.LoadInt64(&t.Streams), atomic.LoadInt64(&t.Duplicates),
		atomic.LoadInt64(&t.BytesOut), atomic.LoadInt64(&t.BytesIn),
		atomic.LoadInt64(&t.Regions), atomic.LoadInt64(&t.Failovers), atomic.LoadInt64(&t.Heartbeats))
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

// providerGrace is how long a provider lookup keeps listening after the last
// new provider arrived. A full lookup can stall on an unresponsive DHT peer
// (its RPC timeout), while provider records usually come from the first peers
// asked; the lookup ends at its context deadline at the latest.
const providerGrace = time.Second

// FindKindProviders returns the peers announcing a node kind in the DHT.
func FindKindProviders(ctx context.Context, kadDHT *dht.IpfsDHT, kind string) []peer.ID {
	key, err := KindKey(kind)
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var out []peer.ID
	seen := map[peer.ID]bool{}
	ch := kadDHT.FindProvidersAsync(ctx, key, 0)
	var grace <-chan time.Time
	for {
		select {
		case info, ok := <-ch:
			if !ok {
				return out
			}
			if info.ID != "" && !seen[info.ID] {
				seen[info.ID] = true
				out = append(out, info.ID)
				grace = time.After(providerGrace)
			}
		case <-grace:
			return out
		case <-ctx.Done():
			return out
		}
	}
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
