/*
 * ! \file community.go
 * Energy-community membership for IDSS peers.
 *
 * A peer started with -community <id> belongs to that energy community; with
 * -manager as well it is the community's manager. Membership then works as
 * follows:
 *
 *   1. The manager records an EnergyCommunity node and announces itself as
 *      the provider of the community's DHT key.
 *   2. Each member looks the key up (or uses a pinned -community-manager),
 *      and registers its customers with that manager over the IDSS protocol.
 *      The manager stores them as Customer nodes linked to the
 *      EnergyCommunity node by memberOf edges, and keeps the member peers in
 *      its registry. The member remembers the manager that accepted it.
 *   3. Settlement ("settle <from> <to>") asks exactly the registered member
 *      peers. A member answers only if the request names its own community
 *      and comes from the manager that accepted its registration (the peer
 *      identity is authenticated by the libp2p secure channel), and only if
 *      its access policy does not deny the manager role; it returns
 *      aggregate totals, never raw readings.
 *
 * "settle-unscoped <from> <to> [community]" is a jurisdiction probe: it sends
 * the request to the manager's whole routing table (as unscoped settlement
 * does), optionally claiming another community, so that experiments can show
 * that peers outside the community refuse.
 *
 * Peers started without -community keep the original, unscoped behaviour.
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"idss/graphdb/access"
	"idss/graphdb/broadcast"
	"idss/graphdb/common"
	"idss/graphdb/flags"
	"idss/graphdb/helpers"

	"github.com/ipfs/go-cid"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/proto"
)

// memberState is a member peer's view of its community: the manager that
// accepted its registration (empty until registered).
type memberState struct {
	mu        sync.RWMutex
	managerID peer.ID
}

func (m *memberState) manager() peer.ID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.managerID
}

func (m *memberState) setManager(id peer.ID) {
	m.mu.Lock()
	m.managerID = id
	m.mu.Unlock()
}

// communityRegistry is a manager's list of registered member peers and the
// customers each one hosts.
type communityRegistry struct {
	mu      sync.Mutex
	members map[peer.ID]map[string]struct{}
}

// add records one customer of a member peer; it reports whether the peer is
// new, and the registry size after the update.
func (r *communityRegistry) add(member peer.ID, mrid string) (bool, int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	customers, known := r.members[member]
	if !known {
		customers = make(map[string]struct{})
		r.members[member] = customers
	}
	customers[mrid] = struct{}{}
	total := 0
	for _, c := range r.members {
		total += len(c)
	}
	return !known, len(r.members), total
}

// memberPeers returns the registered member peers in a stable order.
func (r *communityRegistry) memberPeers() []peer.ID {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]peer.ID, 0, len(r.members))
	for id := range r.members {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

var (
	membership = &memberState{}
	registry   = &communityRegistry{members: make(map[peer.ID]map[string]struct{})}
)

// communityKey derives the DHT key under which a community's manager is announced.
func communityKey(communityID string) (cid.Cid, error) {
	hash, err := multihash.Sum([]byte("/idss/energy-community/"+communityID), multihash.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, hash), nil
}

func communityNodeKey(communityID string) string {
	return "community-" + communityID
}

// startCommunityManager records the community in the manager's graph and
// announces the manager in the DHT, retrying until the announcement succeeds
// and refreshing it periodically.
func startCommunityManager(ctx context.Context, h host.Host, kadDHT *dht.IpfsDHT, config flags.Config, gm *graph.Manager) {
	community := data.NewGraphNode()
	community.SetAttr("key", communityNodeKey(config.CommunityID))
	community.SetAttr("kind", "EnergyCommunity")
	community.SetAttr("mRID", communityNodeKey(config.CommunityID))
	community.SetAttr("name", "Energy community "+config.CommunityID)
	community.SetAttr("communityId", config.CommunityID)
	community.SetAttr("managerPeer", h.ID().String())
	if err := gm.StoreNode("main", community); err != nil {
		logger.Errorf("Error storing EnergyCommunity node: %v", err)
	}
	logger.Infof("Community manager for %s", config.CommunityID)

	key, err := communityKey(config.CommunityID)
	if err != nil {
		logger.Errorf("Cannot derive DHT key for community %s: %v", config.CommunityID, err)
		return
	}
	go func() {
		announce := func() error {
			pctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			return kadDHT.Provide(pctx, key, true)
		}
		for {
			if err := announce(); err == nil {
				logger.Infof("Announced community %s in the DHT", config.CommunityID)
				break
			} else {
				logger.Debugf("Announcing community %s failed, retrying: %v", config.CommunityID, err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := announce(); err != nil {
					logger.Warnf("Re-announcing community %s failed: %v", config.CommunityID, err)
				}
			}
		}
	}()
}

// startCommunityMember finds the community's manager and registers this
// peer's customers with it, retrying until one manager accepts them all.
func startCommunityMember(ctx context.Context, h host.Host, kadDHT *dht.IpfsDHT, config flags.Config, gm *graph.Manager) {
	logger.Infof("Community member of %s", config.CommunityID)
	key, err := communityKey(config.CommunityID)
	if err != nil {
		logger.Errorf("Cannot derive DHT key for community %s: %v", config.CommunityID, err)
		return
	}
	registrations, err := localCustomerRegistrations(h, config, gm)
	if err != nil {
		logger.Errorf("Error reading local customers for registration: %v", err)
		return
	}
	started := time.Now()
	for attempt := 1; ; attempt++ {
		candidates := findCommunityManagers(ctx, h, kadDHT, key, config)
		if len(candidates) == 0 && attempt%10 == 0 {
			logger.Infof("No manager found yet for %s (attempt %d, %.1f s)", config.CommunityID, attempt, time.Since(started).Seconds())
		}
		for _, candidate := range candidates {
			accepted, regErr := registerWithManager(ctx, h, config, candidate, registrations)
			if regErr == nil && accepted == len(registrations) {
				membership.setManager(candidate.ID)
				logger.Infof("Registered with community manager %s for %s (%d customers, attempt %d, %.1f s)",
					candidate.ID, config.CommunityID, accepted, attempt, time.Since(started).Seconds())
				return
			}
			logger.Infof("Registration with %s for %s incomplete (%d/%d accepted): %v",
				candidate.ID, config.CommunityID, accepted, len(registrations), regErr)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

// findCommunityManagers returns candidate managers: the pinned one if set,
// otherwise the providers of the community's DHT key.
func findCommunityManagers(ctx context.Context, h host.Host, kadDHT *dht.IpfsDHT, key cid.Cid, config flags.Config) []peer.AddrInfo {
	fctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if config.CommunityManager != "" {
		pinned, err := peer.Decode(config.CommunityManager)
		if err != nil {
			logger.Errorf("Invalid -community-manager %q: %v", config.CommunityManager, err)
			return nil
		}
		info, err := kadDHT.FindPeer(fctx, pinned)
		if err != nil {
			return nil
		}
		return []peer.AddrInfo{info}
	}
	var candidates []peer.AddrInfo
	for info := range kadDHT.FindProvidersAsync(fctx, key, 3) {
		if info.ID != h.ID() && info.ID != "" {
			candidates = append(candidates, info)
		}
	}
	return candidates
}

// localCustomerRegistrations lists this peer's own customers (not the local
// "Community Manager" placeholder the data generator creates on every peer).
func localCustomerRegistrations(h host.Host, config flags.Config, gm *graph.Manager) ([]*common.CustomerRegistration, error) {
	rows, header, err := broadcast.RunIDSSQuery("get Customer", h.ID(), gm)
	if err != nil {
		return nil, err
	}
	index := make(map[string]int)
	for position, label := range header {
		index[strings.ToLower(label)] = position
	}
	field := func(row []interface{}, name string) string {
		position, ok := index[name]
		if !ok || position >= len(row) || row[position] == nil {
			return ""
		}
		return fmt.Sprint(row[position])
	}
	var registrations []*common.CustomerRegistration
	for _, row := range rows {
		if field(row, "role") == "manager" || field(row, "mrid") == "" {
			continue
		}
		registrations = append(registrations, &common.CustomerRegistration{
			Mrid: field(row, "mrid"), Name: field(row, "name"), Role: field(row, "role"),
			MembershipStatus: field(row, "membershipstatus"), ContractNumber: field(row, "contractnumber"),
			CommunityId: config.CommunityID,
		})
	}
	return registrations, nil
}

// registerWithManager sends each registration to the manager on one stream
// and counts the acknowledgements that accept it.
func registerWithManager(ctx context.Context, h host.Host, config flags.Config, manager peer.AddrInfo, registrations []*common.CustomerRegistration) (int, error) {
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if len(manager.Addrs) > 0 {
		if err := h.Connect(rctx, manager); err != nil {
			return 0, err
		}
	}
	stream, err := h.NewStream(rctx, manager.ID, protocol.ID(config.ProtocolID))
	if err != nil {
		return 0, err
	}
	defer stream.Close()
	accepted := 0
	for _, registration := range registrations {
		payload, err := proto.Marshal(&common.QueryMessage{Type: common.MessageType_CUSTOMER_REGISTRATION, CustomerRegistration: registration})
		if err != nil {
			return accepted, err
		}
		if err := helpers.WriteDelimitedMessage(stream, payload); err != nil {
			return accepted, err
		}
		ackData, err := helpers.ReadDelimitedMessage(stream, rctx)
		if err != nil {
			return accepted, err
		}
		ack := &common.QueryMessage{}
		if err := proto.Unmarshal(ackData, ack); err != nil {
			return accepted, err
		}
		if ack.Type != common.MessageType_REGISTRATION_ACK || ack.Error != "" {
			return accepted, fmt.Errorf("registration of %s rejected: %s", registration.Mrid, ack.Error)
		}
		accepted++
		if accepted == 1 {
			// The manager has admitted this peer to its registry: answer its
			// settlement requests from now on, even while the remaining
			// customers are still being registered (or retried).
			membership.setManager(manager.ID)
		}
	}
	return accepted, nil
}

// handleCommunityRegistration runs on a peer with a community. A manager
// stores a registration for its own community, linked to its EnergyCommunity
// node, and records the (authenticated) sending peer as a member; anything
// else is rejected. Every registration is acknowledged on the same stream.
func handleCommunityRegistration(conn network.Stream, msg *common.QueryMessage, remotePeerID string, config flags.Config, gm *graph.Manager) {
	registration := msg.CustomerRegistration
	ack := &common.QueryMessage{Type: common.MessageType_REGISTRATION_ACK, Query: config.CommunityID}
	switch {
	case !config.IsManager:
		ack.Error = "this peer is not a community manager"
	case registration == nil || registration.Mrid == "":
		ack.Error = "empty registration"
	case registration.CommunityId != config.CommunityID:
		ack.Error = fmt.Sprintf("community %q is not managed by this peer (manages %q)", registration.CommunityId, config.CommunityID)
	default:
		if err := storeMembership(gm, config.CommunityID, remotePeerID, registration); err != nil {
			ack.Error = "storing registration failed"
			logger.Errorf("Error storing registration %s: %v", registration.Mrid, err)
			break
		}
		memberID, err := peer.Decode(remotePeerID)
		if err != nil {
			ack.Error = "invalid member peer ID"
			break
		}
		isNew, memberPeers, customers := registry.add(memberID, registration.Mrid)
		if isNew {
			logger.Infof("Community %s registry: member peer %s joined (%d member peers, %d customers)",
				config.CommunityID, remotePeerID, memberPeers, customers)
		}
	}
	if ack.Error != "" {
		logger.Infof("Registration from %s rejected: %s", remotePeerID, ack.Error)
	}
	if payload, err := proto.Marshal(ack); err == nil {
		_ = helpers.WriteDelimitedMessage(conn, payload)
	}
}

// storeMembership writes the registered customer and its memberOf edge to
// the community's EnergyCommunity node.
func storeMembership(gm *graph.Manager, communityID string, hostPeer string, registration *common.CustomerRegistration) error {
	customer := data.NewGraphNode()
	customer.SetAttr("key", registration.Mrid)
	customer.SetAttr("kind", "Customer")
	customer.SetAttr("mRID", registration.Mrid)
	customer.SetAttr("name", registration.Name)
	customer.SetAttr("role", registration.Role)
	customer.SetAttr("membershipStatus", registration.MembershipStatus)
	customer.SetAttr("contractNumber", registration.ContractNumber)
	customer.SetAttr("communityId", communityID)
	customer.SetAttr("hostPeer", hostPeer)
	if err := gm.StoreNode("main", customer); err != nil {
		return err
	}
	edge := data.NewGraphEdge()
	edge.SetAttr("key", "membership-"+communityID+"-"+registration.Mrid)
	edge.SetAttr("kind", "memberOf")
	edge.SetAttr(data.EdgeEnd1Key, registration.Mrid)
	edge.SetAttr(data.EdgeEnd1Kind, "Customer")
	edge.SetAttr(data.EdgeEnd1Role, "member")
	edge.SetAttr(data.EdgeEnd1Cascading, false)
	edge.SetAttr(data.EdgeEnd2Key, communityNodeKey(communityID))
	edge.SetAttr(data.EdgeEnd2Kind, "EnergyCommunity")
	edge.SetAttr(data.EdgeEnd2Role, "community")
	edge.SetAttr(data.EdgeEnd2Cascading, false)
	return gm.StoreEdge("main", edge)
}

// handleScopedSettlementRequest answers a settlement request on a peer that
// belongs to a community, or refuses it with a reason.
func handleScopedSettlementRequest(conn network.Stream, msg *common.QueryMessage, remotePeerID string, config flags.Config, gm *graph.Manager, h host.Host, accessPolicy *access.Policy) {
	request := msg.SettlementRequest
	if request == nil {
		return
	}
	reply := func(result *common.SettlementResult) {
		payload, err := proto.Marshal(&common.QueryMessage{Type: common.MessageType_SETTLEMENT_RESULT, SettlementResult: result})
		if err == nil {
			_ = helpers.WriteDelimitedMessage(conn, payload)
		}
	}
	refuse := func(reason string) {
		logger.Infof("Settlement request REFUSED from %s: %s", remotePeerID, reason)
		reply(&common.SettlementResult{PeerId: h.ID().String(), CommunityId: config.CommunityID, Refusal: reason})
	}

	manager := membership.manager()
	switch {
	case request.CommunityId != config.CommunityID:
		refuse(fmt.Sprintf("community mismatch: request settles %q, this peer belongs to %q", request.CommunityId, config.CommunityID))
		return
	case config.IsManager:
		refuse("this peer is a community manager, not a member")
		return
	case manager == "":
		refuse("not registered with a community manager yet")
		return
	case remotePeerID != manager.String():
		refuse(fmt.Sprintf("requester is not the registered manager of %s", config.CommunityID))
		return
	case accessPolicy != nil && accessPolicy.Evaluate([]string{"MeterReading", "Trade"}, "manager") == access.Deny:
		refuse("access policy denies settlement data to the manager role")
		return
	}

	from, err := time.Parse(time.RFC3339, request.From)
	if err != nil {
		refuse("invalid period start")
		return
	}
	to, err := time.Parse(time.RFC3339, request.To)
	if err != nil {
		refuse("invalid period end")
		return
	}
	meterSum, tradeSum, err := broadcast.LocalSettlementTotals(gm, h.ID(), from, to)
	if err != nil {
		logger.Errorf("Error computing settlement totals: %v", err)
		refuse("local totals unavailable")
		return
	}
	logger.Infof("Settlement request ANSWERED for %s (manager %s)", config.CommunityID, remotePeerID)
	reply(&common.SettlementResult{PeerId: h.ID().String(), MeterReadingSum: meterSum, TradeVolumeSum: tradeSum, CommunityId: config.CommunityID})
}

// handleCommunitySettlementCommand runs "settle" (registered members) or the
// "settle-unscoped" jurisdiction probe on a community manager.
func handleCommunitySettlementCommand(conn network.Stream, msg *common.QueryMessage, remotePeerID string, config flags.Config, gm *graph.Manager, kadDHT *dht.IpfsDHT) {
	if !config.IsManager || msg.RequesterRole != "manager" {
		helpers.SendErrorMessage(conn, peer.ID(remotePeerID), "settle is available only to a manager client connected to a manager peer")
		return
	}
	parts := strings.Fields(msg.Query)
	command := strings.ToLower(parts[0])
	if (command == "settle" && len(parts) != 3) || (command == "settle-unscoped" && len(parts) != 3 && len(parts) != 4) {
		helpers.SendErrorMessage(conn, peer.ID(remotePeerID), "invalid command: expected settle <from> <to> or settle-unscoped <from> <to> [community]")
		return
	}
	from, err := time.Parse(time.RFC3339, parts[1])
	if err != nil {
		helpers.SendErrorMessage(conn, peer.ID(remotePeerID), err.Error())
		return
	}
	to, err := time.Parse(time.RFC3339, parts[2])
	if err != nil {
		helpers.SendErrorMessage(conn, peer.ID(remotePeerID), err.Error())
		return
	}

	scope := "registry"
	targets := registry.memberPeers()
	requestCommunity := config.CommunityID
	if command == "settle-unscoped" {
		scope = "routing-table"
		targets = kadDHT.RoutingTable().ListPeers()
		if len(parts) == 4 {
			requestCommunity = parts[3]
			scope = "routing-table-claiming-" + requestCommunity
		}
	}
	stats, err := broadcast.CompileCommunitySettlement(gm, kadDHT, protocol.ID(config.ProtocolID),
		config.CommunityID, requestCommunity, scope, targets, from, to)
	if err != nil {
		helpers.SendErrorMessage(conn, peer.ID(remotePeerID), err.Error())
		return
	}
	sendSuccessMessage(conn, remotePeerID, "Settlement summaries compiled: "+stats.Summary(), kadDHT)
}
