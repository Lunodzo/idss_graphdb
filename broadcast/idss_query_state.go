/*
 * ! \file idss_query_state.go
 * Query states, progress heartbeats and stored results for long-running
 * Green Power (@group / @join) queries.
 *
 * As in the original G-IDSS design, every peer records the state of each query
 * it handles (QUEUED -> LOCALLY_EXECUTED -> COMPLETED -> SENT_BACK, or FAILED)
 * and the initiator stores the final result, so that a client can disconnect
 * and fetch the result later by its UQI ("@result <uqid>").
 *
 * While a query runs, each peer sends progress heartbeats to its parent (and
 * the initiator to the client): RESULT messages with RecordCount -1 carrying
 * the peer's current state. A parent therefore distinguishes a slow subtree
 * (heartbeats keep arriving, it waits up to the query deadline) from a hung
 * delegate (silent for the idle timeout, it fails over at once), which keeps
 * failover fast even for queries with a TTL of hours.
 */

package broadcast

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"idss/graphdb/common"
	"idss/graphdb/helpers"

	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/libp2p/go-libp2p/core/network"
	"google.golang.org/protobuf/proto"
)

// heartbeatInterval is how often a running peer reports progress upstream.
const heartbeatInterval = 2 * time.Second

// HeartbeatRecordCount marks a progress message (never a final result).
const HeartbeatRecordCount = -1

// idleTimeout is how long a parent waits without any frame from a delegate
// before treating it as hung (IDSS_GP_IDLE_TIMEOUT, Go duration; default 10 s).
func idleTimeout() time.Duration {
	if v := os.Getenv("IDSS_GP_IDLE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 10 * time.Second
}

// IsHeartbeat reports whether a reply frame is a progress heartbeat.
func IsHeartbeat(m *common.QueryMessage) bool {
	return m != nil && m.Type == common.MessageType_RESULT && m.RecordCount == HeartbeatRecordCount && m.Error == ""
}

// replyStream serialises everything a peer writes back on one stream (progress
// heartbeats from a goroutine, then the final reply or an error) and records
// each state change of the query in the local graph.
type replyStream struct {
	conn    network.Stream
	msg     *common.QueryMessage
	gm      *graph.Manager
	mu      sync.Mutex
	state   atomic.Int32
	stop    chan struct{}
	done    sync.WaitGroup
	stopped sync.Once
}

func newReplyStream(conn network.Stream, msg *common.QueryMessage, gm *graph.Manager) *replyStream {
	r := &replyStream{conn: conn, msg: msg, gm: gm, stop: make(chan struct{})}
	r.setState(common.QueryState_QUEUED)
	return r
}

// setState records a state change locally (graph) and in the next heartbeat.
func (r *replyStream) setState(s common.QueryState_State) {
	r.state.Store(int32(s))
	if err := SetQueryNodeState(r.gm, r.msg.Uqid, s); err != nil {
		logger.Errorf("Recording state %s of %s: %v", s, r.msg.Uqid, err)
	}
}

// QueryStatusKind is the node kind holding the live state of a query at a peer.
const QueryStatusKind = "QueryStatus"

// SetQueryNodeState records the state of a query at this peer in a small
// "QueryStatus" node (key = UQI: state and time only). Updating the full
// "Query" node instead re-indexes its long attributes (query text, results)
// on every transition, which measurably slowed queries; the shared
// UpdateQueryState of the energy-community path also omits the node kind,
// so EliasDB silently drops its updates and the state stays QUEUED.
func SetQueryNodeState(gm *graph.Manager, uqid string, s common.QueryState_State) error {
	if gm == nil || strings.TrimSpace(uqid) == "" {
		return nil
	}
	n := data.NewGraphNode()
	n.SetAttr("key", uqid)
	n.SetAttr("kind", QueryStatusKind)
	n.SetAttr("state", s.String())
	n.SetAttr("updated", time.Now().UTC().Format(time.RFC3339Nano))
	trans := graph.NewGraphTrans(gm)
	if err := trans.StoreNode("main", n); err != nil {
		return err
	}
	return trans.Commit()
}

// QueryNodeState returns the recorded state of a query at this peer, or "".
func QueryNodeState(gm *graph.Manager, uqid string) string {
	n, err := gm.FetchNode("main", uqid, QueryStatusKind)
	if err != nil || n == nil || n.Attr("state") == nil {
		return ""
	}
	return fmt.Sprint(n.Attr("state"))
}

func (r *replyStream) write(m *common.QueryMessage) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return helpers.WriteDelimitedMessage(r.conn, b)
}

// startHeartbeat sends one heartbeat at once (receipt acknowledgement) and then
// one every heartbeatInterval until stopHeartbeat is called.
func (r *replyStream) startHeartbeat() {
	beat := func() bool {
		hb := &common.QueryMessage{Type: common.MessageType_RESULT, Uqid: r.msg.Uqid, RecordCount: HeartbeatRecordCount,
			State: &common.QueryState{State: common.QueryState_State(r.state.Load())}}
		return r.write(hb) == nil
	}
	r.done.Add(1)
	go func() {
		defer r.done.Done()
		if !beat() {
			return
		}
		t := time.NewTicker(heartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-t.C:
				if !beat() {
					return // parent or client gone; the query itself continues
				}
			}
		}
	}()
}

func (r *replyStream) stopHeartbeat() {
	r.stopped.Do(func() { close(r.stop) })
	r.done.Wait()
}

// final stops the heartbeats and sends the final reply.
func (r *replyStream) final(m *common.QueryMessage) {
	r.stopHeartbeat()
	if err := r.write(m); err != nil {
		logger.Warnf("Could not send result of %s upstream (requester may fetch it later): %v", r.msg.Uqid, err)
	}
}

// fail records FAILED, stops the heartbeats and sends an error reply.
func (r *replyStream) fail(err error) {
	r.setState(common.QueryState_FAILED)
	r.stopHeartbeat()
	logger.Errorf("Query %s failed: %v", r.msg.Uqid, err)
	if werr := r.write(&common.QueryMessage{Type: common.MessageType_RESULT, Error: err.Error()}); werr != nil {
		logger.Warnf("Could not send error of %s upstream: %v", r.msg.Uqid, werr)
	}
}

// ---------------------------------------------------------------------------
// Stored results at the initiator

// StoredResult is the initiator's record of a grouped query, kept in its graph
// (kind "GroupedResult", key "<uqid>_grouped") until fetched or the peer restarts.
type StoredResult struct {
	Uqid      string          `json:"uqid"`
	Query     string          `json:"query"`
	Requester string          `json:"requester"`
	State     string          `json:"state"`
	Submitted string          `json:"submitted"`
	Completed string          `json:"completed,omitempty"`
	Header    []string        `json:"header,omitempty"`
	Rows      [][]string      `json:"rows,omitempty"`
	Manifest  []ManifestEntry `json:"manifest,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// ManifestEntry is one partition status in a stored result.
type ManifestEntry struct {
	Partition string `json:"partition"`
	Peer      string `json:"peer,omitempty"`
	Status    string `json:"status"`
	Detail    string `json:"detail,omitempty"`
}

const storedResultKind = "GroupedResult"

func storedResultKey(uqid string) string { return uqid + "_grouped" }

// SaveStoredResult writes (or overwrites) the initiator's record of a query.
func SaveStoredResult(gm *graph.Manager, r StoredResult) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	n := data.NewGraphNode()
	n.SetAttr("key", storedResultKey(r.Uqid))
	n.SetAttr("kind", storedResultKind)
	n.SetAttr("name", storedResultKind)
	n.SetAttr("query_key", r.Uqid)
	n.SetAttr("state", r.State)
	n.SetAttr("requester", r.Requester)
	n.SetAttr("record", string(b))
	trans := graph.NewGraphTrans(gm)
	if err := trans.StoreNode("main", n); err != nil {
		return err
	}
	return trans.Commit()
}

// LoadStoredResult returns the initiator's record of a query, or nil.
func LoadStoredResult(gm *graph.Manager, uqid string) (*StoredResult, error) {
	n, err := gm.FetchNode("main", storedResultKey(uqid), storedResultKind)
	if err != nil || n == nil {
		return nil, err
	}
	var r StoredResult
	if err := json.Unmarshal([]byte(fmt.Sprint(n.Attr("record"))), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func manifestEntries(entries []*common.PartitionPartial) []ManifestEntry {
	out := make([]ManifestEntry, len(entries))
	for i, e := range entries {
		out[i] = ManifestEntry{Partition: e.PartitionId, Peer: e.PeerId, Status: e.Status.String(), Detail: e.Detail}
	}
	return out
}

// ---------------------------------------------------------------------------
// Fetching a result later: "@result <uqid>"

// ResultPrefix starts a request for the stored result of an earlier query.
const ResultPrefix = "@result"

// IsResultRequest reports whether a query asks for a stored result.
func IsResultRequest(q string) bool {
	f := strings.Fields(q)
	return len(f) == 2 && f[0] == ResultPrefix
}

// ResultReply builds the reply to "@result <uqid>" from the stored record:
// the full result if the query completed, otherwise a status row. Only the
// verified requester of the original query may read it.
func ResultReply(stored *StoredResult, requester string, verifyErr error) *common.QueryMessage {
	status := func(state, detail string) *common.QueryMessage {
		return &common.QueryMessage{Type: common.MessageType_RESULT, RecordCount: 1,
			Result: []*common.Row{{Data: []string{"Status", state, detail}}}}
	}
	switch {
	case verifyErr != nil:
		return &common.QueryMessage{Type: common.MessageType_RESULT, Error: "unverified requester: " + verifyErr.Error()}
	case stored == nil:
		return status("UNKNOWN", "no query with this UQI was initiated at this peer")
	case stored.Requester != requester:
		return &common.QueryMessage{Type: common.MessageType_RESULT, Error: "only the requester of the query may fetch its result"}
	case stored.State == common.QueryState_FAILED.String():
		return &common.QueryMessage{Type: common.MessageType_RESULT, Error: "query failed: " + stored.Error}
	case stored.State != common.QueryState_COMPLETED.String():
		return status(stored.State, "query still running since "+stored.Submitted+"; fetch again later")
	}
	rows := make([]*common.Row, len(stored.Rows))
	for i, r := range stored.Rows {
		rows[i] = &common.Row{Data: r}
	}
	manifest := make([]*common.PartitionPartial, len(stored.Manifest))
	for i, m := range stored.Manifest {
		manifest[i] = &common.PartitionPartial{PartitionId: m.Partition, PeerId: m.Peer,
			Status: common.PartitionPartial_Status(common.PartitionPartial_Status_value[m.Status]), Detail: m.Detail}
	}
	return &common.QueryMessage{Type: common.MessageType_RESULT, Uqid: stored.Uqid, Query: stored.Query,
		Labels: stored.Header, Result: rows, RecordCount: int32(len(rows)), PartitionPartials: manifest}
}

// HandleResultRequest answers "@result <uqid>" locally (never forwarded).
func HandleResultRequest(conn network.Stream, msg *common.QueryMessage, gm *graph.Manager, verifyErr error) {
	uqid := strings.Fields(msg.Query)[1]
	stored, err := LoadStoredResult(gm, uqid)
	if err != nil {
		logger.Errorf("Reading stored result %s: %v", uqid, err)
	}
	if stored != nil && stored.State != common.QueryState_COMPLETED.String() && stored.State != common.QueryState_FAILED.String() {
		// Still running: report the live state recorded at this peer.
		if st := QueryNodeState(gm, uqid); st != "" {
			stored.State = st
		}
	}
	reply := ResultReply(stored, msg.RequesterId, verifyErr)
	b, err := proto.Marshal(reply)
	if err != nil {
		logger.Errorf("Error marshalling result reply: %v", err)
		return
	}
	if err := helpers.WriteDelimitedMessage(conn, b); err != nil {
		logger.Errorf("Error sending result reply: %v", err)
	}
}
