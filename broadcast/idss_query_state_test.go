package broadcast

import (
	"errors"
	"testing"

	"idss/graphdb/common"

	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/graphstorage"
)

func TestIsResultRequest(t *testing.T) {
	cases := map[string]bool{
		"@result Qm1-123":   true,
		" @result  Qm1-123": true,
		"@result":           false,
		"@result a b":       false,
		"@group(keys=0; aggs=count) get FailureEvent show failureYear": false,
	}
	for q, want := range cases {
		if got := IsResultRequest(q); got != want {
			t.Errorf("IsResultRequest(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestStoredResultRoundTripAndReply(t *testing.T) {
	gm := graph.NewGraphManager(graphstorage.NewMemoryGraphStorage("t"))
	if r, err := LoadStoredResult(gm, "missing"); err != nil || r != nil {
		t.Fatalf("missing record: %v %v", r, err)
	}
	rec := StoredResult{Uqid: "u1", Query: "@group(...) get X", Requester: "alice", State: common.QueryState_QUEUED.String(), Submitted: "t0"}
	if err := SaveStoredResult(gm, rec); err != nil {
		t.Fatal(err)
	}
	got, _ := LoadStoredResult(gm, "u1")
	running := ResultReply(got, "alice", nil)
	if len(running.Result) != 1 || running.Result[0].Data[0] != "Status" || running.Result[0].Data[1] != "QUEUED" {
		t.Fatalf("running reply %v", running)
	}
	if r := ResultReply(got, "mallory", nil); r.Error == "" {
		t.Fatal("other requester must be refused")
	}
	if r := ResultReply(got, "alice", errors.New("bad signature")); r.Error == "" {
		t.Fatal("unverified requester must be refused")
	}
	if r := ResultReply(nil, "alice", nil); r.Result[0].Data[1] != "UNKNOWN" {
		t.Fatalf("unknown uqid reply %v", r)
	}

	rec.State, rec.Completed = common.QueryState_COMPLETED.String(), "t1"
	rec.Header, rec.Rows = []string{"year", "count"}, [][]string{{"2020", "2"}, {"2021", "1"}}
	rec.Manifest = []ManifestEntry{{Partition: "dso01", Status: "ANSWERED"}, {Partition: "dso04", Status: "UNREACHABLE", Detail: "not reached"}}
	if err := SaveStoredResult(gm, rec); err != nil { // overwrite in place
		t.Fatal(err)
	}
	got, _ = LoadStoredResult(gm, "u1")
	done := ResultReply(got, "alice", nil)
	if done.RecordCount != 2 || len(done.Labels) != 2 || len(done.PartitionPartials) != 2 ||
		done.PartitionPartials[1].Status != common.PartitionPartial_UNREACHABLE || done.Uqid != "u1" {
		t.Fatalf("completed reply %v", done)
	}

	rec.State, rec.Error = common.QueryState_FAILED.String(), "boom"
	_ = SaveStoredResult(gm, rec)
	got, _ = LoadStoredResult(gm, "u1")
	if r := ResultReply(got, "alice", nil); r.Error == "" {
		t.Fatal("failed query must report its error")
	}
}

func TestIsHeartbeat(t *testing.T) {
	hb := &common.QueryMessage{Type: common.MessageType_RESULT, RecordCount: HeartbeatRecordCount}
	final := &common.QueryMessage{Type: common.MessageType_RESULT, RecordCount: 0}
	if !IsHeartbeat(hb) || IsHeartbeat(final) || IsHeartbeat(nil) {
		t.Fatal("heartbeat detection")
	}
}

func TestSetQueryNodeStatePersists(t *testing.T) {
	gm := graph.NewGraphManager(graphstorage.NewMemoryGraphStorage("s"))
	if QueryNodeState(gm, "u9") != "" {
		t.Fatal("unknown query must have no state")
	}
	for _, s := range []common.QueryState_State{common.QueryState_QUEUED, common.QueryState_LOCALLY_EXECUTED, common.QueryState_COMPLETED, common.QueryState_SENT_BACK} {
		if err := SetQueryNodeState(gm, "u9", s); err != nil {
			t.Fatal(err)
		}
		if got := QueryNodeState(gm, "u9"); got != s.String() {
			t.Fatalf("after %s: state %q", s, got)
		}
	}
}
