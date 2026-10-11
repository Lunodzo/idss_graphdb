package broadcast

import (
	"reflect"
	"strings"
	"testing"

	"idss/graphdb/common"
	"idss/graphdb/groupagg"

	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
)

func entry(part, peerID string, status common.PartitionPartial_Status, rows ...[]string) *common.PartitionPartial {
	e := &common.PartitionPartial{PartitionId: part, PeerId: peerID, Status: status}
	for _, r := range rows {
		e.Partials = append(e.Partials, &common.Row{Data: r})
	}
	return e
}

func ids(es []*common.PartitionPartial) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.PartitionId + "=" + e.Status.String()
	}
	return out
}

func TestUnionEntriesDeduplicatesByPartitionNotContent(t *testing.T) {
	// Two DSOs with identical partial counts must both survive.
	a := entry("dso01", "P1", common.PartitionPartial_ANSWERED, []string{"2020", "3"})
	b := entry("dso02", "P2", common.PartitionPartial_ANSWERED, []string{"2020", "3"})
	// dso01 arriving a second time via another path is counted once.
	got := UnionEntries([]*common.PartitionPartial{a}, []*common.PartitionPartial{b, a})
	if !reflect.DeepEqual(ids(got), []string{"dso01=ANSWERED", "dso02=ANSWERED"}) {
		t.Fatalf("union = %v", ids(got))
	}
	spec, _, _ := groupagg.Parse("@group(keys=0; aggs=count) get FailureEvent show failureYear")
	merged, err := FoldEntries(spec, got)
	if err != nil {
		t.Fatal(err)
	}
	_, rows := groupagg.Finalise(spec, nil, merged)
	if !reflect.DeepEqual(rows, [][]string{{"2020", "6"}}) {
		t.Fatalf("fold = %v, want 2020=6", rows)
	}
}

func TestUnionEntriesAnsweredBeatsFailure(t *testing.T) {
	timeout := entry("peer:P3", "P3", common.PartitionPartial_TIMEOUT)
	answered := entry("dso03", "P3", common.PartitionPartial_ANSWERED)
	unreachable := entry("peer:P4", "P4", common.PartitionPartial_UNREACHABLE)
	got := UnionEntries([]*common.PartitionPartial{timeout, unreachable}, []*common.PartitionPartial{answered})
	if !reflect.DeepEqual(ids(got), []string{"dso03=ANSWERED", "peer:P4=UNREACHABLE"}) {
		t.Fatalf("union = %v", ids(got))
	}
	// Same partition: ANSWERED replaces a non-answered entry, never the reverse.
	x := entry("dso05", "P5", common.PartitionPartial_ERROR)
	y := entry("dso05", "P5", common.PartitionPartial_ANSWERED)
	if got := UnionEntries([]*common.PartitionPartial{y, x}); got[0].Status != common.PartitionPartial_ANSWERED {
		t.Fatalf("answered entry was replaced: %v", ids(got))
	}
	if got := UnionEntries([]*common.PartitionPartial{x, y}); got[0].Status != common.PartitionPartial_ANSWERED {
		t.Fatalf("answered entry did not replace error: %v", ids(got))
	}
}

func TestFoldEntriesSkipsNonAnsweredAndRejectsBadRows(t *testing.T) {
	spec, _, _ := groupagg.Parse("@group(keys=0; aggs=count) get FailureEvent show failureYear")
	es := []*common.PartitionPartial{
		entry("dso01", "P1", common.PartitionPartial_ANSWERED, []string{"2020", "2"}),
		entry("dso02", "P2", common.PartitionPartial_DENIED, []string{"2020", "99"}),
	}
	merged, err := FoldEntries(spec, es)
	if err != nil {
		t.Fatal(err)
	}
	_, rows := groupagg.Finalise(spec, nil, merged)
	if !reflect.DeepEqual(rows, [][]string{{"2020", "2"}}) {
		t.Fatalf("fold = %v", rows)
	}
	bad := []*common.PartitionPartial{entry("dso03", "P3", common.PartitionPartial_ANSWERED, []string{"2020"})}
	if _, err := FoldEntries(spec, bad); err == nil || !strings.Contains(err.Error(), "dso03") {
		t.Fatalf("expected error naming the partition, got %v", err)
	}
}

func TestManifestSummary(t *testing.T) {
	s := ManifestSummary([]*common.PartitionPartial{
		entry("dso01", "P1", common.PartitionPartial_ANSWERED),
		entry("dso02", "P2", common.PartitionPartial_DENIED),
		entry("peer:P4", "P4", common.PartitionPartial_TIMEOUT),
	})
	want := "answered 1/3, denied 1 [dso02], timeout 1 [peer:P4], unreachable 0, error 0"
	if s != want {
		t.Fatalf("summary = %q, want %q", s, want)
	}
}

func TestIsEmptyResultError(t *testing.T) {
	for msg, want := range map[string]bool{
		"EQL error in q: Unknown node kind (FailureEvent) (Line:1 Pos:5)": true,
		"Empty traversal":        true,
		"EQL error: Parse error": false,
	} {
		if got := isEmptyResultError(errString(msg)); got != want {
			t.Errorf("isEmptyResultError(%q) = %v, want %v", msg, got, want)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestRunGroupedOnGraphCountsAndJoins(t *testing.T) {
	gs := graphstorage.NewMemoryGraphStorage("t")
	gm := graph.NewGraphManager(gs)
	add := func(kind, key string, attrs map[string]interface{}) {
		n := data.NewGraphNode()
		n.SetAttr("kind", kind)
		n.SetAttr("key", key)
		for k, v := range attrs {
			n.SetAttr(k, v)
		}
		if err := gm.StoreNode("main", n); err != nil {
			t.Fatal(err)
		}
	}
	add("FailureEvent", "f1", map[string]interface{}{"failureYear": "2020", "failureDay": "100"})
	add("FailureEvent", "f2", map[string]interface{}{"failureYear": "2020", "failureDay": "200"})
	add("FailureEvent", "f3", map[string]interface{}{"failureYear": "2021", "failureDay": "300"})

	header, rows, events, err := RunGroupedOnGraph("@group(keys=0; aggs=count) get FailureEvent show failureYear", gm)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r[0]] = r[1]
	}
	if len(header) != 2 || events != 0 || got["2020"] != "2" || got["2021"] != "1" {
		t.Fatalf("header %v rows %v events %d", header, rows, events)
	}
	// A join with no public events on the graph keeps no failures.
	_, rows, events, err = RunGroupedOnGraph("@join(type=weather; window=3; cols=1,2,3) @group(keys=0; aggs=count) get FailureEvent show failureYear, failureDay, failureDay, failureDay", gm)
	if err != nil || events != 0 || len(rows) != 0 {
		t.Fatalf("join without events: rows %v events %d", rows, events)
	}
}

func TestTrafficCounterLogLine(t *testing.T) {
	c := TrafficCounter{Streams: 3, Duplicates: 1, BytesOut: 120, BytesIn: 4096, Regions: 2, Failovers: 1, Heartbeats: 4}
	want := "gptraffic uqi=q1 streams=3 dup=1 bytes_out=120 bytes_in=4096 regions=2 failover=1 heartbeats=4"
	if got := c.LogLine("q1"); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestMergePhase1ReportsMissingEvents(t *testing.T) {
	phase2 := []*common.PartitionPartial{
		entry("dso01", "p1", common.PartitionPartial_ANSWERED, []string{"DSO-01", "3"}),
		entry("public_digging", "p2", common.PartitionPartial_ANSWERED),
	}
	phase1 := []*common.PartitionPartial{
		entry("public_digging", "p2", common.PartitionPartial_TIMEOUT),
		entry("public_weather", "p3", common.PartitionPartial_ANSWERED),
		{PartitionId: "events:ActivityRecord", Status: common.PartitionPartial_UNREACHABLE, Detail: "no provider"},
	}
	got := MergePhase1(phase2, phase1)
	status := map[string]common.PartitionPartial_Status{}
	for _, e := range got {
		status[e.PartitionId] = e.Status
	}
	if len(got) != 3 || status["dso01"] != common.PartitionPartial_ANSWERED ||
		status["public_digging"] != common.PartitionPartial_TIMEOUT ||
		status["events:ActivityRecord"] != common.PartitionPartial_UNREACHABLE {
		t.Fatalf("merged manifest %v", status)
	}
	if !strings.HasPrefix(got[1].Detail, "join phase 1: ") {
		t.Fatalf("detail %q", got[1].Detail)
	}
}
