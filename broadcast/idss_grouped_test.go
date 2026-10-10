package broadcast

import (
	"reflect"
	"strings"
	"testing"

	"idss/graphdb/common"
	"idss/graphdb/groupagg"
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
