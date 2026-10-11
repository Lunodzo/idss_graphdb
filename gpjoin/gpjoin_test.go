package gpjoin

import (
	"strings"
	"testing"

	"idss/graphdb/groupagg"

	eql2 "github.com/krotik/eliasdb/eql"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/data"
	"github.com/krotik/eliasdb/graph/graphstorage"
)

func TestParse(t *testing.T) {
	q := "@join(type=digging; window=30; cols=1,2,3,4,5) @group(keys=0; aggs=count) get FailureEvent show a, b, c, d, e, f"
	s, rest, err := Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	if s.Type != "digging" || s.Window != 30 || len(s.Cols) != 5 || s.Cols[4] != 5 {
		t.Fatalf("spec = %+v", s)
	}
	if !strings.HasPrefix(rest, "@group(") {
		t.Fatalf("rest = %q", rest)
	}
	for _, bad := range []string{
		"@join(type=flood; window=1; cols=1,2,3) @group(keys=0; aggs=count) get X show a",
		"@join(type=weather; cols=1,2,3) @group(keys=0; aggs=count) get X show a",
		"@join(type=weather; window=3; cols=1,2) @group(keys=0; aggs=count) get X show a",
		"@join(type=weather; window=3; cols=1,2,3) get X show a",
		"@join(type=weather; window=3; cols=1,2,3; extra=1) @group(keys=0; aggs=count) get X show a",
	} {
		if _, _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) should fail", bad)
		}
	}
}

func TestCheckNoGeometryLeak(t *testing.T) {
	js := Spec{Type: "weather", Window: 3, Cols: []int{1, 2, 3}}
	ok, _, _ := groupagg.Parse("@group(keys=0; aggs=count) get X show a")
	if err := CheckNoGeometryLeak(js, ok); err != nil {
		t.Fatalf("count by DSO should be allowed: %v", err)
	}
	for _, q := range []string{
		"@group(keys=2; aggs=count) get X show a",  // latitude as key
		"@group(keys=0; aggs=max(1)) get X show a", // failure day aggregated
		"@group(keys=0; aggs=avg(3)) get X show a", // longitude aggregated
	} {
		gs, _, _ := groupagg.Parse(q)
		if err := CheckNoGeometryLeak(js, gs); err == nil {
			t.Errorf("%q should be rejected", q)
		}
	}
}

func TestDiggingPredicate(t *testing.T) {
	js := Spec{Type: "digging", Window: 30, Cols: []int{1, 2, 3, 4, 5}}
	events := []Event{{MinLat: 55.0000, MaxLat: 55.0010, MinLon: 10.0000, MaxLon: 10.0010, Start: 1000, End: 1010}}
	m := NewMatcher(js, events)
	row := func(fd int, minLat, maxLat, minLon, maxLon float64) []interface{} {
		return []interface{}{"DSO-01", fd, minLat, maxLat, minLon, maxLon}
	}
	cases := []struct {
		name string
		row  []interface{}
		want bool
	}{
		{"overlap, during digging", row(1005, 55.0005, 55.0020, 10.0005, 10.0020), true},
		{"overlap, failure 30 days after end", row(1040, 55.0005, 55.0020, 10.0005, 10.0020), true},
		{"overlap, failure 31 days after end", row(1041, 55.0005, 55.0020, 10.0005, 10.0020), false},
		{"overlap, failure before digging started", row(999, 55.0005, 55.0020, 10.0005, 10.0020), false},
		{"touching edge counts (closed intervals)", row(1005, 55.0010, 55.0020, 10.0010, 10.0020), true},
		{"no overlap", row(1005, 55.0011, 55.0020, 10.0000, 10.0010), false},
		{"box spanning several index cells", row(1005, 54.99, 55.02, 9.99, 10.02), true},
	}
	for _, c := range cases {
		got, err := m.Match(c.row)
		if err != nil || got != c.want {
			t.Errorf("%s: got %v (%v), want %v", c.name, got, err, c.want)
		}
	}
}

func TestWeatherPredicate(t *testing.T) {
	js := Spec{Type: "weather", Window: 3, Cols: []int{1, 2, 3}}
	events := []Event{{MinLat: 55.25, MaxLat: 55.5, MinLon: 10.0, MaxLon: 10.25, Start: 2000, End: 2000}}
	m := NewMatcher(js, events)
	row := func(fd int, lat, lon float64) []interface{} { return []interface{}{"DSO-01", fd, lat, lon} }
	cases := []struct {
		name string
		row  []interface{}
		want bool
	}{
		{"in cell, same day", row(2000, 55.3, 10.1), true},
		{"in cell, 3 days after event", row(2003, 55.3, 10.1), true},
		{"in cell, 4 days after event", row(2004, 55.3, 10.1), false},
		{"in cell, event after failure", row(1999, 55.3, 10.1), false},
		{"on lower cell edge (inclusive)", row(2000, 55.25, 10.0), true},
		{"on upper cell edge (exclusive)", row(2000, 55.5, 10.1), false},
		{"other cell", row(2000, 55.6, 10.1), false},
	}
	for _, c := range cases {
		got, err := m.Match(c.row)
		if err != nil || got != c.want {
			t.Errorf("%s: got %v (%v), want %v", c.name, got, err, c.want)
		}
	}
}

func TestParseEventsAndFilter(t *testing.T) {
	ev, err := ParseEvents("digging", [][]string{{"55", "55.001", "10", "10.001", "1000", "1010", "1"}})
	if err != nil || len(ev) != 1 || ev[0].End != 1010 {
		t.Fatalf("ParseEvents = %+v, %v", ev, err)
	}
	if _, err := ParseEvents("weather", [][]string{{"55", "55.25"}}); err == nil {
		t.Fatalf("short row should fail")
	}
	m := NewMatcher(Spec{Type: "digging", Window: 30, Cols: []int{1, 2, 3, 4, 5}}, ev)
	rows := [][]interface{}{
		{"DSO-01", 1005, 55.0, 55.002, 10.0, 10.002},
		{"DSO-02", 1005, 56.0, 56.002, 10.0, 10.002},
	}
	kept, err := m.Filter(rows)
	if err != nil || len(kept) != 1 || kept[0][0] != "DSO-01" {
		t.Fatalf("Filter = %v, %v", kept, err)
	}
	if _, err := m.Match([]interface{}{"DSO-01", 1005}); err == nil {
		t.Fatalf("missing columns should fail")
	}
}

func TestEventQuery(t *testing.T) {
	for _, typ := range []string{"digging", "weather"} {
		q, err := EventQuery(typ)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := groupagg.Parse(q); err != nil {
			t.Errorf("event query for %s does not parse: %v", typ, err)
		}
	}
	if _, err := EventQuery("flood"); err == nil {
		t.Fatalf("unknown type should fail")
	}
}

func TestEventQueryInBoxFiltersInEliasDB(t *testing.T) {
	gm := graph.NewGraphManager(graphstorage.NewMemoryGraphStorage("box"))
	add := func(key string, minLat, maxLat, minLon, maxLon float64, start, end int) {
		n := data.NewGraphNode()
		for k, v := range map[string]interface{}{"key": key, "kind": "ActivityRecord", "minLat": minLat, "maxLat": maxLat,
			"minLon": minLon, "maxLon": maxLon, "startDay": start, "endDay": end} {
			n.SetAttr(k, v)
		}
		if err := gm.StoreNode("main", n); err != nil {
			t.Fatal(err)
		}
	}
	add("in", 55.50, 55.51, 10.10, 10.11, 100, 110)    // inside
	add("edge", 55.60, 55.70, 10.20, 10.30, 100, 110)  // touches the box corner (closed intervals)
	add("west", 55.50, 55.51, 9.00, 9.01, 100, 110)    // outside (longitude)
	add("north", 56.50, 56.51, 10.10, 10.11, 100, 110) // outside (latitude)
	add("span", 55.00, 56.00, 10.00, 10.05, 100, 110)  // larger than the box, overlapping it
	q, err := EventQueryInBox("digging", Box{MinLat: 55.4, MaxLat: 55.6, MinLon: 10.05, MaxLon: 10.2})
	if err != nil {
		t.Fatal(err)
	}
	_, eql, err := groupagg.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	res, err := eql2.RunQuery("t", "main", eql, gm)
	if err != nil {
		t.Fatalf("%s: %v", eql, err)
	}
	if got := len(res.Rows()); got != 3 {
		t.Fatalf("%s: %d rows, want 3 (in, edge, span): %v", eql, got, res.Rows())
	}
}

func TestBoxExtend(t *testing.T) {
	var b Box
	b.Extend(55, 55.1, 10, 10.1, true)
	b.Extend(54.9, 55.05, 10.05, 10.3, false)
	if b != (Box{54.9, 55.1, 10, 10.3}) {
		t.Fatalf("box %+v", b)
	}
}

func TestParseEventBoxQueryRoundTrip(t *testing.T) {
	for _, jt := range []string{"digging", "weather"} {
		b := Box{MinLat: 54.6124, MaxLat: 57.6789, MinLon: 8.3232, MaxLon: 8.576}
		q, err := EventQueryInBox(jt, b)
		if err != nil {
			t.Fatal(err)
		}
		gotType, gotBox, ok := ParseEventBoxQuery(q)
		if !ok || gotType != jt || gotBox != b {
			t.Fatalf("%s: got %q %+v %v", jt, gotType, gotBox, ok)
		}
	}
	for _, q := range []string{
		"@group(keys=0; aggs=count) get FailureEvent show failureYear",
		"@group(keys=0,1,2,3,4,5; aggs=count) get ActivityRecord where maxLat >= x and minLat <= 1 and maxLon >= 1 and minLon <= 1 show minLat, maxLat, minLon, maxLon, startDay, endDay",
		"@group(keys=0; aggs=count) get ActivityRecord where maxLat >= 1 and minLat <= 1 and maxLon >= 1 and minLon <= 1 show reason",
	} {
		if _, _, ok := ParseEventBoxQuery(q); ok {
			t.Fatalf("must not match: %s", q)
		}
	}
	if !(Box{0, 1, 0, 1}).Overlaps(1, 2, 1, 2) || (Box{0, 1, 0, 1}).Overlaps(1.1, 2, 0, 1) {
		t.Fatal("overlap")
	}
}
