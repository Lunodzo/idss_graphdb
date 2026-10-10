package gpjoin

import (
	"strings"
	"testing"

	"idss/graphdb/groupagg"
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
