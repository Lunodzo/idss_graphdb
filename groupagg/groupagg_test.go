package groupagg

import (
	"math"
	"reflect"
	"testing"
)

func mustParse(t *testing.T, q string) (Spec, string) {
	t.Helper()
	s, eql, err := Parse(q)
	if err != nil {
		t.Fatalf("Parse(%q): %v", q, err)
	}
	return s, eql
}

func TestParse(t *testing.T) {
	s, eql := mustParse(t, "@group(keys=0,decade(1); aggs=count,sum(2),avg(2),exposure(2,3,2013,2023)) get ACLineSegment show insulation, inServiceYear, lengthKm, inServiceYear")
	if eql != "get ACLineSegment show insulation, inServiceYear, lengthKm, inServiceYear" {
		t.Fatalf("eql = %q", eql)
	}
	want := Spec{
		Keys: []KeySpec{{Col: 0}, {Col: 1, Transform: "decade"}},
		Aggs: []AggSpec{{Func: "count"}, {Func: "sum", Col: 2}, {Func: "avg", Col: 2}, {Func: "exposure", Col: 2, YearCol: 3, From: 2013, To: 2023}},
	}
	if !reflect.DeepEqual(s, want) {
		t.Fatalf("spec = %+v, want %+v", s, want)
	}
	if s.Width() != 5 {
		t.Fatalf("Width = %d, want 5", s.Width())
	}
	if _, _, err := Parse("@group(keys=; aggs=count) get FailureEvent show failureYear"); err != nil {
		t.Fatalf("empty keys should be allowed: %v", err)
	}
	for _, bad := range []string{
		"get FailureEvent",
		"@group(keys=0; aggs=) get X show a",
		"@group(keys=0; aggs=median(1)) get X show a",
		"@group(keys=0; aggs=count)",
		"@group(keys=x; aggs=count) get X show a",
		"@group(keys=0; aggs=exposure(1,2,2023,2013)) get X show a,b,c",
	} {
		if _, _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) should fail", bad)
		}
	}
}

func TestLocalMergeFinalise(t *testing.T) {
	s, _ := mustParse(t, "@group(keys=0; aggs=count,sum(1),min(1),max(1),avg(1)) get X show k, v")
	a, err := Local(s, [][]interface{}{{"A", 1.0}, {"A", 3.0}, {"B", 10.0}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Local(s, [][]interface{}{{"A", 5.0}, {"C", 2.0}}) // C only on partition b
	if err != nil {
		t.Fatal(err)
	}
	merged := Partials{}
	Merge(s, merged, a)
	Merge(s, merged, b)
	h, rows := Finalise(s, []string{"K", "V"}, merged)
	if !reflect.DeepEqual(h, []string{"K", "count", "sum(V)", "min(V)", "max(V)", "avg(V)"}) {
		t.Fatalf("header = %v", h)
	}
	want := [][]string{
		{"A", "3", "9", "1", "5", "3"}, // avg = 9/3, not mean of partition means (2 and 5)
		{"B", "1", "10", "10", "10", "10"},
		{"C", "1", "2", "2", "2", "2"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	// Merge must not alias src partials.
	if a[encodeKey([]string{"A"})].Values[0] != 2 {
		t.Fatalf("Merge modified its source")
	}
}

func TestMergeOrderIndependentAndEmpty(t *testing.T) {
	s, _ := mustParse(t, "@group(keys=0; aggs=count,min(1)) get X show k, v")
	p1, _ := Local(s, [][]interface{}{{"A", 4.0}})
	p2, _ := Local(s, nil) // a partition with no rows
	p3, _ := Local(s, [][]interface{}{{"A", 2.0}})
	x, y := Partials{}, Partials{}
	for _, p := range []Partials{p1, p2, p3} {
		Merge(s, x, p)
	}
	for _, p := range []Partials{p3, p2, p1} {
		Merge(s, y, p)
	}
	_, rx := Finalise(s, nil, x)
	_, ry := Finalise(s, nil, y)
	if !reflect.DeepEqual(rx, ry) || !reflect.DeepEqual(rx, [][]string{{"A", "2", "2"}}) {
		t.Fatalf("order-dependent or wrong: %v vs %v", rx, ry)
	}
	if len(p2) != 0 {
		t.Fatalf("empty input should give no groups")
	}
}

func TestKeyNormalisationAndDecade(t *testing.T) {
	s, _ := mustParse(t, "@group(keys=0,decade(1); aggs=count) get X show y, iy")
	p, err := Local(s, [][]interface{}{{2020.0, 1987}, {2020, "1981"}, {"2020", 1990.0}})
	if err != nil {
		t.Fatal(err)
	}
	_, rows := Finalise(s, nil, p)
	want := [][]string{{"2020", "1980s", "2"}, {"2020", "1990s", "1"}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
}

func TestExposure(t *testing.T) {
	for _, c := range []struct {
		l           float64
		y, from, to int
		want        float64
	}{
		{2, 1980, 2013, 2023, 22}, // in service before the window: 11 years
		{2, 2013, 2013, 2023, 22},
		{2, 2020, 2013, 2023, 8}, // 2020..2023
		{2, 2023, 2013, 2023, 2},
		{2, 2024, 2013, 2023, 0}, // after the window
	} {
		if got := Exposure(c.l, c.y, c.from, c.to); got != c.want {
			t.Errorf("Exposure(%v,%d,%d,%d) = %v, want %v", c.l, c.y, c.from, c.to, got, c.want)
		}
	}
}

func TestEncodeDecodeLossless(t *testing.T) {
	s, _ := mustParse(t, "@group(keys=0; aggs=sum(1),min(1),max(1)) get X show k, v")
	p, _ := Local(s, [][]interface{}{{"A", 0.1}, {"A", 0.2}, {"B", 1.0 / 3}})
	back, err := Decode(s, Encode(p))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, back) {
		t.Fatalf("round trip changed values: %v vs %v", Encode(p), Encode(back))
	}
	if _, err := Decode(s, [][]string{{"A", "1"}}); err == nil {
		t.Fatalf("short row should fail")
	}
	if _, err := Decode(s, [][]string{{"A", "1", "1", "1"}, {"A", "2", "2", "2"}}); err == nil {
		t.Fatalf("duplicate group should fail")
	}
	// +Inf/-Inf survive (min/max of a group that never saw a value cannot occur
	// after Local, but encoding must still be lossless).
	inf := Partials{"Z": {Key: []string{"Z"}, Values: []float64{0, math.Inf(1), math.Inf(-1)}}}
	back, err = Decode(s, Encode(inf))
	if err != nil || !math.IsInf(back["Z"].Values[1], 1) || !math.IsInf(back["Z"].Values[2], -1) {
		t.Fatalf("infinities not preserved: %v %v", back, err)
	}
}

func TestLocalErrors(t *testing.T) {
	s, _ := mustParse(t, "@group(keys=2; aggs=count) get X show a")
	if _, err := Local(s, [][]interface{}{{"a"}}); err == nil {
		t.Fatalf("out-of-range key column should fail")
	}
	s, _ = mustParse(t, "@group(keys=0; aggs=sum(1)) get X show a, b")
	if _, err := Local(s, [][]interface{}{{"a", "not-a-number"}}); err == nil {
		t.Fatalf("non-numeric sum should fail")
	}
}
