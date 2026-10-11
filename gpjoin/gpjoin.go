// Package gpjoin implements the Green Power two-phase join between DSO failure
// and asset data and public event registers (digging activities, extreme
// weather). Public events are shipped to the DSO peers, which evaluate the join
// locally; cable geometry never leaves its owner.
//
// A join query has the form
//
//	@join(type=digging|weather; window=<days>; cols=<i,...>) @group(...) <EQL>
//
// where cols names the columns of the local EQL result that hold the failure
// day and the segment geometry:
//
//	digging: failureDay, minLat, maxLat, minLon, maxLon   (segment bounding box)
//	weather: failureDay, lat, lon                          (segment midpoint)
//
// These columns are used for the join only and may not appear in the @group
// keys or aggregates, so no geometry is returned.
//
// The predicates are identical to those used by server/generate_cim_data.py to
// compute the ground truth:
//
//	digging: segment box overlaps event box (closed intervals) and
//	         event.startDay <= failureDay and event.endDay >= failureDay - window
//	weather: event.minLat <= lat < event.maxLat and event.minLon <= lon < event.maxLon
//	         and failureDay - window <= event.day <= failureDay
package gpjoin

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"idss/graphdb/groupagg"
)

// Prefix marks a join query.
const Prefix = "@join("

// Spec is a parsed @join clause.
type Spec struct {
	Type   string // digging or weather
	Window int    // days before the failure
	Cols   []int  // failureDay followed by geometry columns
}

// IsJoin reports whether the query uses the @join prefix.
func IsJoin(query string) bool {
	return strings.HasPrefix(strings.TrimSpace(query), Prefix)
}

// Parse splits a join query into its Spec and the remaining @group query.
func Parse(query string) (Spec, string, error) {
	q := strings.TrimSpace(query)
	if !strings.HasPrefix(q, Prefix) {
		return Spec{}, "", fmt.Errorf("not a join query: expected %stype=...; window=...; cols=...) @group(...) <EQL>", Prefix)
	}
	end := strings.Index(q, ")")
	if end < 0 {
		return Spec{}, "", fmt.Errorf("join query: missing ')'")
	}
	var spec Spec
	spec.Window = -1
	for _, part := range strings.Split(q[len(Prefix):end], ";") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return Spec{}, "", fmt.Errorf("join query: invalid item %q", strings.TrimSpace(part))
		}
		key, val := strings.ToLower(strings.TrimSpace(kv[0])), strings.TrimSpace(kv[1])
		switch key {
		case "type":
			spec.Type = strings.ToLower(val)
		case "window":
			w, err := strconv.Atoi(val)
			if err != nil || w < 0 {
				return Spec{}, "", fmt.Errorf("join query: invalid window %q", val)
			}
			spec.Window = w
		case "cols":
			for _, c := range strings.Split(val, ",") {
				i, err := strconv.Atoi(strings.TrimSpace(c))
				if err != nil || i < 0 {
					return Spec{}, "", fmt.Errorf("join query: invalid column %q", c)
				}
				spec.Cols = append(spec.Cols, i)
			}
		default:
			return Spec{}, "", fmt.Errorf("join query: unknown item %q", key)
		}
	}
	want := map[string]int{"digging": 5, "weather": 3}
	n, ok := want[spec.Type]
	if !ok {
		return Spec{}, "", fmt.Errorf("join query: type must be digging or weather, got %q", spec.Type)
	}
	if spec.Window < 0 {
		return Spec{}, "", fmt.Errorf("join query: window is required")
	}
	if len(spec.Cols) != n {
		return Spec{}, "", fmt.Errorf("join query: type %s needs %d cols, got %d", spec.Type, n, len(spec.Cols))
	}
	rest := strings.TrimSpace(q[end+1:])
	if !groupagg.IsGrouped(rest) {
		return Spec{}, "", fmt.Errorf("join query: expected @group(...) after @join(...)")
	}
	return spec, rest, nil
}

// CheckNoGeometryLeak rejects group specs whose keys or aggregates use the join
// columns, so that geometry and failure days never leave the peer.
func CheckNoGeometryLeak(js Spec, gs groupagg.Spec) error {
	protected := map[int]bool{}
	for _, c := range js.Cols {
		protected[c] = true
	}
	for _, k := range gs.Keys {
		if protected[k.Col] {
			return fmt.Errorf("join column %d may not be a group key", k.Col)
		}
	}
	for _, a := range gs.Aggs {
		if a.Func != "count" && (protected[a.Col] || (a.Func == "exposure" && protected[a.YearCol])) {
			return fmt.Errorf("join column %d may not be aggregated", a.Col)
		}
	}
	return nil
}

// EventQuery returns the phase-1 grouped query that collects the public events
// for a join type: every distinct event becomes one group.
func EventQuery(joinType string) (string, error) {
	switch joinType {
	case "digging":
		return "@group(keys=0,1,2,3,4,5; aggs=count) get ActivityRecord show minLat, maxLat, minLon, maxLon, startDay, endDay", nil
	case "weather":
		return "@group(keys=0,1,2,3,4; aggs=count) get MeasurementValue show minLat, maxLat, minLon, maxLon, day", nil
	}
	return "", fmt.Errorf("unknown join type %q", joinType)
}

// EventKind returns the node kind of the public register a join type reads.
func EventKind(joinType string) (string, error) {
	switch joinType {
	case "digging":
		return "ActivityRecord", nil
	case "weather":
		return "MeasurementValue", nil
	}
	return "", fmt.Errorf("unknown join type %q", joinType)
}

// Event is a public event: a bounding box and a day interval [Start, End]
// (weather events have Start == End).
type Event struct {
	MinLat, MaxLat, MinLon, MaxLon float64
	Start, End                     int
}

// ParseEvents decodes phase-1 rows (the key cells of EventQuery's groups).
func ParseEvents(joinType string, rows [][]string) ([]Event, error) {
	need := map[string]int{"digging": 6, "weather": 5}[joinType]
	if need == 0 {
		return nil, fmt.Errorf("unknown join type %q", joinType)
	}
	out := make([]Event, 0, len(rows))
	for i, r := range rows {
		if len(r) < need {
			return nil, fmt.Errorf("event row %d: %d cells, want %d", i, len(r), need)
		}
		v := make([]float64, need)
		for j := 0; j < need; j++ {
			f, err := strconv.ParseFloat(strings.TrimSpace(r[j]), 64)
			if err != nil {
				return nil, fmt.Errorf("event row %d: %v", i, err)
			}
			v[j] = f
		}
		e := Event{MinLat: v[0], MaxLat: v[1], MinLon: v[2], MaxLon: v[3], Start: int(v[4]), End: int(v[4])}
		if joinType == "digging" {
			e.End = int(v[5])
		}
		out = append(out, e)
	}
	return out, nil
}

const digCell = 0.01 // index cell size for digging events, degrees

type cell struct{ i, j int }

// Matcher evaluates the join predicate against an indexed set of events.
type Matcher struct {
	spec  Spec
	dig   map[cell][]int // digging: events per 0.01° cell
	wx    []Event
	wxIdx map[cell][]int // weather: events per exact cell (minLat, minLon)
}

// NewMatcher indexes the events for the join spec.
func NewMatcher(spec Spec, events []Event) *Matcher {
	m := &Matcher{spec: spec}
	switch spec.Type {
	case "digging":
		m.dig = map[cell][]int{}
		m.wx = events
		for idx, e := range events {
			for ci := int(math.Floor(e.MinLat / digCell)); ci <= int(math.Floor(e.MaxLat/digCell)); ci++ {
				for cj := int(math.Floor(e.MinLon / digCell)); cj <= int(math.Floor(e.MaxLon/digCell)); cj++ {
					m.dig[cell{ci, cj}] = append(m.dig[cell{ci, cj}], idx)
				}
			}
		}
	case "weather":
		m.wx = events
		m.wxIdx = map[cell][]int{}
		for idx, e := range events {
			c := cell{int(math.Floor(e.MinLat*4 + 1e-9)), int(math.Floor(e.MinLon*4 + 1e-9))}
			m.wxIdx[c] = append(m.wxIdx[c], idx)
		}
	}
	return m
}

func boxesOverlap(aMinLat, aMaxLat, aMinLon, aMaxLon float64, e Event) bool {
	return !(aMaxLat < e.MinLat || e.MaxLat < aMinLat || aMaxLon < e.MinLon || e.MaxLon < aMinLon)
}

// Match reports whether a local result row satisfies the join predicate.
func (m *Matcher) Match(row []interface{}) (bool, error) {
	vals := make([]float64, len(m.spec.Cols))
	for i, c := range m.spec.Cols {
		if c >= len(row) {
			return false, fmt.Errorf("join column %d out of range (%d columns)", c, len(row))
		}
		f, err := groupagg.ToFloat(row[c])
		if err != nil {
			return false, fmt.Errorf("join column %d: %v", c, err)
		}
		vals[i] = f
	}
	fd := int(vals[0])
	switch m.spec.Type {
	case "digging":
		minLat, maxLat, minLon, maxLon := vals[1], vals[2], vals[3], vals[4]
		seen := map[int]bool{}
		for ci := int(math.Floor(minLat / digCell)); ci <= int(math.Floor(maxLat/digCell)); ci++ {
			for cj := int(math.Floor(minLon / digCell)); cj <= int(math.Floor(maxLon/digCell)); cj++ {
				for _, idx := range m.dig[cell{ci, cj}] {
					if seen[idx] {
						continue
					}
					seen[idx] = true
					e := m.wx[idx]
					if boxesOverlap(minLat, maxLat, minLon, maxLon, e) && e.Start <= fd && e.End >= fd-m.spec.Window {
						return true, nil
					}
				}
			}
		}
	case "weather":
		lat, lon := vals[1], vals[2]
		c := cell{int(math.Floor(lat * 4)), int(math.Floor(lon * 4))}
		for _, idx := range m.wxIdx[c] {
			e := m.wx[idx]
			if e.MinLat <= lat && lat < e.MaxLat && e.MinLon <= lon && lon < e.MaxLon &&
				fd-m.spec.Window <= e.Start && e.Start <= fd {
				return true, nil
			}
		}
	}
	return false, nil
}

// Filter keeps the rows that satisfy the join predicate.
func (m *Matcher) Filter(rows [][]interface{}) ([][]interface{}, error) {
	out := make([][]interface{}, 0, len(rows))
	for _, r := range rows {
		ok, err := m.Match(r)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, r)
		}
	}
	return out, nil
}
